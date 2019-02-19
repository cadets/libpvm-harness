extern crate avro_rs;
#[macro_use]
extern crate clap;
#[macro_use]
extern crate maplit;
#[macro_use]
extern crate serde_derive;
#[macro_use]
extern crate lazy_static;
extern crate ctrlc;
extern crate env_logger;
extern crate kafka;
extern crate log;
extern crate openssl;
extern crate opus;
extern crate prometheus;
extern crate serde;
extern crate serde_json;
extern crate toml;
extern crate uuid;

mod cdm_view;

use std::{
    fs::File,
    io::Read,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
};

use clap::Arg;
use kafka::{
    client::{FetchOffset, SecurityConfig},
    consumer::Consumer,
};
use openssl::{
    pkey::PKey,
    ssl::{SslConnectorBuilder, SslContextBuilder, SslMethod, SSL_VERIFY_PEER},
    x509::X509_FILETYPE_PEM,
};

use opus::{cfg, engine, trace::cadets::TraceEvent};

use cdm_view::CDMView;

#[derive(Debug, Deserialize)]
struct Config<'a> {
    #[serde(borrow)]
    kafka: Option<KafkaConfig<'a>>,
    src_file: Option<String>,
    #[serde(borrow)]
    neo4j: Option<Neo4jConfig<'a>>,
}

#[derive(Debug, Deserialize)]
struct KafkaConfig<'a> {
    khost: Vec<&'a str>,
    topic: &'a str,
    #[serde(borrow)]
    ssl: Option<SSLConfig<'a>>,
}

#[derive(Debug, Deserialize)]
struct Neo4jConfig<'a> {
    db_host: &'a str,
    db_user: &'a str,
    db_pass: &'a str,
}

#[derive(Debug, Deserialize)]
struct SSLConfig<'a> {
    ca_file: &'a str,
    cert_file: &'a str,
    key_file: &'a str,
    key_pass: &'a str,
}

fn main() {
    env_logger::init();

    let args = app_from_crate!()
        .arg(
            Arg::with_name("cfg")
                .long("cfg")
                .takes_value(true)
                .required(true),
        )
        .arg(Arg::with_name("current").short("c").long("current"))
        .arg(Arg::with_name("nofollow").long("no-follow"))
        .get_matches();

    let mut cfg_data = Vec::new();
    File::open(args.value_of("cfg").unwrap())
        .unwrap()
        .read_to_end(&mut cfg_data)
        .unwrap();

    let cfg = toml::from_slice::<Config>(&cfg_data).unwrap();

    let mut engine = if let Some(ref neo4j) = cfg.neo4j {
        engine::Engine::new(cfg::Config {
            cfg_mode: cfg::CfgMode::Auto,
            db_server: neo4j.db_host.to_string(),
            db_user: neo4j.db_user.to_string(),
            db_password: neo4j.db_pass.to_string(),
            suppress_default_views: false,
            cfg_detail: None,
        })
    } else {
        engine::Engine::new(cfg::Config {
            cfg_mode: cfg::CfgMode::Auto,
            db_server: "".to_string(),
            db_user: "".to_string(),
            db_password: "".to_string(),
            suppress_default_views: true,
            cfg_detail: None,
        })
    };

    engine.init_pipeline().expect("Failed to init pipeline");

    let cdm_view_id = engine.register_view_type::<CDMView>().unwrap();

    engine.create_view_by_id(cdm_view_id, hashmap!("cdm_file".to_string() => Box::new("cdm.bin".to_string()) as Box<std::any::Any>)).unwrap();

    engine.init_record::<TraceEvent>().unwrap();

    if let Some(src_file) = cfg.src_file {
        engine
            .ingest_stream(File::open(src_file).unwrap().into())
            .unwrap();
    } else if let Some(kafka) = cfg.kafka {
        let nofollow = args.is_present("nofollow");

        let fetch_off = {
            if args.is_present("current") {
                FetchOffset::Latest
            } else {
                FetchOffset::Earliest
            }
        };

        let kbuilder = Consumer::from_hosts(kafka.khost.iter().map(|x| x.to_string()).collect())
            .with_topic(kafka.topic.to_string())
            .with_fallback_offset(fetch_off);

        let mut kafka = if let Some(ssl) = kafka.ssl {
            // OpenSSL offers a variety of complex configurations. Here is an example:
            let mut builder = SslConnectorBuilder::new(SslMethod::tls()).unwrap();
            {
                let mut pem = Vec::new();
                File::open(ssl.key_file)
                    .unwrap()
                    .read_to_end(&mut pem)
                    .unwrap();

                let ctx = &mut builder as &mut SslContextBuilder;
                ctx.set_cipher_list("DEFAULT").unwrap();
                ctx.set_ca_file(ssl.ca_file).unwrap();
                ctx.set_certificate_file(ssl.cert_file, X509_FILETYPE_PEM)
                    .unwrap();
                ctx.set_private_key(
                    &PKey::private_key_from_pem_passphrase(&pem, ssl.key_pass.as_bytes()).unwrap(),
                )
                .unwrap();
                ctx.set_default_verify_paths().unwrap();
                ctx.set_verify(SSL_VERIFY_PEER);
            }
            let connector = builder.build();

            kbuilder.with_security(SecurityConfig::new(connector).with_hostname_verification(false))
        } else {
            kbuilder
        }
        .create()
        .expect("Failed to create kafka client");

        let running = Arc::new(AtomicBool::new(true));
        let r = running.clone();
        ctrlc::set_handler(move || {
            r.store(false, Ordering::SeqCst);
        })
        .expect("Error setting Ctrl-C handler");

        while running.load(Ordering::SeqCst) {
            match kafka.poll() {
                Ok(mss) => {
                    if mss.is_empty() {
                        if nofollow {
                            break;
                        } else {
                            continue;
                        }
                    } else {
                        for ms in mss.iter() {
                            for m in ms.messages() {
                                match serde_json::from_slice::<TraceEvent>(m.value) {
                                    Ok(ref tr) => match engine.ingest_record(tr) {
                                        Ok(_) => (),
                                        Err(e) => {
                                            eprintln!("Offset: {}", m.offset);
                                            eprintln!("PVM error: {}", e);
                                            eprintln!("{}", tr);
                                        }
                                    },
                                    Err(perr) => {
                                        eprintln!("Offset: {}", m.offset);
                                        eprintln!("JSON Parsing error: {}", perr);
                                        eprintln!("{}", String::from_utf8_lossy(m.value));
                                    }
                                }
                            }
                            match kafka.consume_messageset(ms) {
                                Ok(_) => {}
                                Err(e) => {
                                    eprintln!("Error: {}", e);
                                }
                            }
                        }
                    }
                }
                Err(e) => {
                    eprintln!("Error: {}", e);
                }
            }
        }
    } else {
        eprintln!("Please supply either kafka or src_file details in your cfg file.")
    }

    engine
        .shutdown_pipeline()
        .expect("Failed to shutdown pipeline");
}
