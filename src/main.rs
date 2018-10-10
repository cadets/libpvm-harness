#[macro_use]
extern crate clap;
#[macro_use]
extern crate maplit;
#[macro_use]
extern crate serde_derive;
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

use std::{
    collections::HashMap,
    fs::File,
    io::Read,
    sync::{
        atomic::{AtomicBool, Ordering},
        mpsc::Receiver,
        Arc,
    },
    thread,
};

use clap::Arg;
use kafka::{client::SecurityConfig, consumer::Consumer};
use openssl::{
    pkey::PKey,
    ssl::{SslConnectorBuilder, SslContextBuilder, SslMethod, SSL_VERIFY_PEER},
    x509::X509_FILETYPE_PEM,
};

use opus::{
    cfg, engine,
    trace::cadets::TraceEvent,
    views::{DBTr, View, ViewInst},
};

#[derive(Debug, Deserialize)]
struct Config<'a> {
    khost: Vec<String>,
    topic: String,
    #[serde(borrow)]
    neo4j: Neo4jConfig<'a>,
    #[serde(borrow)]
    ssl: SSLConfig<'a>,
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

#[derive(Debug)]
pub struct CDMView {
    id: usize,
}

impl View for CDMView {
    fn new(id: usize) -> CDMView {
        CDMView { id }
    }
    fn id(&self) -> usize {
        self.id
    }
    fn name(&self) -> &'static str {
        "CDMView"
    }
    fn desc(&self) -> &'static str {
        "View for producing CDM data to kafka."
    }
    fn params(&self) -> HashMap<&'static str, &'static str> {
        hashmap!()
    }
    fn create(
        &self,
        id: usize,
        params: HashMap<String, String>,
        _cfg: &cfg::Config,
        _stream: Receiver<Arc<DBTr>>,
    ) -> ViewInst {
        let thr = thread::spawn(move || {});
        ViewInst {
            id,
            vtype: self.id,
            params,
            handle: thr,
        }
    }
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
        .arg(Arg::with_name("print").short("p").long("printing"))
        .arg(Arg::with_name("ingest").short("i").long("ingestion"))
        .get_matches();

    let mut cfg_data = Vec::new();
    File::open(args.value_of("cfg").unwrap())
        .unwrap()
        .read_to_end(&mut cfg_data)
        .unwrap();

    let cfg = toml::from_slice::<Config>(&cfg_data).unwrap();

    let print = args.is_present("print");
    let ingest = args.is_present("ingest");

    let running = Arc::new(AtomicBool::new(true));
    let r = running.clone();
    ctrlc::set_handler(move || {
        r.store(false, Ordering::SeqCst);
    }).expect("Error setting Ctrl-C handler");

    // OpenSSL offers a variety of complex configurations. Here is an example:
    let mut builder = SslConnectorBuilder::new(SslMethod::tls()).unwrap();
    {
        let mut pem = Vec::new();
        File::open(cfg.ssl.key_file)
            .unwrap()
            .read_to_end(&mut pem)
            .unwrap();

        let ctx = &mut builder as &mut SslContextBuilder;
        ctx.set_cipher_list("DEFAULT").unwrap();
        ctx.set_ca_file(cfg.ssl.ca_file).unwrap();
        ctx.set_certificate_file(cfg.ssl.cert_file, X509_FILETYPE_PEM)
            .unwrap();
        ctx.set_private_key(
            &PKey::private_key_from_pem_passphrase(&pem, cfg.ssl.key_pass.as_bytes()).unwrap(),
        ).unwrap();
        ctx.set_default_verify_paths().unwrap();
        ctx.set_verify(SSL_VERIFY_PEER);
    }
    let connector = builder.build();

    let mut kafka = Consumer::from_hosts(cfg.khost)
        .with_topic(cfg.topic)
        .with_security(SecurityConfig::new(connector))
        .create()
        .expect("Failed to create kafka client");

    let mut engine = None;

    if ingest {
        engine = Some(engine::Engine::new(cfg::Config {
            cfg_mode: cfg::CfgMode::Auto,
            db_server: cfg.neo4j.db_host.to_string(),
            db_user: cfg.neo4j.db_user.to_string(),
            db_password: cfg.neo4j.db_pass.to_string(),
            suppress_default_views: false,
            cfg_detail: None,
        }));

        engine
            .as_mut()
            .unwrap()
            .init_pipeline()
            .expect("Failed to init pipeline");

        let cdm_view_id = engine
            .as_mut()
            .unwrap()
            .register_view_type::<CDMView>()
            .unwrap();

        engine
            .as_mut()
            .unwrap()
            .create_view_by_id(cdm_view_id, hashmap!())
            .unwrap();
    }

    while running.load(Ordering::SeqCst) {
        match kafka.poll() {
            Ok(mss) => {
                if mss.is_empty() {
                    continue;
                } else {
                    for ms in mss.iter() {
                        for m in ms.messages() {
                            if print {
                                println!(
                                    "Topic: {}, Partition: {}, Offset: {}, Key: {}, Value: {}",
                                    ms.topic(),
                                    ms.partition(),
                                    m.offset,
                                    String::from_utf8_lossy(m.key),
                                    String::from_utf8_lossy(m.value)
                                );
                            }
                            if ingest {
                                let eng = engine.as_mut().unwrap();
                                match serde_json::from_slice::<TraceEvent>(m.value) {
                                    Ok(ref tr) => match eng.ingest_record(tr) {
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
    if ingest {
        engine
            .as_mut()
            .unwrap()
            .shutdown_pipeline()
            .expect("Failed to shutdown pipeline");
    }
}
