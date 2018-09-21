extern crate kafka;
extern crate opus;
extern crate prometheus;
#[macro_use]
extern crate clap;
extern crate ctrlc;
extern crate serde_json;
extern crate log;
extern crate env_logger;

use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};

use clap::Arg;
use kafka::consumer::Consumer;

use opus::{cfg, engine, trace::cadets::TraceEvent};

fn main() {
    env_logger::init();
    
    let args = app_from_crate!()
        .arg(
            Arg::with_name("khost")
                .long("khost")
                .multiple(true)
                .takes_value(true)
                .required(true),
        )
        .arg(
            Arg::with_name("topic")
                .long("topic")
                .required(true)
                .takes_value(true),
        )
        .arg(
            Arg::with_name("db-host")
                .long("db-host")
                .takes_value(true),
        )
        .arg(
            Arg::with_name("db-user")
                .long("db-user")
                .takes_value(true),
        )
        .arg(
            Arg::with_name("db-pass")
                .long("db-pass")
                .takes_value(true),
        )
        .arg(
            Arg::with_name("print")
                .short("p")
                .long("printing")
                .required_unless("ingest"),
        )
        .arg(
            Arg::with_name("ingest")
                .short("i")
                .long("ingestion")
                .requires_all(&["db-host", "db-user", "db-pass"]),
        )
        .get_matches();

    let hosts = args
        .values_of("khost")
        .unwrap()
        .map(|v| v.to_string())
        .collect();
    let topic = args.value_of("topic").unwrap().to_string();
    let print = args.is_present("print");
    let ingest = args.is_present("ingest");

    let running = Arc::new(AtomicBool::new(true));
    let r = running.clone();
    ctrlc::set_handler(move || {
        r.store(false, Ordering::SeqCst);
    }).expect("Error setting Ctrl-C handler");

    let mut kafka = Consumer::from_hosts(hosts)
        .with_topic(topic)
        .create()
        .expect("Failed to create kafka client");

    let mut engine = None;

    if ingest {
        let db_host = args.value_of("db-host").unwrap().to_string();
        let db_user = args.value_of("db-user").unwrap().to_string();
        let db_pass = args.value_of("db-pass").unwrap().to_string();

        engine = Some(engine::Engine::new(cfg::Config {
            cfg_mode: cfg::CfgMode::Auto,
            db_server: db_host,
            db_user: db_user,
            db_password: db_pass,
            suppress_default_views: false,
            cfg_detail: None,
        }));

        engine
            .as_mut()
            .unwrap()
            .init_pipeline()
            .expect("Failed to init pipeline");
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
