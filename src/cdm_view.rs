use std::{
    any::Any,
    collections::HashMap,
    fs::File,
    io::Read,
    sync::{mpsc::Receiver, Arc},
    thread,
};

use opus::{
    cfg,
    data::{
        node_types::{NameNode, Node, PVMDataType, SchemaNode},
        rel_types::Rel,
        HasDst, HasID, HasSrc, ID,
    },
    views::{DBTr, View, ViewInst},
};

use uuid::Uuid;

use avro_rs::{
    types::{Record as AvroRecord, ToAvro, Value as AvroValue},
    Schema, Writer,
};
use lazy_static::lazy_static;
use maplit::{convert_args, hashmap};

fn load_schema(src: &'static str) -> Schema {
    let mut schema_data = String::new();
    File::open(src)
        .unwrap()
        .read_to_string(&mut schema_data)
        .unwrap();
    Schema::parse_str(&schema_data).unwrap()
}

lazy_static! {
    static ref TC_CDM_DATUM_SCHEMATA: Schema = load_schema("avro/TCCDMDatum.avsc");
    static ref HOST_SCHEMATA: Schema = load_schema("avro/Host.avsc");
    static ref EVENT_SCHEMATA: Schema = load_schema("avro/Event.avsc");
    static ref SUBJECT_SCHEMATA: Schema = load_schema("avro/Subject.avsc");
    static ref SRC_SINK_OBJECT_SCHEMATA: Schema = load_schema("avro/SrcSinkObject.avsc");
    static ref ABSTRACT_OBJECT_SCHEMATA: Schema = load_schema("avro/AbstractObject.avsc");
    static ref PROVENANCE_TAG_NODE_SCHEMATA: Schema = load_schema("avro/ProvenanceTagNode.avsc");
    static ref NULL: AvroValue = { (None as Option<()>).avro() };
}

const CDMVERSION: &str = "20";

fn mkuuid(id: ID) -> Uuid {
    Uuid::new_v5(&Uuid::nil(), &format!("{:?}", id))
}

struct UuidW(Uuid);

impl UuidW {
    fn nil() -> Self {
        UuidW(Uuid::nil())
    }
}

impl ToAvro for UuidW {
    fn avro(self) -> AvroValue {
        AvroValue::Fixed(16, self.0.as_bytes().to_vec())
    }
}

#[derive(Debug)]
enum RecordType {
    Host,
    ProvenanceTagNode,
    Subject,
    SrcSinkObject,
    Event,
}

impl ToAvro for RecordType {
    fn avro(self) -> AvroValue {
        match self {
            RecordType::Host => AvroValue::Enum(0, "RECORD_HOST".into()),
            RecordType::ProvenanceTagNode => {
                AvroValue::Enum(2, "RECORD_PROVENANCE_TAG_NODE".into())
            }
            RecordType::Subject => AvroValue::Enum(4, "RECORD_SUBJECT".into()),
            RecordType::SrcSinkObject => AvroValue::Enum(11, "RECORD_SRC_SINK_OBJECT".into()),
            RecordType::Event => AvroValue::Enum(12, "RECORD_EVENT".into()),
        }
    }
}

#[derive(Debug)]
enum HostType {
    Other,
}

impl ToAvro for HostType {
    fn avro(self) -> AvroValue {
        AvroValue::Enum(3, "HOST_OTHER".into())
    }
}

#[derive(Debug)]
enum SubjectType {
    Other,
}

impl ToAvro for SubjectType {
    fn avro(self) -> AvroValue {
        AvroValue::Enum(4, "SUBJECT_OTHER".into())
    }
}

#[derive(Debug)]
enum SrcSinkType {
    Unknown,
}

impl ToAvro for SrcSinkType {
    fn avro(self) -> AvroValue {
        AvroValue::Enum(123, "SRCSINK_UNKNOWN".into())
    }
}

#[derive(Debug)]
enum EventType {
    FlowsTo,
    Other,
}

impl ToAvro for EventType {
    fn avro(self) -> AvroValue {
        match self {
            EventType::FlowsTo => AvroValue::Enum(16, "EVENT_FLOWS_TO".into()),
            EventType::Other => AvroValue::Enum(31, "EVENT_OTHER".into()),
        }
    }
}

#[derive(Debug)]
enum InstrumentationSource {
    PVMCadets,
}

impl ToAvro for InstrumentationSource {
    fn avro(self) -> AvroValue {
        AvroValue::Enum(16, "SOURCE_PVM_CADETS".into())
    }
}

#[derive(Debug)]
struct ProvenanceTagNode {
    tag_id: Uuid,
    properties: HashMap<String, String>,
}

impl ToAvro for ProvenanceTagNode {
    fn avro(self) -> AvroValue {
        let mut rec = AvroRecord::with_placeholder(&PROVENANCE_TAG_NODE_SCHEMATA, &NULL).unwrap();
        rec.put("tagId", UuidW(self.tag_id));
        rec.put("subject", UuidW::nil());
        rec.put(
            "properties",
            AvroValue::Union(Box::new(self.properties.avro())),
        );
        rec.avro()
    }
}

#[derive(Debug)]
struct Host {
    host_name: &'static str,
    ta1_version: &'static str,
    host_type: HostType,
}

impl ToAvro for Host {
    fn avro(self) -> AvroValue {
        let mut rec = AvroRecord::with_placeholder(&HOST_SCHEMATA, &NULL).unwrap();
        rec.put("uuid", UuidW::nil());
        rec.put("hostName", self.host_name);
        rec.put("hostType", self.host_type);
        rec.put("ta1Version", self.ta1_version);
        rec.avro()
    }
}

#[derive(Debug)]
struct Subject {
    uuid: Uuid,
    ty: SubjectType,
    properties: HashMap<String, String>,
}

impl ToAvro for Subject {
    fn avro(self) -> AvroValue {
        let mut rec = AvroRecord::with_placeholder(&SUBJECT_SCHEMATA, &NULL).unwrap();
        rec.put("uuid", UuidW(self.uuid));
        rec.put("type", self.ty);
        rec.put("cid", 0);
        rec.put(
            "properties",
            AvroValue::Union(Box::new(self.properties.avro())),
        );
        rec.avro()
    }
}

#[derive(Debug)]
struct AbstractObject {
    properties: HashMap<String, String>,
}

impl ToAvro for AbstractObject {
    fn avro(self) -> AvroValue {
        let mut rec = AvroRecord::with_placeholder(&ABSTRACT_OBJECT_SCHEMATA, &NULL).unwrap();
        rec.put(
            "properties",
            AvroValue::Union(Box::new(self.properties.avro())),
        );
        rec.avro()
    }
}

#[derive(Debug)]
struct SrcSinkObject {
    uuid: Uuid,
    base_object: AbstractObject,
    ty: SrcSinkType,
}

impl ToAvro for SrcSinkObject {
    fn avro(self) -> AvroValue {
        let mut rec = AvroRecord::with_placeholder(&SRC_SINK_OBJECT_SCHEMATA, &NULL).unwrap();
        rec.put("uuid", UuidW(self.uuid));
        rec.put("baseObject", self.base_object);
        rec.put("type", self.ty);
        rec.avro()
    }
}

#[derive(Debug)]
struct Event {
    uuid: Uuid,
    ty: EventType,
    subject: Option<Uuid>,
    predicate_object: Option<Uuid>,
    predicate_object2: Option<Uuid>,
    timestamp_nanos: i64,
    properties: HashMap<String, String>,
}

impl ToAvro for Event {
    fn avro(self) -> AvroValue {
        let mut rec = AvroRecord::with_placeholder(&EVENT_SCHEMATA, &NULL).unwrap();
        rec.put("uuid", UuidW(self.uuid));
        rec.put("type", self.ty);
        rec.put("subject", self.subject.map(UuidW));
        rec.put("predicateObject", self.predicate_object.map(UuidW));
        rec.put("predicateObject2", self.predicate_object2.map(UuidW));
        rec.put("timestampNanos", self.timestamp_nanos);
        rec.put(
            "properties",
            AvroValue::Union(Box::new(self.properties.avro())),
        );
        rec.avro()
    }
}

#[derive(Debug)]
enum Record {
    Host(Host),
    ProvenanceTagNode(ProvenanceTagNode),
    Subject(Subject),
    SrcSinkObject(SrcSinkObject),
    Event(Event),
}

impl Record {
    fn into_avro(self) -> (AvroValue, RecordType) {
        match self {
            Record::Host(v) => (AvroValue::Union(Box::new(v.avro())), RecordType::Host),
            Record::ProvenanceTagNode(v) => (
                AvroValue::Union(Box::new(v.avro())),
                RecordType::ProvenanceTagNode,
            ),
            Record::Subject(v) => (AvroValue::Union(Box::new(v.avro())), RecordType::Subject),
            Record::SrcSinkObject(v) => (
                AvroValue::Union(Box::new(v.avro())),
                RecordType::SrcSinkObject,
            ),
            Record::Event(v) => (AvroValue::Union(Box::new(v.avro())), RecordType::Event),
        }
    }
}

impl ToAvro for Record {
    fn avro(self) -> AvroValue {
        let (datum, ty) = self.into_avro();
        let mut rec = AvroRecord::with_placeholder(&TC_CDM_DATUM_SCHEMATA, &NULL).unwrap();
        rec.put("datum", datum);
        rec.put("CDMVersion", CDMVERSION);
        rec.put("type", ty);
        rec.put("hostId", UuidW::nil());
        rec.put("sessionNumber", 0);
        rec.put("source", InstrumentationSource::PVMCadets);
        rec.avro()
    }
}

fn dbtr_to_avro(val: &DBTr) -> impl ToAvro {
    match val {
        DBTr::CreateNode(n) | DBTr::UpdateNode(n) => match *n {
            Node::Data(ref d) => match d.pvm_ty() {
                PVMDataType::Actor => Record::Subject(Subject {
                    uuid: mkuuid(d.get_db_id()),
                    ty: SubjectType::Other,
                    properties: convert_args!(hashmap!("type" => "Node;Actor",
                                                                               "schema" => d.ty().name,
                                                                               "uuid" => d.uuid().hyphenated().to_string(),
                                                                               "ctx" => mkuuid(d.ctx()).hyphenated().to_string())),
                }),
                PVMDataType::Store => Record::SrcSinkObject(SrcSinkObject {
                    uuid: mkuuid(d.get_db_id()),
                    base_object: AbstractObject {
                        properties: convert_args!(hashmap!("type" => "Node;Object;Store",
                                                                                   "schema" => d.ty().name,
                                                                                   "uuid" => d.uuid().hyphenated().to_string(),
                                                                                   "ctx" => mkuuid(d.ctx()).hyphenated().to_string())),
                    },
                    ty: SrcSinkType::Unknown,
                }),
                PVMDataType::Conduit => Record::SrcSinkObject(SrcSinkObject {
                    uuid: mkuuid(d.get_db_id()),
                    base_object: AbstractObject {
                        properties: convert_args!(hashmap!("type" => "Node;Object;Conduit",
                                                                                   "schema" => d.ty().name,
                                                                                   "uuid" => d.uuid().hyphenated().to_string(),
                                                                                   "ctx" => mkuuid(d.ctx()).hyphenated().to_string())),
                    },
                    ty: SrcSinkType::Unknown,
                }),
                PVMDataType::EditSession => Record::SrcSinkObject(SrcSinkObject {
                    uuid: mkuuid(d.get_db_id()),
                    base_object: AbstractObject {
                        properties: convert_args!(hashmap!("type" => "Node;Object;EditSession",
                                                                                   "schema" => d.ty().name,
                                                                                   "uuid" => d.uuid().hyphenated().to_string(),
                                                                                   "ctx" => mkuuid(d.ctx()).hyphenated().to_string())),
                    },
                    ty: SrcSinkType::Unknown,
                }),
            },
            Node::Name(ref n) => {
                let (id, props): (ID, HashMap<String, String>) = match n {
                    NameNode::Path(id, pth) => (
                        *id,
                        convert_args!(hashmap!("type" => "Node;Name;Path",
                                                                                            "path" => pth.clone())),
                    ),
                    NameNode::Net(id, addr, port) => (
                        *id,
                        convert_args!(hashmap!("type" => "Node;Name;Net",
                                                                                                  "addr" => addr.clone(),
                                                                                                  "port" => port.to_string())),
                    ),
                };
                Record::ProvenanceTagNode(ProvenanceTagNode {
                    tag_id: mkuuid(id),
                    properties: props,
                })
            }
            Node::Ctx(ref c) => {
                let mut props: HashMap<String, String> = convert_args!(
                    hashmap!("type" => "Node;Context",
                                                                                                "schema" => c.ty().name)
                );
                for (k, v) in &c.cont {
                    props.insert((*k).into(), v.clone());
                }
                Record::ProvenanceTagNode(ProvenanceTagNode {
                    tag_id: mkuuid(c.get_db_id()),
                    properties: props,
                })
            }
            Node::Schema(ref s) => {
                let (id, props): (ID, HashMap<String, String>) = match s {
                    SchemaNode::Data(id, ty) => {
                        let p = ty.props.keys().cloned().collect::<Vec<_>>();
                        (
                            *id,
                            convert_args!(hashmap!("type" => "Node;Schema",
                                                                     "name" => ty.name.to_string(),
                                                                     "base" => ty.pvm_ty.to_string(),
                                                                     "props" => p.join(";"))),
                        )
                    }
                    SchemaNode::Context(id, ty) => {
                        let p = ty.props.to_vec();
                        (
                            *id,
                            convert_args!(hashmap!("type" => "Node;Schema",
                                                                     "name" => ty.name.to_string(),
                                                                     "base" => "Context",
                                                                     "props" => p.join(";"))),
                        )
                    }
                };
                Record::ProvenanceTagNode(ProvenanceTagNode {
                    tag_id: mkuuid(id),
                    properties: props,
                })
            }
        },
        DBTr::CreateRel(r) | DBTr::UpdateRel(r) => match *r {
            Rel::Inf(ref i) => Record::Event(Event {
                uuid: mkuuid(r.get_db_id()),
                ty: EventType::FlowsTo,
                subject: None,
                predicate_object: Some(mkuuid(r.get_src())),
                predicate_object2: Some(mkuuid(r.get_dst())),
                timestamp_nanos: 0,
                properties: convert_args!(hashmap!("type" => "INF",
                                                                       "ctx" => mkuuid(i.ctx).hyphenated().to_string())),
            }),
            Rel::Named(ref n) => Record::Event(Event {
                uuid: mkuuid(r.get_db_id()),
                ty: EventType::Other,
                subject: Some(mkuuid(r.get_src())),
                predicate_object: Some(mkuuid(r.get_dst())),
                predicate_object2: None,
                timestamp_nanos: 0,
                properties: convert_args!(hashmap!("type" => "NAMED",
                                                                       "start" => mkuuid(n.start).hyphenated().to_string(),
                                                                       "end" => mkuuid(n.end).hyphenated().to_string())),
            }),
        },
    }
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
        hashmap!("cdm_file" => "CDM file location")
    }
    fn create(
        &self,
        id: usize,
        params: HashMap<String, Box<Any>>,
        _cfg: &cfg::Config,
        stream: Receiver<Arc<DBTr>>,
    ) -> ViewInst {
        let cdm_path = params["cdm_file"]
            .downcast_ref::<String>()
            .unwrap()
            .to_string();
        let thr = thread::spawn(move || {
            let mut writer = Writer::new(&TC_CDM_DATUM_SCHEMATA, File::create(cdm_path).unwrap());

            writer
                .append(Record::Host(Host {
                    host_name: "",
                    ta1_version: "",
                    host_type: HostType::Other,
                }))
                .unwrap();
            for tr in stream {
                writer.append(dbtr_to_avro(tr.as_ref())).unwrap();
            }
            writer.flush().unwrap();
        });
        ViewInst {
            id,
            vtype: self.id,
            params,
            handle: thr,
        }
    }
}
