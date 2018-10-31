use std::{
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

fn load_schema(src: &'static str) -> Schema {
    let mut schema_data = String::new();
    File::open(src)
        .unwrap()
        .read_to_string(&mut schema_data)
        .unwrap();
    Schema::parse_str(&schema_data).unwrap()
}

lazy_static! {
    static ref TCCDMDatumSchemata: Schema = load_schema("avro/TCCDMDatum.avsc");
    static ref HostSchemata: Schema = load_schema("avro/Host.avsc");
    static ref EventSchemata: Schema = load_schema("avro/Event.avsc");
    static ref SubjectSchemata: Schema = load_schema("avro/Subject.avsc");
    static ref SrcSinkObjectSchemata: Schema = load_schema("avro/SrcSinkObject.avsc");
    static ref AbstractObjectSchemata: Schema = load_schema("avro/AbstractObject.avsc");
    static ref ProvenanceTagNodeSchemata: Schema = load_schema("avro/ProvenanceTagNode.avsc");
    static ref NULL: AvroValue = { (None as Option<()>).avro() };
}

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
        AvroValue::Fixed(16, self.0.as_bytes().into_iter().cloned().collect())
    }
}

#[derive(Debug)]
enum RecordType {
    RecordHost,
    RecordProvenanceTagNode,
    RecordSubject,
    RecordSrcSinkObject,
    RecordEvent,
}

impl ToAvro for RecordType {
    fn avro(self) -> AvroValue {
        match self {
            RecordType::RecordHost => AvroValue::Enum(0, "RECORD_HOST".into()),
            RecordType::RecordProvenanceTagNode => {
                AvroValue::Enum(2, "RECORD_PROVENANCE_TAG_NODE".into())
            }
            RecordType::RecordSubject => AvroValue::Enum(3, "RECORD_SUBBJECT".into()),
            RecordType::RecordSrcSinkObject => AvroValue::Enum(10, "RECORD_SRC_SINK_OBJECT".into()),
            RecordType::RecordEvent => AvroValue::Enum(1, "RECORD_EVENT".into()),
        }
    }
}

#[derive(Debug)]
enum HostType {
    HostServer,
}

impl ToAvro for HostType {
    fn avro(self) -> AvroValue {
        AvroValue::Enum(1, "HOST_SERVER".into())
    }
}

#[derive(Debug)]
enum SubjectType {
    SubjectProcess,
}

impl ToAvro for SubjectType {
    fn avro(self) -> AvroValue {
        AvroValue::Enum(0, "SUBJECT_PROCESS".into())
    }
}

#[derive(Debug)]
enum SrcSinkType {
    SrcsinkUnknown,
}

impl ToAvro for SrcSinkType {
    fn avro(self) -> AvroValue {
        AvroValue::Enum(123, "SRCSINK_UNKNOWN".into())
    }
}

#[derive(Debug)]
enum EventType {
    EventFlowsTo,
    EventOther,
}

impl ToAvro for EventType {
    fn avro(self) -> AvroValue {
        match self {
            EventType::EventFlowsTo => AvroValue::Enum(16, "EVENT_FLOWS_TO".into()),
            EventType::EventOther => AvroValue::Enum(31, "EVENT_OTHER".into()),
        }
    }
}

#[derive(Debug)]
enum InstrumentationSource {
    SourceFreebsdDtraceCadets,
}

impl ToAvro for InstrumentationSource {
    fn avro(self) -> AvroValue {
        AvroValue::Enum(3, "SOURCE_FREEBSD_DTRACE_CADETS".into())
    }
}

const CDMVERSION: &'static str = "19";

#[derive(Debug)]
struct ProvenanceTagNode {
    tag_id: Uuid,
    properties: HashMap<String, String>,
}

impl ToAvro for ProvenanceTagNode {
    fn avro(self) -> AvroValue {
        let mut rec = AvroRecord::new(&ProvenanceTagNodeSchemata, Some(&NULL)).unwrap();
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
    host_type: HostType,
}

impl ToAvro for Host {
    fn avro(self) -> AvroValue {
        let mut rec = AvroRecord::new(&HostSchemata, Some(&NULL)).unwrap();
        rec.put("uuid", UuidW::nil());
        rec.put("hostName", self.host_name);
        rec.put("hostType", self.host_type);
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
        let mut rec = AvroRecord::new(&SubjectSchemata, Some(&NULL)).unwrap();
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
        let mut rec = AvroRecord::new(&AbstractObjectSchemata, Some(&NULL)).unwrap();
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
        let mut rec = AvroRecord::new(&SrcSinkObjectSchemata, Some(&NULL)).unwrap();
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
        let mut rec = AvroRecord::new(&EventSchemata, Some(&NULL)).unwrap();
        rec.put("uuid", UuidW(self.uuid));
        rec.put("type", self.ty);
        rec.put("subject", self.subject.map(|v| UuidW(v)));
        rec.put("predicateObject", self.predicate_object.map(|v| UuidW(v)));
        rec.put("predicateObject2", self.predicate_object2.map(|v| UuidW(v)));
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
            Record::Host(v) => (AvroValue::Union(Box::new(v.avro())), RecordType::RecordHost),
            Record::ProvenanceTagNode(v) => (
                AvroValue::Union(Box::new(v.avro())),
                RecordType::RecordProvenanceTagNode,
            ),
            Record::Subject(v) => (
                AvroValue::Union(Box::new(v.avro())),
                RecordType::RecordSubject,
            ),
            Record::SrcSinkObject(v) => (
                AvroValue::Union(Box::new(v.avro())),
                RecordType::RecordSrcSinkObject,
            ),
            Record::Event(v) => (
                AvroValue::Union(Box::new(v.avro())),
                RecordType::RecordEvent,
            ),
        }
    }
}

impl ToAvro for Record {
    fn avro(self) -> AvroValue {
        let (datum, ty) = self.into_avro();
        let mut rec = AvroRecord::new(&TCCDMDatumSchemata, Some(&NULL)).unwrap();
        rec.put("datum", datum);
        rec.put("CDMVersion", CDMVERSION);
        rec.put("type", ty);
        rec.put("hostId", UuidW::nil());
        rec.put("sessionNumber", 0);
        rec.put("source", InstrumentationSource::SourceFreebsdDtraceCadets);
        rec.avro()
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
        hashmap!()
    }
    fn create(
        &self,
        id: usize,
        params: HashMap<String, String>,
        _cfg: &cfg::Config,
        stream: Receiver<Arc<DBTr>>,
    ) -> ViewInst {
        let thr = thread::spawn(move || {
            let mut writer = Writer::new(&TCCDMDatumSchemata, File::create("cdm.bin").unwrap());

            writer
                .append(Record::Host(Host {
                    host_name: "",
                    host_type: HostType::HostServer,
                }))
                .unwrap();

            for tr in stream {
                writer.append(match tr.as_ref() {
                    DBTr::CreateNode(n) | DBTr::UpdateNode(n) => {
                        match *n {
                            Node::Data(ref d) => {
                                match d.pvm_ty() {
                                    PVMDataType::Actor => {
                                        Record::Subject(Subject {
                                            uuid: mkuuid(d.get_db_id()),
                                            ty: SubjectType::SubjectProcess,
                                            properties: convert_args!(hashmap!("type" => "Node;Actor",
                                                                               "schema" => d.ty().name,
                                                                               "uuid" => d.uuid().hyphenated().to_string(),
                                                                               "ctx" => mkuuid(d.ctx()).hyphenated().to_string())),
                                        })
                                    }
                                    PVMDataType::Store => {
                                        Record::SrcSinkObject(SrcSinkObject{
                                            uuid: mkuuid(d.get_db_id()),
                                            base_object: AbstractObject {
                                                properties: convert_args!(hashmap!("type" => "Node;Object;Store",
                                                                                   "schema" => d.ty().name,
                                                                                   "uuid" => d.uuid().hyphenated().to_string(),
                                                                                   "ctx" => mkuuid(d.ctx()).hyphenated().to_string())),
                                            },
                                            ty: SrcSinkType::SrcsinkUnknown,
                                        })
                                    }
                                    PVMDataType::Conduit => {
                                        Record::SrcSinkObject(SrcSinkObject{
                                            uuid: mkuuid(d.get_db_id()),
                                            base_object: AbstractObject {
                                                properties: convert_args!(hashmap!("type" => "Node;Object;Conduit",
                                                                                   "schema" => d.ty().name,
                                                                                   "uuid" => d.uuid().hyphenated().to_string(),
                                                                                   "ctx" => mkuuid(d.ctx()).hyphenated().to_string())),
                                            },
                                            ty: SrcSinkType::SrcsinkUnknown,
                                        })
                                    }
                                    PVMDataType::EditSession => {
                                        Record::SrcSinkObject(SrcSinkObject{
                                            uuid: mkuuid(d.get_db_id()),
                                            base_object: AbstractObject {
                                                properties: convert_args!(hashmap!("type" => "Node;Object;EditSession",
                                                                                   "schema" => d.ty().name,
                                                                                   "uuid" => d.uuid().hyphenated().to_string(),
                                                                                   "ctx" => mkuuid(d.ctx()).hyphenated().to_string())),
                                            },
                                            ty: SrcSinkType::SrcsinkUnknown,
                                        })
                                    }
                                    PVMDataType::StoreCont => unreachable!(),
                                }
                            },
                            Node::Name(ref n) => {
                                let (id, props): (ID, HashMap<String, String>) = match n {
                                    NameNode::Path(id, pth) => (*id, convert_args!(hashmap!("type" => "Node;Name;Path",
                                                                                            "path" => pth.clone()))),
                                    NameNode::Net(id, addr, port) => (*id, convert_args!(hashmap!("type" => "Node;Name;Net",
                                                                                                  "addr" => addr.clone(),
                                                                                                  "port" => port.to_string()))),
                                };
                                Record::ProvenanceTagNode(
                                    ProvenanceTagNode {
                                        tag_id: mkuuid(id),
                                        properties: props,
                                    }
                                )
                            },
                            Node::Ctx(ref c) => {
                                let mut props: HashMap<String, String> = convert_args!(hashmap!("type" => "Node;Context",
                                                                                                "schema" => c.ty().name));
                                for (k, v) in &c.cont {
                                    props.insert((*k).into(), v.clone());
                                }
                                Record::ProvenanceTagNode(
                                    ProvenanceTagNode {
                                        tag_id: mkuuid(c.get_db_id()),
                                        properties: props,
                                    }
                                )
                            },
                            Node::Schema(ref s) => {
                                let (id, props): (ID, HashMap<String, String>) = match s {
                                    SchemaNode::Data(id, ty) => {
                                        let p: Vec<&str> = ty.props.keys().cloned().collect();
                                        (*id, convert_args!(hashmap!("type" => "Node;Schema",
                                                                     "name" => ty.name.clone(),
                                                                     "base" => ty.pvm_ty.to_string(),
                                                                     "props" => p.join(";"))))
                                    },
                                    SchemaNode::Context(id, ty) => {
                                        let p: Vec<&str> = ty.props.iter().cloned().collect();
                                        (*id, convert_args!(hashmap!("type" => "Node;Schema",
                                                                     "name" => ty.name.clone(),
                                                                     "base" => "Context",
                                                                     "props" => p.join(";"))))
                                    },
                                };
                                Record::ProvenanceTagNode(
                                    ProvenanceTagNode {
                                        tag_id: mkuuid(id),
                                        properties: props,
                                    }
                                )
                            },
                        }
                    },
                    DBTr::CreateRel(r) | DBTr::UpdateRel(r) => {
                        match *r {
                            Rel::Inf(ref i) => {
                                Record::Event(Event {
                                    uuid: mkuuid(r.get_db_id()),
                                    ty: EventType::EventFlowsTo,
                                    subject: None,
                                    predicate_object: Some(mkuuid(r.get_src())),
                                    predicate_object2: Some(mkuuid(r.get_dst())),
                                    timestamp_nanos: 0,
                                    properties: convert_args!(hashmap!("type" => "INF",
                                                                       "ctx" => mkuuid(i.ctx).hyphenated().to_string()))
                                })
                            }
                            Rel::Named(ref n) => {
                                Record::Event(Event {
                                    uuid: mkuuid(r.get_db_id()),
                                    ty: EventType::EventOther,
                                    subject: Some(mkuuid(r.get_src())),
                                    predicate_object: Some(mkuuid(r.get_dst())),
                                    predicate_object2: None,
                                    timestamp_nanos: 0,
                                    properties: convert_args!(hashmap!("type" => "NAMED",
                                                                       "start" => mkuuid(n.start).hyphenated().to_string(),
                                                                       "end" => mkuuid(n.end).hyphenated().to_string()))
                                })
                            }
                        }
                    },
                }).unwrap();
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
