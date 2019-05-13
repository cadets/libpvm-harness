use std::{
    cell::RefCell,
    collections::{HashMap, HashSet},
    fs::File,
    rc::Rc,
    sync::{mpsc::Receiver, Arc},
    thread,
};

use opus::{
    cfg,
    data::{
        node_types::{DataNode, NameNode, Node, PVMDataType},
        rel_types::Rel,
        HasSrc, HasDst, HasID,
    },
    views::{DBTr, View, ViewInst, ViewParams, ViewParamsExt},
};

use maplit::hashmap;
use uuid::Uuid;

#[derive(PartialEq)]
struct Act {
    uuid: Uuid,
    pid: Option<i64>,
    cmdline: Option<String>,
}

impl Act {
    fn from_actor(val: &DataNode) -> Rc<RefCell<Self>> {
        Rc::new(RefCell::new(Act {
            uuid: val.uuid(),
            pid: val.meta.cur("pid").map(|v| v.parse::<i64>().unwrap()),
            cmdline: val.meta.cur("cmdline").map(ToString::to_string),
        }))
    }
}

struct Conduit {
    uuid: Uuid,
    acts: Vec<Rc<RefCell<Act>>>,
}

#[derive(Debug)]
pub struct NetworkTrafficView {
    id: usize,
}

impl View for NetworkTrafficView {
    fn new(id: usize) -> NetworkTrafficView {
        NetworkTrafficView { id }
    }
    fn id(&self) -> usize {
        self.id
    }
    fn name(&self) -> &'static str {
        "NetworkTrafficView"
    }
    fn desc(&self) -> &'static str {
        "View for storing a network traffic log."
    }
    fn params(&self) -> HashMap<&'static str, &'static str> {
        hashmap!("output" => "Output file location")
    }
    fn create(
        &self,
        id: usize,
        params: ViewParams,
        _cfg: &cfg::Config,
        stream: Receiver<Arc<DBTr>>,
    ) -> ViewInst {
        let path = params.get_or_def("output", "./network.log");
        let mut _out = File::create(path).unwrap();
        let thr = thread::spawn(move || {
            let mut actors = HashMap::new();
            let mut conduits = HashSet::new();
            let mut addrs = HashMap::new();
            for tr in stream {
                match tr.as_ref() {
                    DBTr::CreateNode(n) | DBTr::UpdateNode(n) => {
                        if let Node::Data(n) = n {
                            let id = n.get_db_id();
                            match *n.pvm_ty() {
                                PVMDataType::Actor => {
                                    let act = Act::from_actor(n);
                                    if !actors.contains_key(&id) {
                                        actors.insert(id, act);
                                    } else if actors[&id] != act {
                                        actors[&id].swap(&act)
                                    }
                                }
                                PVMDataType::Conduit => {
                                    if !conduits.contains(&id) {
                                        conduits.insert(id);
                                    }
                                }
                                _ => {}
                            }
                        } else if let Node::Name(n) = n {
                            match n {
                                NameNode::Net(id, addr, port) => {
                                    let addr = format!("{}:{}", addr, port);
                                    addrs.insert(*id, addr);
                                }
                                _ => {}
                            }
                        }
                    }
                    DBTr::CreateRel(r) => {
                        let src = r.get_src();
                        let dst = r.get_dst();
                        match r {
                            Rel::Inf(r) => {
                                if conduits.contains(&src) {
                                    if actors.contains_key(&dst) {

                                    }
                                } else if conduits.contains(&dst) {
                                    if actors.contains_key(&src) {

                                    }
                                }
                            }
                            Rel::Named(r) => {
                                if conduits.contains(&src) && addrs.contains_key(&dst) {

                                }
                            }
                        }
                    },
                    _ => {}
                }
            }
        });
        ViewInst {
            id,
            vtype: self.id,
            params,
            handle: thr,
        }
    }
}
