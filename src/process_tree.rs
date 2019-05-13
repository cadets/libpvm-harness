use std::{
    collections::HashMap,
    fs::File,
    io::{Seek, SeekFrom, Write},
    sync::{mpsc::Receiver, Arc},
    thread,
};

use opus::{
    cfg,
    data::{
        node_types::{Node, PVMDataType},
        rel_types::Rel,
        HasDst, HasID, HasSrc, ID,
    },
    views::{DBTr, View, ViewInst, ViewParams, ViewParamsExt},
};

use maplit::hashmap;
use serde_json::json;

trait WriteMap: Send {
    fn write_node(&mut self, id: ID, label: &str);
    fn write_rel(&mut self, src: ID, dst: ID);
}

struct DotFile<W: Write + Send + Seek>(W);

impl<W: Write + Send + Seek> DotFile<W> {
    fn new(mut f: W) -> Self {
        writeln!(f, "digraph {{").unwrap();
        write!(f, "}}").unwrap();
        f.flush().unwrap();
        DotFile(f)
    }
}

impl<W: Write + Send + Seek> WriteMap for DotFile<W> {
    fn write_node(&mut self, id: ID, label: &str) {
        self.0.seek(SeekFrom::Current(-1)).unwrap();
        writeln!(
            self.0,
            "\"{}\" [label=\"{}\"];",
            id.inner(),
            label.replace("\"", "\\\"")
        )
        .unwrap();
        write!(self.0, "}}").unwrap();
        self.0.flush().unwrap();
    }

    fn write_rel(&mut self, src: ID, dst: ID) {
        self.0.seek(SeekFrom::Current(-1)).unwrap();
        writeln!(self.0, "\"{}\"  -> \"{}\";", src.inner(), dst.inner()).unwrap();
        write!(self.0, "}}").unwrap();
        self.0.flush().unwrap();
    }
}

struct JSONFile<W: Write + Send>(W);

impl<W: Write + Send> JSONFile<W> {
    fn new(f: W) -> Self {
        JSONFile(f)
    }
}

impl<W: Write + Send> WriteMap for JSONFile<W> {
    fn write_node(&mut self, id: ID, label: &str) {
        writeln!(
            self.0,
            "{}",
            json!({
                "type": "node",
                "id": id.inner(),
                "label": label,
            })
        )
        .unwrap();
        self.0.flush().unwrap();
    }

    fn write_rel(&mut self, src: ID, dst: ID) {
        writeln!(
            self.0,
            "{}",
            json!({
                "type": "rel",
                "src": src.inner(),
                "dst": dst.inner(),
            })
        )
        .unwrap();
        self.0.flush().unwrap();
    }
}

#[derive(Debug)]
pub struct ProcTreeView {
    id: usize,
}

fn neq(a: &Option<&str>, b: &Option<String>) -> bool {
    match a {
        Some(va) => match b {
            Some(vb) => *va != *vb,
            None => true,
        },
        None => b.is_some(),
    }
}

impl View for ProcTreeView {
    fn new(id: usize) -> ProcTreeView {
        ProcTreeView { id }
    }
    fn id(&self) -> usize {
        self.id
    }
    fn name(&self) -> &'static str {
        "ProcTreeView"
    }
    fn desc(&self) -> &'static str {
        "View for storing a process tree."
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
        let path = params.get_or_def("output", "./proc_tree.json");
        let meta_key = params.get_or_def("meta_key", "cmdline").to_string();
        let fmt = params.get_or_def("fmt", "json");
        let mut out: Box<WriteMap> = if fmt == "json" {
            Box::new(JSONFile::new(File::create(path).unwrap()))
        } else if fmt == "dot" {
            Box::new(DotFile::new(File::create(path).unwrap()))
        } else {
            unimplemented!()
        };
        let thr = thread::spawn(move || {
            let mut nodes = HashMap::new();
            for tr in stream {
                match *tr {
                    DBTr::CreateNode(ref n) | DBTr::UpdateNode(ref n) => {
                        if let Node::Data(n) = n {
                            if *n.pvm_ty() == PVMDataType::Actor {
                                let id = n.get_db_id();
                                let cmd = n.meta.cur(&meta_key);
                                if !nodes.contains_key(&id) || neq(&cmd, &nodes[&id]) {
                                    let cmd = cmd.map(|v| v.to_string());
                                    if let Some(cmd) = &cmd {
                                        out.write_node(n.get_db_id(), cmd);
                                    } else {
                                        out.write_node(n.get_db_id(), "???");
                                    }
                                    nodes.insert(id, cmd);
                                }
                            }
                        }
                    }
                    DBTr::CreateRel(ref r) => {
                        if let Rel::Inf(r) = r {
                            if nodes.contains_key(&r.get_src()) && nodes.contains_key(&r.get_dst())
                            {
                                out.write_rel(r.get_src(), r.get_dst());
                            }
                        }
                    }
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
