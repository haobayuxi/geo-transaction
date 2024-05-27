use std::time::Duration;

use common::{get_txnid, CID_LEN};
use rpc::common::{Msg, ReadStruct};
use tokio::{
    sync::{
        mpsc::{Receiver, UnboundedSender},
        oneshot::Sender,
        RwLock,
    },
    time::sleep,
};

use crate::data::DATA;

pub static mut TXNS: Vec<Vec<Node>> = Vec::new();

#[derive(Debug)]
pub struct Node {
    pub executed: bool,
    pub committed: bool,
    pub waiting_dep: i32,
    pub notify: RwLock<Vec<UnboundedSender<u64>>>,
    // msg: Msg,
    pub txn: Option<Msg>,
    pub callback: Option<Sender<Msg>>,
    // tarjan
    pub dfn: i32,
    pub low: i32,
}

impl Node {
    pub fn new(txn: Msg) -> Self {
        Self {
            executed: false,
            committed: false,
            txn: Some(txn),
            callback: None,
            notify: RwLock::new(Vec::new()),
            dfn: -1,
            low: -1,
            waiting_dep: 0,
        }
    }
    pub fn default() -> Self {
        Self {
            executed: true,
            committed: true,
            txn: None,
            callback: None,
            notify: RwLock::new(Vec::new()),
            dfn: -1,
            low: -1,
            waiting_dep: 0,
        }
    }
}
pub struct DepGraph {
    // wait list
    wait_list: Receiver<u64>,

    // stack for tarjan
    stack: Vec<u64>,
    index: i32,
    visit: i32,
}

impl DepGraph {
    pub fn new(wait_list: Receiver<u64>) -> Self {
        Self {
            wait_list,
            stack: Vec::new(),
            index: 0,
            visit: 0,
        }
    }

    pub async fn run(&mut self) {
        loop {
            match self.wait_list.recv().await {
                Some(txnid) => {
                    self.execute_txn(txnid).await;
                }
                None => {
                    sleep(Duration::from_nanos(10)).await;
                }
            }
        }
    }

    async fn apply(&mut self, txnids: Vec<u64>) {
        unsafe {
            for txnid in txnids {
                let (client_id, index) = get_txnid(txnid);

                // println!("try to execute {},{}", client_id, index);
                let node = &mut TXNS[client_id as usize][index as usize];
                {
                    let notifies = node.notify.write().await;
                    if node.executed {
                        return;
                    }

                    node.executed = true;
                }

                let txn = node.txn.as_ref().unwrap();

                let mut reply = Msg::default();
                // for read in txn.read_set.iter() {
                //     if read.table_id > 8 {
                //         println!("{} {:?}", txn.txn_id, txn.read_set);
                //     }
                //     let table = &mut DATA[read.table_id as usize];
                //     match table.get_mut(&read.key) {
                //         Some(rwlock) => {
                //             let guard = rwlock.write().await;

                //             let read_struct = ReadStruct {
                //                 key: read.key,
                //                 table_id: read.table_id,
                //                 value: Some(guard.data.clone()),
                //                 timestamp: Some(guard.ts),
                //             };
                //             reply.read_set.push(read_struct);
                //         }
                //         None => {
                //             reply.success = false;
                //             break;
                //         }
                //     }
                // }
                match node.callback.take() {
                    Some(callback) => {
                        callback.send(reply);
                    }
                    None => continue,
                }
                for iter in txn.write_set.iter() {
                    let table = &mut DATA[iter.table_id as usize];
                    match table.get_mut(&iter.key) {
                        Some(lock) => {
                            let mut guard = lock.write().await;
                            guard.data = iter.value().to_string();
                        }
                        None => break,
                    }
                }
            }
        }
    }

    async fn execute_txn(&mut self, txnid: u64) {
        unsafe {
            let (client_id, index) = get_txnid(txnid);

            let node = &TXNS[client_id as usize][index as usize];
            // println!("find scc {},{}, execute{}", client_id, index, node.executed);
            if !node.executed {
                self.find_scc(txnid).await;
            }
        }
    }

    async fn find_scc(&mut self, txnid: u64) -> bool {
        unsafe {
            self.stack.clear();
            self.visit = 0;
            self.index = 0;
            // insert into stack
            self.stack.push(txnid);
            while self.visit >= 0 {
                let tid = self.stack[self.visit as usize];
                let (client_id, index) = get_txnid(tid);
                let node = &mut TXNS[client_id as usize][index as usize];

                // println!(
                //     "find scc {},{}, dfn{}, low{}",
                //     client_id, index, node.dfn, node.low
                // );
                if node.low < 0 {
                    self.index += 1;
                    node.dfn = self.index;
                    node.low = self.index;
                    let deps = node.txn.as_ref().unwrap().deps.clone();
                    for dep in deps {
                        if dep == 0 {
                            continue;
                        }
                        let (dep_clientid, dep_index) = get_txnid(dep);

                        // println!(
                        //     " dep {}, {}, len = {}, node = {:?}",
                        //     dep_clientid,
                        //     dep_index,
                        //     TXNS[dep_clientid as usize].len(),
                        //     TXNS[dep_clientid as usize][dep_index as usize]
                        // );
                        while TXNS[dep_clientid as usize].len() - 1 < dep_index as usize {
                            sleep(Duration::from_millis(10)).await;
                        }
                        let next = &mut TXNS[dep_clientid as usize][dep_index as usize];
                        if !next.committed {
                            // sleep(Duration::from_micros(10)).await;
                            continue;
                        }
                        if next.executed {
                            continue;
                        }
                        // check if next in the stack
                        if next.dfn < 0 {
                            // not in stack
                            next.dfn = 0;
                            self.stack.push(dep);
                            self.visit += 1;
                        } else {
                            if node.low > next.dfn {
                                node.low = next.dfn;
                            }
                        }
                    }
                } else {
                    // get scc . pop & exec
                    if node.dfn == node.low {
                        let mut to_execute: Vec<u64> = Vec::new();
                        loop {
                            let tid = self.stack.pop().unwrap();
                            to_execute.push(tid);
                            self.visit -= 1;
                            if tid == txnid {
                                break;
                            }
                        }
                        // to execute
                        to_execute.sort();
                        // send txn to executor
                        self.apply(to_execute).await;
                    } else {
                        self.visit -= 1;
                    }
                }
            }
        }

        return true;
    }
}
