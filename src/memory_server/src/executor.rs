use common::{get_currenttime_micros, get_currenttime_millis, get_txnid, CoordnatorMsg, DtxType};
use rpc::common::{data_service_client::DataServiceClient, Msg, TxnOp};
use std::{cmp::min, time::Duration};
use tokio::sync::mpsc::{unbounded_channel, Sender, UnboundedReceiver};
use tokio::{sync::oneshot::Sender as OneShotSender, time::sleep};
use tonic::transport::Channel;

use crate::data::get_dep_ts;
use crate::data_server::{SAFE, WAITING};
use crate::{
    data::{
        delete, get_deps, get_read_only, get_read_set, insert, lock_write_set, release_read_set,
        releass_locks, update_and_release_locks, validate,
    },
    data_server::{MAX_COMMIT_TS, PEER},
    dep_graph::{Node, TXNS},
};

pub struct Executor {
    pub id: u64,
    pub recv: UnboundedReceiver<CoordnatorMsg>,
    server_id: u32,
    dtx_type: DtxType,
    geo: bool,
    // janus
    send_commit_to_dep_graph: Sender<u64>,
    // spanner
    // peer_senders: Vec<DataServiceClient<Channel>>,
}

impl Executor {
    pub fn new(
        id: u64,
        recv: UnboundedReceiver<CoordnatorMsg>,
        dtx_type: DtxType,
        sender: Sender<u64>,
        server_id: u32,
        geo: bool,
        // peer_senders: Vec<DataServiceClient<Channel>>,
    ) -> Self {
        Self {
            id,
            recv,
            dtx_type,
            send_commit_to_dep_graph: sender,
            server_id,
            geo,
            // peer_senders,
        }
    }

    async fn accept(&mut self, msg: Msg, call_back: OneShotSender<Msg>) {
        unsafe {
            // let data_clients = PEER.clone();
            tokio::spawn(async move {
                let mut accept = msg.clone();
                accept.op = TxnOp::Accept.into();
                // broadcast lock
                // let start = Instant::now();
                let result = sync_broadcast(accept.clone()).await;

                // let end_time = start.elapsed().as_millis();
                // println!("{}accept{}", accept.txn_id, end_time);
                call_back.send(accept.clone());
            });
        }
    }

    pub async fn run(&mut self) {
        unsafe {
            loop {
                match self.recv.recv().await {
                    Some(coor_msg) => match coor_msg.msg.op() {
                        rpc::common::TxnOp::Execute => {
                            let mut reply = coor_msg.msg.clone();
                            let ts = coor_msg.msg.ts();
                            if coor_msg.msg.read_only {
                                let read_set = coor_msg.msg.read_set.clone();
                                // need wait
                                let local_clock = if self.geo {
                                    get_currenttime_millis()
                                } else {
                                    get_currenttime_micros()
                                };
                                match self.dtx_type {
                                    DtxType::spanner => {
                                        if ts > MAX_COMMIT_TS {
                                            // wait
                                            tokio::spawn(async move {
                                                while ts > MAX_COMMIT_TS {
                                                    let wait_time = ts - MAX_COMMIT_TS;
                                                    // println!("ts{}, cts{}", ts, MAX_COMMIT_TS);
                                                    sleep(Duration::from_millis(wait_time)).await;
                                                }
                                                let (success, read_result) =
                                                    get_read_only(read_set).await;
                                                reply.success = success;
                                                reply.read_set = read_result;

                                                coor_msg.call_back.send(reply);
                                            });
                                        } else {
                                            let (success, read_result) =
                                                get_read_only(read_set).await;
                                            reply.success = success;
                                            reply.read_set = read_result;

                                            coor_msg.call_back.send(reply);
                                        }
                                    }
                                    DtxType::janus => {
                                        let txn_id = coor_msg.msg.txn_id;
                                        let (client_id, index) = get_txnid(txn_id);
                                        let mut last_index = TXNS[client_id as usize].len();
                                        while index >= last_index as u64 {
                                            let node = Node::default();
                                            TXNS[client_id as usize].push(node);
                                            last_index += 1;
                                        }
                                        let node = Node::new(coor_msg.msg.clone());
                                        if TXNS[client_id as usize].len() as u64 == index + 1 {
                                            //
                                            TXNS[client_id as usize][index as usize] = node;
                                        } else {
                                            TXNS[client_id as usize].push(node);
                                        }
                                        let (success, deps, read_results) =
                                            get_deps(coor_msg.msg).await;
                                        reply.success = success;
                                        reply.deps = deps.clone();
                                        reply.read_set = read_results;
                                        coor_msg.call_back.send(reply);
                                    }
                                    DtxType::ocean_vista => {
                                        // wait for the commit ts > read only ts
                                        let mut new_msg = coor_msg.msg.clone();
                                        new_msg.ts = Some(get_currenttime_millis());
                                        if SAFE > new_msg.ts() {
                                            // execute
                                            let (success, read_result) =
                                                get_read_only(coor_msg.msg.read_set.clone()).await;
                                            reply.success = success;
                                            coor_msg.call_back.send(reply);
                                        } else {
                                            // insert into waiting list
                                            WAITING.insert(new_msg.ts(), coor_msg);
                                        }
                                    }
                                    DtxType::mercury => {
                                        let mut new_msg = coor_msg.msg.clone();
                                        new_msg.ts = Some(get_currenttime_millis());
                                        if SAFE > new_msg.ts() {
                                            // execute
                                            let (success, read_result) =
                                                get_read_only(coor_msg.msg.read_set.clone()).await;
                                            reply.success = success;
                                            coor_msg.call_back.send(reply);
                                        } else {
                                            // insert into waiting list
                                            WAITING.insert(new_msg.ts(), coor_msg);
                                        }
                                    }
                                    DtxType::cockroachdb => {}
                                }
                            } else {
                                match self.dtx_type {
                                    DtxType::spanner => {
                                        // lock the read set
                                        let (success, read_result) = get_read_set(
                                            coor_msg.msg.read_set.clone(),
                                            coor_msg.msg.txn_id,
                                            self.dtx_type,
                                        )
                                        .await;
                                        // lock the write set

                                        reply.success = success;
                                        if !success {
                                            // send back failure
                                            reply.success = false;
                                            coor_msg.call_back.send(reply);
                                            continue;
                                        }
                                        reply.read_set = read_result;
                                        if !coor_msg.msg.write_set.is_empty() {
                                            let (success, mut read_write_set) = lock_write_set(
                                                coor_msg.msg.write_set.clone(),
                                                coor_msg.msg.txn_id,
                                            )
                                            .await;
                                            reply.read_set.append(&mut read_write_set);
                                            reply.write_set = coor_msg.msg.write_set.clone();
                                            reply.success = success;
                                            self.accept(reply, coor_msg.call_back).await;
                                        } else {
                                            coor_msg.call_back.send(reply);
                                        }
                                    }
                                    DtxType::janus => {
                                        // init node
                                        let txn_id = coor_msg.msg.txn_id;
                                        let (client_id, index) = get_txnid(txn_id);
                                        let mut last_index = TXNS[client_id as usize].len();
                                        while index >= last_index as u64 {
                                            let node = Node::default();
                                            TXNS[client_id as usize].push(node);
                                            last_index += 1;
                                        }
                                        let node = Node::new(coor_msg.msg.clone());
                                        if TXNS[client_id as usize].len() as u64 == index + 1 {
                                            //
                                            TXNS[client_id as usize][index as usize] = node;
                                        } else {
                                            TXNS[client_id as usize].push(node);
                                        }
                                        let (success, deps, read_results) =
                                            get_deps(coor_msg.msg).await;
                                        reply.success = success;
                                        reply.deps = deps.clone();
                                        reply.read_set = read_results;
                                        coor_msg.call_back.send(reply);
                                    }
                                    DtxType::ocean_vista => {
                                        // broadcast to all to accept
                                        let mut new_msg = coor_msg.msg.clone();
                                        new_msg.ts = Some(get_currenttime_millis());
                                        self.accept(new_msg, coor_msg.call_back).await;
                                    }
                                    DtxType::mercury => {
                                        let mut new_msg = coor_msg.msg.clone();
                                        new_msg.ts = Some(get_currenttime_millis());
                                        self.accept(new_msg, coor_msg.call_back).await;
                                    }
                                    DtxType::cockroachdb => {
                                        let mut new_msg = coor_msg.msg.clone();
                                        // check conflict ts
                                        let ts = get_dep_ts(new_msg.clone()).await;
                                        reply.success = true;
                                        reply.ts = Some(ts);
                                        coor_msg.call_back.send(reply);
                                    }
                                }
                            }
                        }
                        rpc::common::TxnOp::Commit => {
                            // update and release the lock
                            let commit_ts = coor_msg.msg.ts();
                            unsafe {
                                if MAX_COMMIT_TS < commit_ts {
                                    MAX_COMMIT_TS = commit_ts;
                                }
                            }
                            let mut reply = Msg::default();
                            if self.dtx_type == DtxType::janus {
                                // insert callback to node
                                let txn_id = coor_msg.msg.txn_id;
                                let (client_id, index) = get_txnid(txn_id);

                                let node: &mut Node = &mut TXNS[client_id as usize][index as usize];
                                node.txn = Some(coor_msg.msg);
                                node.committed = true;
                                coor_msg.call_back.send(reply);
                                // node.callback = Some(coor_msg.call_back);
                                // send commit txn to dep_graph
                                // self.send_commit_to_dep_graph.send(txn_id).await;

                                // println!("commit cid={},index={}", client_id, index);
                            } else {
                                if self.dtx_type == DtxType::spanner {
                                    release_read_set(
                                        coor_msg.msg.read_set.clone(),
                                        coor_msg.msg.txn_id,
                                    )
                                    .await;
                                }
                                if self.dtx_type == DtxType::spanner {
                                    let commit: Msg = coor_msg.msg.clone();
                                    unsafe {
                                        tokio::spawn(async move {
                                            sync_broadcast(commit);
                                        });
                                    }
                                }
                                update_and_release_locks(coor_msg.msg.clone(), self.dtx_type).await;
                                // insert & delete
                                insert(coor_msg.msg.insert.clone());
                                delete(coor_msg.msg.delete);
                                reply.success = true;
                                coor_msg.call_back.send(reply);
                            }
                        }
                        rpc::common::TxnOp::Abort => {
                            // release the lock
                            if self.dtx_type == DtxType::spanner {
                                release_read_set(
                                    coor_msg.msg.read_set.clone(),
                                    coor_msg.msg.txn_id,
                                )
                                .await;
                            }
                            if self.dtx_type == DtxType::janus {
                                // mark as executed
                                let txn_id = coor_msg.msg.txn_id;
                                let (client_id, index) = get_txnid(txn_id);
                                let mut last_index = TXNS[client_id as usize].len() - 1;
                                while index >= last_index as u64 {
                                    let node = Node::default();
                                    TXNS[client_id as usize].push(node);
                                    last_index += 1;
                                }
                                let node = &mut TXNS[client_id as usize][index as usize];
                                node.executed = true;
                                node.committed = true;
                                // println!("abort cid={},index={}", client_id, index);
                            } else {
                                releass_locks(coor_msg.msg, self.dtx_type).await;
                            }
                            let mut reply = Msg::default();
                            reply.success = true;
                            coor_msg.call_back.send(reply);
                        }
                        rpc::common::TxnOp::Validate => {
                            // return read set ts
                            let success = validate(coor_msg.msg, self.dtx_type).await;
                            let mut reply = Msg::default();
                            // println!("validate  = {}", success);
                            reply.success = success;
                            coor_msg.call_back.send(reply);
                        }
                        rpc::common::TxnOp::Accept => {
                            //
                            unsafe {
                                if self.dtx_type == DtxType::ocean_vista {
                                    // insert into waiting list
                                    WAITING.insert(coor_msg.msg.ts(), coor_msg);
                                } else if self.dtx_type == DtxType::mercury {
                                    // insert into waiting list
                                    WAITING.insert(coor_msg.msg.ts(), coor_msg);
                                } else {
                                    let commit_ts = coor_msg.msg.ts();
                                    if MAX_COMMIT_TS < commit_ts {
                                        MAX_COMMIT_TS = commit_ts;
                                    }

                                    let mut reply = Msg::default();
                                    reply.success = true;
                                    if self.dtx_type == DtxType::spanner {
                                        // lock the write set
                                        lock_write_set(coor_msg.msg.write_set, coor_msg.msg.txn_id)
                                            .await;
                                    }
                                    coor_msg.call_back.send(reply);
                                }
                            }
                        }
                    },
                    None => {}
                }
            }
        }
    }
}

async fn sync_broadcast(msg: Msg) -> Vec<Msg> {
    let mut result = Vec::new();
    let data_clients = unsafe { PEER.clone() };
    let (sender, mut recv) = unbounded_channel::<Msg>();
    for iter in data_clients.iter() {
        let mut client = iter.clone();
        let s_ = sender.clone();
        let msg_ = msg.clone();
        tokio::spawn(async move {
            s_.send(client.communication(msg_).await.unwrap().into_inner());
        });
    }
    // println!("data client len {}", data_clients.len());
    for _ in 0..data_clients.len() {
        // println!("{:?}", recv.recv().await.unwrap());
        result.push(recv.recv().await.unwrap());
    }
    return result;
}

async fn async_broadcast_commit(commit: Msg, data_clients: Vec<DataServiceClient<Channel>>) {
    for iter in data_clients.iter() {
        let mut client = iter.clone();
        let msg_ = commit.clone();
        tokio::spawn(async move {
            client.communication(msg_).await.unwrap().into_inner();
        });
    }
}
