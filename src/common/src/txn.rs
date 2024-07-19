use chrono::Local;
use rpc::common::Ts;
use rpc::common::{
    cto_service_client::CtoServiceClient, data_service_client::DataServiceClient, Echo, Msg,
    ReadStruct, TxnOp,
};
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::thread::sleep as STDSleep;
use tokio::sync::mpsc::unbounded_channel;
use tokio::sync::oneshot;
use tokio::sync::RwLock;
use tokio::time::sleep;
use tokio::time::Duration;
use tonic::transport::Channel;

use crate::{
    get_currenttime_micros, get_currenttime_millis, GLOBAL_COMMITTED, LOCAL_UNCERTAINTY,
    UNCERTAINTY,
};
use crate::{get_txnid, DtxType};
use crate::{ip_addr_add_prefix, CID_LEN};

pub static LEADER_ID: usize = 2;

pub async fn connect_to_peer(data_ip: Vec<String>) -> Vec<DataServiceClient<Channel>> {
    let mut data_clients = Vec::new();
    for iter in data_ip {
        let server_ip = ip_addr_add_prefix(iter);
        loop {
            match DataServiceClient::connect(server_ip.clone()).await {
                Ok(data_client) => {
                    data_clients.push(data_client);
                    break;
                }
                Err(e) => {
                    // println!("connect error {}-- {:?}", server_ip, e);
                    sleep(Duration::from_millis(10)).await;
                }
            }
        }
    }
    return data_clients;
}

pub fn gen_future_timestamp() -> u64 {
    get_currenttime_millis() + 0
}
pub struct DtxCoordinator {
    pub id: u64,
    pub local_ts: Arc<RwLock<u64>>,
    pub dtx_type: DtxType,
    preferred_server: u64,
    geo: bool,
    pub txn_id: u64,
    read_only: bool,
    commit_ts: u64,
    // <shard>
    pub read_set: Vec<ReadStruct>,
    pub write_set: Vec<Arc<RwLock<ReadStruct>>>,
    read_to_execute: Vec<ReadStruct>,
    write_to_execute: Vec<Arc<RwLock<ReadStruct>>>,
    insert: Vec<ReadStruct>,
    delete: Vec<ReadStruct>,
    write_tuple_ts: Vec<u64>,
    // janus
    fast_commit: bool,
    deps: Vec<u64>,

    data_clients: Vec<DataServiceClient<Channel>>,
}

impl DtxCoordinator {
    pub async fn new(
        id: u64,
        local_ts: Arc<RwLock<u64>>,
        dtx_type: DtxType,
        data_ip: Vec<String>,
        geo: bool,
        preferred_server: u64,
    ) -> Self {
        // init  data client
        let data_clients = connect_to_peer(data_ip).await;
        Self {
            id,
            local_ts,
            dtx_type,
            txn_id: id << CID_LEN,
            // start_ts: 0,
            commit_ts: 0,
            read_set: Vec::new(),
            write_set: Vec::new(),
            // cto_client,
            data_clients,
            read_to_execute: Vec::new(),
            write_to_execute: Vec::new(),
            write_tuple_ts: Vec::new(),
            read_only: false,
            fast_commit: true,
            deps: Vec::new(),
            insert: Vec::new(),
            delete: Vec::new(),
            preferred_server,
            geo,
            // committed,
        }
    }

    pub async fn tx_begin(&mut self, read_only: bool) {
        // init coordinator
        self.txn_id += 1;
        self.read_set.clear();
        self.write_set.clear();
        self.read_to_execute.clear();
        self.write_to_execute.clear();
        self.write_tuple_ts.clear();
        self.insert.clear();
        self.delete.clear();
        self.read_only = read_only;
        if read_only {
            self.commit_ts = get_currenttime_millis() + UNCERTAINTY;
        } else {
            self.commit_ts = gen_future_timestamp();
        }
        self.fast_commit = true;
        self.deps.clear();
    }

    pub async fn tx_exe(&mut self) -> (bool, Vec<ReadStruct>) {
        if self.read_to_execute.is_empty() && self.write_to_execute.is_empty() {
            return (true, Vec::new());
        }
        let mut write_set = Vec::new();
        for iter in self.write_to_execute.iter() {
            write_set.push(iter.read().await.clone());
        }
        let mut success = true;
        let mut result = Vec::new();
        // preferred
        let preferred_server_id = if self.geo {
            self.preferred_server
        } else {
            self.id % 3
        };

        if self.dtx_type == DtxType::spanner {
            if self.read_only {
                let read = Msg {
                    txn_id: self.txn_id,
                    read_set: self.read_to_execute.clone(),
                    write_set: Vec::new(),
                    op: TxnOp::Execute.into(),
                    success: true,
                    ts: Some(self.commit_ts),
                    deps: Vec::new(),
                    read_only: true,
                    insert: self.insert.clone(),
                    delete: self.delete.clone(),
                };
                // println!("client ts = {}", self.commit_ts);
                let client = self
                    .data_clients
                    .get_mut(preferred_server_id as usize)
                    .unwrap();

                let reply: Msg = client.communication(read).await.unwrap().into_inner();
                success = reply.success;
                result = reply.read_set;
                // println!("success {} {:?}", success, result);
            } else {
                if !self.write_to_execute.is_empty() {
                    let execute = Msg {
                        txn_id: self.txn_id,
                        read_set: self.read_to_execute.clone(),
                        write_set,
                        op: TxnOp::Execute.into(),
                        success: true,
                        ts: Some(self.commit_ts),
                        deps: Vec::new(),
                        read_only: false,
                        insert: self.insert.clone(),
                        delete: self.delete.clone(),
                    };
                    if self.dtx_type == DtxType::spanner {
                        let reply = self.data_clients[LEADER_ID]
                            .communication(execute)
                            .await
                            .unwrap()
                            .into_inner();
                        success = reply.success;
                        result = reply.read_set.clone();
                    } else {
                        let replies = self.sync_broadcast(execute).await;
                        self.deps = replies[0].deps.clone();
                        for i in 0..=2 {
                            if !replies[i].success {
                                success = false;
                            }
                            result = replies[i].read_set.clone();
                        }
                    }
                } else {
                    // simple read
                    let read = Msg {
                        txn_id: self.txn_id,
                        read_set: self.read_to_execute.clone(),
                        write_set: Vec::new(),
                        success: true,
                        op: TxnOp::Execute.into(),
                        ts: Some(self.commit_ts),
                        deps: Vec::new(),
                        read_only: false,
                        insert: self.insert.clone(),
                        delete: self.delete.clone(),
                    };
                    let client = if self.dtx_type == DtxType::spanner {
                        // read lock at leader
                        self.data_clients.get_mut(LEADER_ID).unwrap()
                    } else {
                        self.data_clients
                            .get_mut(preferred_server_id as usize)
                            .unwrap()
                    };

                    let reply: Msg = client.communication(read).await.unwrap().into_inner();
                    success = reply.success;
                    result = reply.read_set;
                }
            }
        } else if self.dtx_type == DtxType::janus {
            let execute = Msg {
                txn_id: self.txn_id,
                read_set: self.read_to_execute.clone(),
                write_set,
                op: TxnOp::Execute.into(),
                success: true,
                ts: Some(self.commit_ts),
                deps: Vec::new(),
                read_only: false,
                insert: Vec::new(),
                delete: Vec::new(),
            };
            let replies = self.sync_broadcast(execute).await;
            self.deps = replies[0].deps.clone();
            for i in 0..=2 {
                if !replies[i].success {
                    success = false;
                }
                if replies[i].deps != self.deps {
                    self.fast_commit = false;
                    for iter in replies[i].deps.iter() {
                        if !self.deps.contains(iter) {
                            self.deps.push(*iter);
                        }
                    }
                }
            }
            result = replies[0].read_set.clone();
        } else if self.dtx_type == DtxType::ocean_vista || self.dtx_type == DtxType::mercury {
            //
            let execute = Msg {
                txn_id: self.txn_id,
                read_set: self.read_to_execute.clone(),
                write_set,
                op: TxnOp::Execute.into(),
                success: true,
                ts: Some(self.commit_ts),
                deps: Vec::new(),
                read_only: false,
                insert: Vec::new(),
                delete: Vec::new(),
            };
            let client = self
                .data_clients
                .get_mut(preferred_server_id as usize)
                .unwrap();

            let reply: Msg = client.communication(execute).await.unwrap().into_inner();
        } else if self.dtx_type == DtxType::cockroachdb {
            let request = Msg {
                txn_id: self.txn_id,
                read_set: self.read_to_execute.clone(),
                write_set,
                op: TxnOp::Execute.into(),
                success: true,
                ts: Some(self.commit_ts),
                deps: Vec::new(),
                read_only: true,
                insert: self.insert.clone(),
                delete: self.delete.clone(),
            };
            // println!("client ts = {}", self.commit_ts);
            let client = self
                .data_clients
                .get_mut(preferred_server_id as usize)
                .unwrap();

            let reply: Msg = client.communication(request).await.unwrap().into_inner();
            success = reply.success;
            result = reply.read_set.clone();
            self.commit_ts = reply.ts();
        }

        self.read_set.extend(result.clone());
        self.write_set.extend(self.write_to_execute.clone());
        self.read_to_execute.clear();
        self.write_to_execute.clear();
        return (success, result);
    }

    pub async fn tx_commit(&mut self) -> bool {
        if self.read_only && (self.dtx_type == DtxType::spanner) {
            GLOBAL_COMMITTED.fetch_add(1, Ordering::Relaxed);
            return true;
        }

        self.commit_ts = get_currenttime_millis();
        let mut write_set = Vec::new();
        for iter in self.write_set.iter() {
            write_set.push(iter.read().await.clone());
        }
        if self.dtx_type == DtxType::spanner {
            GLOBAL_COMMITTED.fetch_add(1, Ordering::Relaxed);
            let commit = Msg {
                txn_id: self.txn_id,
                read_set: self.read_set.clone(),
                write_set: write_set.clone(),
                op: TxnOp::Commit.into(),
                success: true,
                ts: Some(self.commit_ts),
                deps: self.deps.clone(),
                read_only: false,
                insert: self.insert.clone(),
                delete: self.delete.clone(),
            };

            self.async_broadcast_commit(commit).await;
            return true;
        }
        // validate
        if self.validate().await {
            let commit = Msg {
                txn_id: self.txn_id,
                read_set: self.read_set.clone(),
                write_set: write_set.clone(),
                op: TxnOp::Commit.into(),
                success: true,
                ts: Some(self.commit_ts),
                deps: self.deps.clone(),
                read_only: false,
                insert: self.insert.clone(),
                delete: self.delete.clone(),
            };
            if self.dtx_type == DtxType::janus {
                if !self.fast_commit {
                    let accept = Msg {
                        txn_id: self.txn_id,
                        read_set: Vec::new(),
                        write_set: Vec::new(),
                        op: TxnOp::Accept.into(),
                        success: true,
                        ts: Some(self.commit_ts),
                        deps: self.deps.clone(),
                        read_only: false,
                        insert: Vec::new(),
                        delete: Vec::new(),
                    };
                    self.sync_broadcast(accept).await;
                }
                // println!("commit");
                self.async_broadcast_commit(commit).await;
                GLOBAL_COMMITTED.fetch_add(1, Ordering::Relaxed);
                return true;
            }
            // broadcast
            self.async_broadcast_commit(commit).await;

            GLOBAL_COMMITTED.fetch_add(1, Ordering::Relaxed);
            return true;
        } else {
            self.tx_abort().await;
            return false;
        }
    }

    pub async fn tx_abort(&mut self) {
        if self.write_set.is_empty() && self.read_set.is_empty() {
            return;
        }
        let mut write_set = Vec::new();
        for iter in self.write_set.iter() {
            write_set.push(iter.read().await.clone());
        }
        let abort = Msg {
            txn_id: self.txn_id,
            read_set: self.read_set.clone(),
            write_set,
            op: TxnOp::Abort.into(),
            success: true,
            ts: Some(self.commit_ts),
            deps: Vec::new(),
            read_only: false,
            insert: Vec::new(),
            delete: Vec::new(),
        };
        self.async_broadcast_commit(abort).await;
    }

    pub fn add_read_to_execute(&mut self, key: u64, table_id: u32) {
        let read_struct = ReadStruct {
            key,
            table_id,
            value: None,
            timestamp: None,
        };
        self.read_to_execute.push(read_struct);
    }

    pub fn add_to_insert(&mut self, insert: ReadStruct) {
        self.insert.push(insert);
    }

    pub fn add_to_delete(&mut self, delete: ReadStruct) {
        self.delete.push(delete);
    }

    pub fn add_write_to_execute(
        &mut self,
        key: u64,
        table_id: u32,
        value: String,
    ) -> Arc<RwLock<ReadStruct>> {
        let write_struct = ReadStruct {
            key,
            table_id,
            value: Some(value),
            timestamp: None,
        };
        let obj = Arc::new(RwLock::new(write_struct));
        self.write_to_execute.push(obj.clone());
        obj
    }

    async fn validate(&mut self) -> bool {
        return true;
    }

    async fn sync_broadcast(&mut self, msg: Msg) -> Vec<Msg> {
        let mut result = Vec::new();
        let (sender, mut recv) = unbounded_channel::<Msg>();
        for i in 0..self.data_clients.len() {
            let mut client = self.data_clients[i].clone();
            let s_ = sender.clone();
            let msg_ = msg.clone();
            tokio::spawn(async move {
                s_.send(client.communication(msg_).await.unwrap().into_inner());
            });
        }
        for _ in 0..self.data_clients.len() {
            result.push(recv.recv().await.unwrap());
        }
        return result;
    }

    async fn async_broadcast_commit(&mut self, commit: Msg) {
        for i in 0..self.data_clients.len() {
            let mut client = self.data_clients[i].clone();
            let msg_ = commit.clone();
            tokio::spawn(async move {
                client.communication(msg_).await.unwrap().into_inner();
            });
        }
    }
}
