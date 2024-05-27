use std::{
    collections::HashMap,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    time::Duration,
};

use chrono::Local;
use common::{
    get_currenttime_millis, get_txnid, txn::connect_to_peer, Config, CoordnatorMsg, DbType,
    DtxType, Tuple,
};
use rpc::common::{
    data_service_client::DataServiceClient,
    data_service_server::{DataService, DataServiceServer},
    Echo, Msg, Throughput, TxnOp,
};
use tokio::{
    sync::{
        mpsc::{channel, unbounded_channel, UnboundedSender},
        oneshot, RwLock,
    },
    time::sleep,
};
use tonic::{
    transport::{Channel, Server},
    Request, Response, Status,
};

use crate::{data::init_data, dep_graph::DepGraph, executor::Executor};

pub static mut DATA: Vec<HashMap<u64, RwLock<Tuple>>> = Vec::new();
pub static mut PEER: Vec<DataServiceClient<Channel>> = Vec::new();
pub static mut MAX_COMMIT_TS: u64 = 0;

pub struct RpcServer {
    executor_num: u64,
    addr_to_listen: String,
    sender: Arc<HashMap<u64, UnboundedSender<CoordnatorMsg>>>,
}

impl RpcServer {
    pub fn new(
        executor_num: u64,
        addr_to_listen: String,
        sender: Arc<HashMap<u64, UnboundedSender<CoordnatorMsg>>>,
    ) -> Self {
        Self {
            executor_num,
            sender,
            addr_to_listen,
        }
    }
}

pub async fn run_rpc_server(rpc_server: RpcServer) {
    let addr = rpc_server.addr_to_listen.parse().unwrap();

    println!("rpc server listening on: {:?}", addr);

    let server = DataServiceServer::new(rpc_server);

    Server::builder().add_service(server).serve(addr).await;
}

#[tonic::async_trait]
impl DataService for RpcServer {
    async fn communication(&self, request: Request<Msg>) -> Result<Response<Msg>, Status> {
        let (callback_sender, mut receiver) = oneshot::channel::<Msg>();
        let msg = request.into_inner();
        let (cid, tid) = get_txnid(msg.txn_id);
        let executor_id = cid % self.executor_num;
        let coor_msg = CoordnatorMsg {
            msg,
            call_back: callback_sender,
        };
        self.sender.get(&executor_id).unwrap().send(coor_msg);
        let reply = receiver.await.unwrap();
        Ok(Response::new(reply))
    }
}

pub struct DataServer {
    server_id: u32,
    executor_num: u64,
    executor_senders: HashMap<u64, UnboundedSender<CoordnatorMsg>>,
    config: Config,
    client_num: u64,
    dtx_type: DtxType,
    geo: bool,
    //spanner
    // peer_senders: Vec<DataServiceClient<Channel>>,
}

impl DataServer {
    pub fn new(
        server_id: u32,
        config: Config,
        client_num: u64,
        dtx_type: DtxType,
        geo: bool,
    ) -> Self {
        Self {
            server_id,
            executor_num: config.executor_num,
            executor_senders: HashMap::new(),
            config,
            client_num,
            dtx_type,
            geo,
            // peer_senders: Vec::new(),
        }
    }

    async fn init_rpc(
        &mut self,
        executor_senders: Arc<HashMap<u64, UnboundedSender<CoordnatorMsg>>>,
    ) {
        // start server for client to connect
        let listen_ip = if self.geo {
            self.config.geo_server_addr[self.server_id as usize].clone()
        } else {
            self.config.server_addr[self.server_id as usize].clone()
        };
        println!("server listen ip {}", listen_ip);
        let server = RpcServer::new(self.executor_num, listen_ip, executor_senders);
        if self.dtx_type == DtxType::spanner && self.server_id == 2 {
            //
            let mut data_ip = if self.geo {
                self.config.geo_server_public_addr.clone()
            } else {
                self.config.server_public_addr.clone()
            };
            data_ip.pop();
            unsafe {
                PEER = connect_to_peer(data_ip).await;
                println!("accept{}", PEER.len());
                //

                tokio::spawn(async move {
                    // let mut clients = PEER.clone();
                    loop {
                        sleep(Duration::from_millis(1)).await;
                        let commit_ts = get_currenttime_millis();

                        if commit_ts > MAX_COMMIT_TS {
                            MAX_COMMIT_TS = commit_ts;
                        }
                        // println!("commit ts = {}", MAX_COMMIT_TS);

                        let commit = Msg {
                            txn_id: 0,
                            read_set: Vec::new(),
                            write_set: Vec::new(),
                            op: TxnOp::Accept.into(),
                            success: true,
                            ts: Some(commit_ts),
                            deps: Vec::new(),
                            read_only: false,
                            insert: Vec::new(),
                            delete: Vec::new(),
                        };
                        for iter in PEER.iter() {
                            let commit_clone = commit.clone();
                            tokio::spawn(async move {
                                let _ = iter.clone().communication(commit_clone).await.unwrap();
                            });
                        }
                    }
                });
            }
        }
        run_rpc_server(server).await;
    }

    async fn init_executors(&mut self, dtx_type: DtxType) {
        let (dep_sender, dep_recv) = channel(1000);
        for i in 0..self.executor_num {
            let (sender, receiver) = unbounded_channel::<CoordnatorMsg>();
            self.executor_senders.insert(i, sender);
            let mut exec = Executor::new(
                i,
                receiver,
                dtx_type,
                dep_sender.clone(),
                self.server_id,
                self.geo,
            );
            tokio::spawn(async move {
                exec.run().await;
            });
        }
        if dtx_type == DtxType::janus || dtx_type == DtxType::rjanus {
            let mut dep = DepGraph::new(dep_recv);
            tokio::spawn(async move {
                dep.run().await;
            });
        }
    }

    pub async fn init_and_run(&mut self, db_type: DbType, dtx_type: DtxType) {
        unsafe {
            MAX_COMMIT_TS = get_currenttime_millis();
        }
        init_data(db_type, self.client_num);
        self.init_executors(dtx_type).await;
        self.init_rpc(Arc::new(self.executor_senders.clone())).await;
        // while (true) {
        //     sleep(Duration::from_millis(1)).await;
        // }
    }
}
