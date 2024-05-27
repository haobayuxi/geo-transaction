use std::{
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    time::Duration,
    vec,
};

use rpc::common::{
    throughput_statistics_service_client::ThroughputStatisticsServiceClient,
    throughput_statistics_service_server::{
        ThroughputStatisticsService, ThroughputStatisticsServiceServer,
    },
    Echo, Latencies, Throughput, Ts,
};
use tokio::{
    sync::{mpsc::unbounded_channel, RwLock},
    time::sleep,
};
use tonic::{
    transport::{Channel, Server},
    Request, Response, Status,
};

use crate::{ip_addr_add_prefix, GLOBAL_COMMITTED};

//
static mut LAST_COMMITTED: Vec<u64> = Vec::new();
pub static mut LATENCIES: Vec<u64> = Vec::new();

pub struct coordinator_rpc_server {
    addr_to_listen: String,
}

impl coordinator_rpc_server {
    pub fn new(addr_to_listen: String) -> Self {
        Self { addr_to_listen }
    }
}

pub async fn run_coordinator_server(addr_to_listen: String) {
    let rpc_server = coordinator_rpc_server::new(addr_to_listen);

    let addr = rpc_server.addr_to_listen.parse().unwrap();

    println!("rpc server listening on: {:?}", addr);

    let server = ThroughputStatisticsServiceServer::new(rpc_server);

    Server::builder().add_service(server).serve(addr).await;
}

#[tonic::async_trait]
impl ThroughputStatisticsService for coordinator_rpc_server {
    async fn get(&self, request: Request<Echo>) -> Result<Response<Throughput>, Status> {
        Ok(Response::new(Throughput {
            committed: GLOBAL_COMMITTED.load(Ordering::Relaxed) as u64,
        }))
    }
}

//

pub struct ThroughputStatistics {
    committed: u64,
    clients: Vec<ThroughputStatisticsServiceClient<Channel>>,
}

impl ThroughputStatistics {
    pub async fn new(data_ip: Vec<String>) -> Self {
        let mut clients = Vec::new();
        unsafe {
            let len = data_ip.len() - 1;
            LAST_COMMITTED = vec![0; len];
        }
        for i in 1..data_ip.len() {
            let ip = ip_addr_add_prefix(data_ip.get(i).unwrap().clone());
            println!("connecting {}", ip);
            loop {
                match ThroughputStatisticsServiceClient::connect(ip.clone()).await {
                    Ok(data_client) => {
                        clients.push(data_client);
                        break;
                    }
                    Err(_) => sleep(Duration::from_millis(10)).await,
                }
            }
            // println!("connectted");
        }
        Self {
            committed: 0,
            clients,
        }
    }

    pub async fn run(&mut self) {
        let echo = Echo::default();
        loop {
            // sleep
            sleep(Duration::from_secs(1)).await;
            let (sender, mut recv) = unbounded_channel::<Throughput>();

            for i in 0..self.clients.len() {
                let e = echo.clone();
                let mut client = self.clients[i].clone();
                let s_ = sender.clone();
                tokio::spawn(async move {
                    match client.get(e).await {
                        Ok(reply) => {
                            // update last committed
                            let throughput = reply.into_inner();
                            unsafe {
                                LAST_COMMITTED[i] = throughput.committed;
                            }
                            s_.send(throughput);
                        }
                        Err(_) => unsafe {
                            let mut throughput = Throughput::default();
                            throughput.committed = LAST_COMMITTED[i];
                            s_.send(throughput);
                        },
                    }
                });
            }

            let mut committed = 0;
            for i in 0..self.clients.len() {
                let reply = recv.recv().await.unwrap();
                committed += reply.committed;
                println!("reply committed = {}", reply.committed);
            }
            committed += GLOBAL_COMMITTED.load(Ordering::Relaxed) as u64;
            let throughput_per_second = committed - self.committed;
            self.committed = committed;
            println!("committed = {}", throughput_per_second);
        }
    }
}
