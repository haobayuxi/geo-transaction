use common::throughput_statistics::{
    coordinator_rpc_server, run_coordinator_server, ThroughputStatistics, LATENCIES,
};
use common::{ip_addr_add_prefix, txn::DtxCoordinator, Config, ConfigInFile};
use common::{DbType, GLOBAL_COMMITTED};
use std::env;
use std::sync::atomic::AtomicU64;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc::channel;
use tokio::sync::RwLock;
use tokio::time::sleep;
use workload::micro_txn::micro_run_transactions;
use workload::small_bank_txn::small_bank_run_transactions;
use workload::tatp_txn::tatp_run_transactions;
use workload::tpcc_txn::tpcc_run_transactions;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<String> = env::args().collect();
    let id = args[1].parse::<u32>().unwrap();
    let f = std::fs::File::open("config.yml").unwrap();
    let server_config: ConfigInFile = serde_yaml::from_reader(f).unwrap();
    let dtx_type = serde_yaml::from_str(&server_config.dtx_type).unwrap();
    let db_type: DbType = serde_yaml::from_str(&server_config.db_type).unwrap();
    let local_ts = Arc::new(RwLock::new(0));
    let geo = server_config.geo;
    let config = Config::default();
    // init throughput statistics rpc server
    // let coordinator_server = coordinator_rpc_server::new(
    //     config.client_addr.get(id as usize).unwrap().clone(),
    //     local_ts.clone(),
    // );
    let addr_to_listen = if geo {
        config.geo_client_addr.get(id as usize).unwrap().clone()
    } else {
        config.client_addr.get(id as usize).unwrap().clone()
    };
    tokio::spawn(async move {
        run_coordinator_server(addr_to_listen).await;
    });
    if id == 0 {
        // init client
        println!("init throughput client");
        let data_ips = if geo {
            config.geo_client_public_addr.clone()
        } else {
            config.client_public_addr.clone()
        };
        let mut get_throughput_client = ThroughputStatistics::new(data_ips).await;
        tokio::spawn(async move {
            // loop {
            get_throughput_client.run().await;
            // }
        });
    }

    let (result_sender, mut recv) = channel::<(Vec<u128>, f64)>(100);
    let client_num = server_config.client_num;

    let read_only = server_config.read_only;
    let txns_per_client = server_config.txns_per_client;

    for i in 0..client_num {
        let loca_ts_bk = local_ts.clone();
        let server_addr = if geo {
            config.geo_server_public_addr.clone()
        } else {
            config.server_public_addr.clone()
        };
        let sender = result_sender.clone();
        tokio::spawn(async move {
            let mut dtx_coordinator = DtxCoordinator::new(
                i + (id as u64 * client_num),
                loca_ts_bk,
                dtx_type,
                server_addr,
                true,
                (id % 3) as u64,
            )
            .await;

            match db_type {
                DbType::micro => {
                    sender
                        .send(
                            micro_run_transactions(
                                &mut dtx_coordinator,
                                read_only,
                                txns_per_client,
                            )
                            .await,
                        )
                        .await;
                }
                DbType::tatp => {
                    sender
                        .send(tatp_run_transactions(&mut dtx_coordinator, txns_per_client).await)
                        .await;
                }
                DbType::smallbank => {
                    sender
                        .send(
                            small_bank_run_transactions(&mut dtx_coordinator, txns_per_client)
                                .await,
                        )
                        .await;
                }
                DbType::tpcc => {
                    sender
                        .send(tpcc_run_transactions(&mut dtx_coordinator, txns_per_client).await)
                        .await;
                }
            }
        });
    }
    let mut total_latency: Vec<u128> = Vec::new();
    let mut total_throuthput = 0.0;
    for _ in 0..client_num {
        let (latency_result, throughput) = recv.recv().await.unwrap();
        total_latency.extend(latency_result.iter());
        total_throuthput += throughput;
    }
    total_latency.sort();
    let success_num = total_latency.len();
    //
    println!(
        "mean latency = {}",
        (total_latency[success_num / 2 as usize] as f64) / 1000.0,
        // success_num
    );
    println!(
        ".90 latency = {}",
        (total_latency[success_num * 90 / 100 as usize] as f64) / 1000.0
    );
    // println!("")
    // println!("throughtput = {}", total_throuthput);
    sleep(Duration::from_secs(2)).await;
    Ok(())
}
