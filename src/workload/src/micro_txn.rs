use common::txn::DtxCoordinator;
use rpc::common::ReadStruct;
use tokio::time::Instant;

use crate::micro_db::MicroQuery;

pub async fn micro_run_transactions(
    coordinator: &mut DtxCoordinator,
    // theta: f64,
    read_only: bool,
    txn_nums: u64,
) -> (Vec<u128>, f64) {
    // init workload
    let mut query = MicroQuery::new(read_only);
    // run transaction
    let mut latency_result = Vec::new();
    let total_start = Instant::now();
    for _ in 0..txn_nums {
        let start = Instant::now();
        let (read_set, write_set) = query.generate();
        let success = run_transaction(coordinator, read_set, write_set).await;
        let end_time = start.elapsed().as_micros();

        if success {
            latency_result.push(end_time);
        }
    }
    let total_end = (total_start.elapsed().as_millis() as f64) / 1000.0;
    let start_index = latency_result.len() * 5 / 100;
    latency_result.drain(0..start_index);
    let throughput_result = latency_result.len() as f64 / total_end;
    // println!("throughput = {}", throughput_result);
    (latency_result, throughput_result)
}

async fn run_transaction(
    coordinator: &mut DtxCoordinator,
    read_set: Vec<ReadStruct>,
    write_set: Vec<ReadStruct>,
) -> bool {
    let read_only = write_set.is_empty();
    coordinator.tx_begin(read_only).await;
    for iter in read_set {
        coordinator.add_read_to_execute(iter.key, iter.table_id);
    }
    coordinator.write_set = Vec::new();
    for iter in write_set {
        coordinator.add_write_to_execute(iter.key, iter.table_id, iter.value.unwrap());
    }
    // write_set
    //     .into_iter()
    //     .map(|f| Arc::new(RwLock::new(f)))
    //     .collect();
    let (status, result) = coordinator.tx_exe().await;
    // println!("read status = {}, result = {:?}", status, result);
    if !status {
        coordinator.tx_abort().await;
        return false;
    }
    return coordinator.tx_commit().await;
}
