use common::{
    f64_rand, txn::DtxCoordinator, u64_rand, ACCESS_INFO_TABLE, CALL_FORWARDING_TABLE,
    SPECIAL_FACILITY_TABLE, SUBSCRIBER_TABLE,
};
use tokio::time::Instant;

use crate::tatp_db::{
    ai_key, cf_key, get_sid, rnd, sf_key, CallForwarding, SpecialFacility, Subscriber,
};

async fn run_tatp_transaction(coordinator: &mut DtxCoordinator) -> bool {
    let op = u64_rand(0, 99);

    // return tx_get_subscriber_data(coordinator).await;
    if op < 35 {
        //
        return tx_get_subscriber_data(coordinator).await;
    } else if op < 45 {
        //
        return tx_get_new_destination(coordinator).await;
    } else if op < 80 {
        //
        return tx_get_access_data(coordinator).await;
    } else if op < 95 {
        // return tx_get_subscriber_data(coordinator).await;
        return tx_update_lcoation(coordinator).await;
    } else {
        // return tx_get_subscriber_data(coordinator).await;
        return tx_update_subscriber_data(coordinator).await;
    }
}

pub async fn tatp_run_transactions(
    coordinator: &mut DtxCoordinator,
    txn_nums: u64,
) -> (Vec<u128>, f64) {
    let mut latency_result = Vec::new();
    let total_start = Instant::now();
    for i in 0..txn_nums {
        let start = Instant::now();
        let success = run_tatp_transaction(coordinator).await;
        let end_time = start.elapsed().as_micros();
        if success {
            latency_result.push(end_time);
        }
    }
    let total_end = (total_start.elapsed().as_millis() as f64) / 1000.0;
    let throughput_result = latency_result.len() as f64 / total_end;
    // println!("throughput = {}", throughput_result);
    (latency_result, throughput_result)
}

async fn tx_get_subscriber_data(coordinator: &mut DtxCoordinator) -> bool {
    coordinator.tx_begin(true).await;

    // build key
    let s_id = get_sid();
    coordinator.add_read_to_execute(s_id, SUBSCRIBER_TABLE);

    let (status, result) = coordinator.tx_exe().await;
    if !status {
        coordinator.tx_abort().await;
        return false;
    }
    return coordinator.tx_commit().await;
}

async fn tx_get_new_destination(coordinator: &mut DtxCoordinator) -> bool {
    coordinator.tx_begin(true).await;

    // build key
    let s_id = get_sid();
    let sf_type = u64_rand(1, 4);
    let start_time = u64_rand(0, 2) * 8;
    let end_time = u64_rand(1, 24);
    let cf_to_fetch = start_time / 8 + 1;

    coordinator.add_read_to_execute(sf_key(s_id, sf_type), SPECIAL_FACILITY_TABLE);

    // fetch call forwarding records
    for i in 0..cf_to_fetch {
        let cf_key = cf_key(s_id, sf_type, start_time);
        coordinator.add_read_to_execute(cf_key, CALL_FORWARDING_TABLE);
    }

    let (status, results) = coordinator.tx_exe().await;
    if !status {
        coordinator.tx_abort().await;
        return false;
    }
    let specfac: SpecialFacility = match serde_json::from_str(results[0].value()) {
        Ok(s) => s,
        Err(_) => SpecialFacility::default(),
    };

    if !specfac.is_active {
        coordinator.tx_abort().await;
        return false;
    }

    for i in 1..results.len() {
        let iter = results.get(i).unwrap().value();
        let cf_obj: CallForwarding = serde_json::from_str(iter).unwrap();
        if cf_obj.start_time as u64 > start_time || cf_obj.end_time as u64 <= end_time {
            coordinator.tx_abort().await;
            return false;
        }
    }
    return coordinator.tx_commit().await;
}

async fn tx_get_access_data(coordinator: &mut DtxCoordinator) -> bool {
    coordinator.tx_begin(true).await;

    // build key
    let s_id = get_sid();
    let ai_type = rnd("ai_type") as u64;
    let a_id = ai_key(s_id, ai_type);
    coordinator.add_read_to_execute(a_id, ACCESS_INFO_TABLE);

    let (status, result) = coordinator.tx_exe().await;
    if !status || result.len() == 0 {
        coordinator.tx_abort().await;
        return false;
    }
    return coordinator.tx_commit().await;
}

async fn tx_update_subscriber_data(coordinator: &mut DtxCoordinator) -> bool {
    coordinator.tx_begin(false).await;
    // build key
    let s_id = get_sid();
    coordinator.add_read_to_execute(s_id, SUBSCRIBER_TABLE);
    let sub_write_obj = coordinator.add_write_to_execute(s_id, SUBSCRIBER_TABLE, "".to_string());
    let (status, read) = coordinator.tx_exe().await;

    if !status {
        coordinator.tx_abort().await;
        return false;
    }
    // already locked
    let mut sub_record: Subscriber = serde_json::from_str(read[0].value()).unwrap();
    sub_record.bit[0] = rnd("bit") != 0;
    let sf_type = rnd("sf_type") as u64;
    let sf_id = sf_key(s_id, sf_type);
    coordinator.add_read_to_execute(sf_id, SPECIAL_FACILITY_TABLE);
    let sf_write_obj =
        coordinator.add_write_to_execute(sf_id, SPECIAL_FACILITY_TABLE, "".to_string());

    let (status, read) = coordinator.tx_exe().await;

    if !status {
        coordinator.tx_abort().await;
        return false;
    }
    let mut sf_record: SpecialFacility = serde_json::from_str(read[0].value()).unwrap();
    sf_record.data_a = rnd("data") as u8;

    sub_write_obj.write().await.value = Some(serde_json::to_string(&sub_record).unwrap());
    sf_write_obj.write().await.value = Some(serde_json::to_string(&sf_record).unwrap());

    let commit_status = coordinator.tx_commit().await;

    return commit_status;
}

async fn tx_update_lcoation(coordinator: &mut DtxCoordinator) -> bool {
    coordinator.tx_begin(false).await;
    // build key
    let s_id = get_sid();
    coordinator.add_read_to_execute(s_id, SUBSCRIBER_TABLE);
    let sub_write_obj = coordinator.add_write_to_execute(s_id, SUBSCRIBER_TABLE, "".to_string());
    let (status, read) = coordinator.tx_exe().await;

    if !status {
        coordinator.tx_abort().await;
        return false;
    }

    // already locked
    let mut sub_record: Subscriber = serde_json::from_str(read[0].value()).unwrap();
    sub_record.vlr_location = rnd("vlr_location") as u32;

    sub_write_obj.write().await.value = Some(serde_json::to_string(&sub_record).unwrap());
    return coordinator.tx_commit().await;
}
