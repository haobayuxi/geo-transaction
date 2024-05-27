use common::{txn::DtxCoordinator, u64_rand, ACCOUNT_TABLE, CHECKING_TABLE, SAVING_TABLE};
use tokio::time::Instant;

use crate::small_bank_db::{get_c_id, Checking, Saving, MAX_BALANCE, MIN_BALANCE};

static FREQUENCY_AMALGAMATE: u64 = 15;
static FREQUENCY_BALANCE: u64 = 30;
static FREQUENCY_DEPOSIT_CHECKING: u64 = 45;
static FREQUENCY_TRANSACT_SAVINGS: u64 = 85;
static FREQUENCY_WRITE_CHECK: u64 = 100;

pub async fn small_bank_run_transactions(
    coordinator: &mut DtxCoordinator,
    txn_nums: u64,
) -> (Vec<u128>, f64) {
    let mut latency_result = Vec::new();
    let total_start = Instant::now();
    for i in 0..txn_nums {
        let start = Instant::now();
        let success = small_bank_run_transaction(coordinator).await;
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

async fn small_bank_run_transaction(coordinator: &mut DtxCoordinator) -> bool {
    let op = u64_rand(1, 100);
    if op < FREQUENCY_AMALGAMATE {
        return amalgamate(coordinator).await;
    } else if op < FREQUENCY_BALANCE {
        return balance(coordinator).await;
    } else if op < FREQUENCY_DEPOSIT_CHECKING {
        return deposit_checking(coordinator).await;
    } else if op < FREQUENCY_TRANSACT_SAVINGS {
        return transac_saving(coordinator).await;
    } else {
        return write_check(coordinator).await;
    }
}

// It returns the sum of savings and checking balances for the specified customer
async fn balance(coordinator: &mut DtxCoordinator) -> bool {
    coordinator.tx_begin(true).await;
    // get cid
    let c_name = get_c_id();
    coordinator.add_read_to_execute(c_name, ACCOUNT_TABLE);
    let (status, result) = coordinator.tx_exe().await;

    if !status {
        coordinator.tx_abort().await;
        return false;
    }
    let c_id: u64 = serde_json::from_str(result[0].value()).unwrap();
    coordinator.add_read_to_execute(c_id, SAVING_TABLE);
    coordinator.add_read_to_execute(c_id, CHECKING_TABLE);
    let (status, result) = coordinator.tx_exe().await;

    if !status {
        coordinator.tx_abort().await;
        return false;
    }
    return coordinator.tx_commit().await;
}

async fn deposit_checking(coordinator: &mut DtxCoordinator) -> bool {
    coordinator.tx_begin(false).await;
    let c_name = get_c_id();
    coordinator.add_read_to_execute(c_name, ACCOUNT_TABLE);
    let (status, result) = coordinator.tx_exe().await;

    if !status {
        coordinator.tx_abort().await;
        return false;
    }
    let c_id: u64 = serde_json::from_str(result[0].value()).unwrap();
    coordinator.add_read_to_execute(c_id, CHECKING_TABLE);

    let check_obj = coordinator.add_write_to_execute(c_id, CHECKING_TABLE, "".to_string());

    let (status, result) = coordinator.tx_exe().await;
    if !status {
        coordinator.tx_abort().await;
        return false;
    }
    let mut check_record: Checking = serde_json::from_str(result[0].value()).unwrap();
    check_record.balance += u64_rand(0, 10);
    check_obj.write().await.value = Some(serde_json::to_string(&check_record).unwrap());
    return coordinator.tx_commit().await;
}

async fn transac_saving(coordinator: &mut DtxCoordinator) -> bool {
    coordinator.tx_begin(false).await;
    let c_name = get_c_id();
    coordinator.add_read_to_execute(c_name, ACCOUNT_TABLE);
    let (status, result) = coordinator.tx_exe().await;

    if !status {
        coordinator.tx_abort().await;
        return false;
    }
    let c_id: u64 = serde_json::from_str(result[0].value()).unwrap();
    coordinator.add_read_to_execute(c_id, SAVING_TABLE);

    let save_obj = coordinator.add_write_to_execute(c_id, SAVING_TABLE, "".to_string());

    let (status, result) = coordinator.tx_exe().await;
    if !status {
        coordinator.tx_abort().await;
        return false;
    }
    let mut save_record: Saving = serde_json::from_str(result[0].value()).unwrap();
    let value = u64_rand(0, 10);
    // if value > save_record.balance {
    //     coordinator.tx_abort().await;
    //     return false;
    // }
    save_record.balance -= value;
    save_obj.write().await.value = Some(serde_json::to_string(&save_record).unwrap());
    return coordinator.tx_commit().await;
}

async fn amalgamate(coordinator: &mut DtxCoordinator) -> bool {
    coordinator.tx_begin(false).await;
    let c_name_from = get_c_id();
    let c_name_to = get_c_id();
    coordinator.add_read_to_execute(c_name_from, ACCOUNT_TABLE);
    coordinator.add_read_to_execute(c_name_to, ACCOUNT_TABLE);
    let (status, result) = coordinator.tx_exe().await;

    if !status {
        coordinator.tx_abort().await;
        return false;
    }
    let c_from: u64 = serde_json::from_str(result[0].value()).unwrap();
    let c_to: u64 = serde_json::from_str(result[1].value()).unwrap();
    coordinator.add_read_to_execute(c_from, SAVING_TABLE);
    coordinator.add_read_to_execute(c_from, CHECKING_TABLE);
    coordinator.add_read_to_execute(c_to, CHECKING_TABLE);

    let save_obj_from = coordinator.add_write_to_execute(c_from, SAVING_TABLE, "".to_string());
    let check_obj_from = coordinator.add_write_to_execute(c_from, CHECKING_TABLE, "".to_string());
    let check_obj_to = coordinator.add_write_to_execute(c_to, CHECKING_TABLE, "".to_string());

    let (status, result) = coordinator.tx_exe().await;

    if !status {
        coordinator.tx_abort().await;
        return false;
    }

    let mut save_record_from: Saving = serde_json::from_str(result[0].value()).unwrap();
    let mut check_record_from: Saving = serde_json::from_str(&result[1].value()).unwrap();
    let mut check_record_to: Saving = serde_json::from_str(&result[2].value()).unwrap();

    check_record_to.balance += save_record_from.balance + check_record_from.balance;
    save_record_from.balance = 0;
    check_record_from.balance = 0;

    save_obj_from.write().await.value = Some(serde_json::to_string(&save_record_from).unwrap());

    check_obj_from.write().await.value = Some(serde_json::to_string(&check_record_from).unwrap());
    check_obj_to.write().await.value = Some(serde_json::to_string(&check_record_to).unwrap());
    return coordinator.tx_commit().await;
}

async fn write_check(coordinator: &mut DtxCoordinator) -> bool {
    coordinator.tx_begin(false).await;
    let c_name = get_c_id();
    coordinator.add_read_to_execute(c_name, ACCOUNT_TABLE);
    let (status, result) = coordinator.tx_exe().await;

    if !status {
        coordinator.tx_abort().await;
        return false;
    }
    let c_id: u64 = serde_json::from_str(result[0].value()).unwrap();
    coordinator.add_read_to_execute(c_id, CHECKING_TABLE);

    let check_obj = coordinator.add_write_to_execute(c_id, CHECKING_TABLE, "".to_string());

    let (status, result) = coordinator.tx_exe().await;
    if !status {
        coordinator.tx_abort().await;
        return false;
    }
    let mut check_record: Checking = serde_json::from_str(result[0].value()).unwrap();
    check_record.balance -= u64_rand(0, 10);
    check_obj.write().await.value = Some(serde_json::to_string(&check_record).unwrap());
    return coordinator.tx_commit().await;
}
