use std::collections::HashMap;

use common::{DbType, DtxType, Tuple};
use rpc::common::{Msg, ReadStruct};
use tokio::sync::RwLock;
use workload::{
    micro_db::init_micro_db, small_bank_db::init_smallbank_db, tatp_db::init_tatp_data,
    tpcc_db::init_tpcc_data,
};

use crate::dep_graph::{Node, TXNS};

pub static mut DATA: Vec<HashMap<u64, RwLock<Tuple>>> = Vec::new();

pub fn init_data(txn_type: DbType, client_num: u64) {
    unsafe {
        for _ in 0..client_num {
            let mut in_memory_node = Vec::new();
            in_memory_node.push(Node::default());
            TXNS.push(in_memory_node);
        }
        match txn_type {
            DbType::micro => {
                DATA = init_micro_db();
            }
            DbType::tatp => DATA = init_tatp_data(),
            DbType::smallbank => DATA = init_smallbank_db(),
            DbType::tpcc => DATA = init_tpcc_data(),
        }
    }
}

pub async fn validate(msg: Msg, dtx_type: DtxType) -> bool {
    unsafe {
        return true;
    }
}

pub async fn insert(insert: Vec<ReadStruct>) -> bool {
    unsafe {
        for iter in insert.iter() {
            let table = &mut DATA[iter.table_id as usize];
            // println!("table id = {}", )
            table.insert(
                iter.key,
                RwLock::new(Tuple::new(serde_json::to_string(iter.value()).unwrap())),
            );
        }
    }
    true
}

pub async fn delete(delete: Vec<ReadStruct>) -> bool {
    unsafe {
        for iter in delete.iter() {
            let table = &mut DATA[iter.table_id as usize];
            // println!("table id = {}", )
            table.remove(&iter.key);
        }
    }
    true
}

pub async fn get_read_only(read_set: Vec<ReadStruct>) -> (bool, Vec<ReadStruct>) {
    let mut result = Vec::new();
    unsafe {
        for iter in read_set.iter() {
            let table = &mut DATA[iter.table_id as usize];
            // println!("table id = {}", )
            match table.get_mut(&iter.key) {
                Some(rwlock) => {
                    let guard = rwlock.write().await;

                    let read_struct = ReadStruct {
                        key: iter.key,
                        table_id: iter.table_id,
                        value: Some(guard.data.clone()),
                        timestamp: Some(guard.ts),
                    };
                    result.push(read_struct);
                }
                None => return (false, result),
            }
        }
        (true, result)
    }
}

pub async fn get_read_set(
    read_set: Vec<ReadStruct>,
    txn_id: u64,
    dtx_type: DtxType,
) -> (bool, Vec<ReadStruct>) {
    let mut result = Vec::new();
    unsafe {
        for iter in read_set.iter() {
            let table = &mut DATA[iter.table_id as usize];
            // println!("table id = {}", )
            match table.get_mut(&iter.key) {
                Some(rwlock) => {
                    let mut guard = rwlock.write().await;

                    let read_struct = ReadStruct {
                        key: iter.key,
                        table_id: iter.table_id,
                        value: Some(guard.data.clone()),
                        timestamp: Some(guard.ts),
                    };
                    result.push(read_struct);
                }
                None => return (false, result),
            }
        }
        (true, result)
    }
}

pub async fn lock_write_set(write_set: Vec<ReadStruct>, txn_id: u64) -> (bool, Vec<ReadStruct>) {
    let mut result = Vec::new();
    // println!("{:?}", write_set);
    unsafe {
        for iter in write_set.iter() {
            let table = &mut DATA[iter.table_id as usize];
            match table.get_mut(&iter.key) {
                Some(lock) => {
                    let mut guard = lock.write().await;
                    if !guard.set_lock(txn_id) {
                        return (false, result);
                    }
                    // println!("locked{}", iter.key);
                    let read_struct = ReadStruct {
                        key: iter.key,
                        table_id: iter.table_id,
                        value: Some(guard.data.clone()),
                        timestamp: Some(guard.ts),
                    };
                    result.push(read_struct);
                }
                None => {
                    // println!("not found {} {}", iter.table_id, iter.key);
                    return (false, result);
                }
            }
        }
        (true, result)
    }
}

pub async fn release_read_set(read_set: Vec<ReadStruct>, txn_id: u64) -> bool {
    unsafe {
        for iter in read_set.iter() {
            let table = &mut DATA[iter.table_id as usize];
            match table.get_mut(&iter.key) {
                Some(lock) => {
                    let mut guard = lock.write().await;
                    if !guard.release_read_lock(txn_id) {
                        return false;
                    }
                }
                None => return false,
            }
        }
        true
    }
}

pub async fn update_and_release_locks(msg: Msg, dtx_type: DtxType) {
    unsafe {
        for iter in msg.write_set.iter() {
            let table = &mut DATA[iter.table_id as usize];
            match table.get_mut(&iter.key) {
                Some(lock) => {
                    let mut guard = lock.write().await;
                    guard.release_lock(msg.txn_id);
                    guard.data = iter.value.clone().unwrap();
                    guard.ts = msg.ts();
                    // println!("commit table{}key{} free", iter.table_id, iter.key);
                }
                None => {}
            }
        }
    }
}

pub async fn releass_locks(msg: Msg, dtx_type: DtxType) {
    unsafe {
        for iter in msg.write_set.iter() {
            let table = &mut DATA[iter.table_id as usize];
            match table.get_mut(&iter.key) {
                Some(lock) => {
                    let mut guard = lock.write().await;
                    guard.release_lock(msg.txn_id);
                    // println!("abort table{}key{}lock is free", iter.table_id, iter.key);
                }
                None => {}
            }
        }
    }
}

pub async fn get_dep_ts(msg: Msg) -> u64 {
    unsafe {
        let mut ts = msg.ts();

        for read in msg.read_set.iter() {
            let table = &mut DATA[read.table_id as usize];
            match table.get_mut(&read.key) {
                Some(lock) => {
                    let mut guard: tokio::sync::RwLockWriteGuard<Tuple> = lock.write().await;
                    match guard.prepared_write.last() {
                        Some(write) => {
                            if *write < msg.ts() {
                                guard.prepared_read.insert(msg.ts());
                            } else {
                                if ts < *write + 1 {
                                    ts = *write + 1;
                                }
                                guard.prepared_read.insert(msg.ts());
                            }
                        }
                        None => {
                            guard.prepared_read.insert(msg.ts());
                        }
                    }
                }
                None => {
                    return 0;
                }
            }
        }

        for write in msg.write_set.iter() {
            let table = &mut DATA[write.table_id as usize];
            match table.get_mut(&write.key) {
                Some(lock) => {
                    let mut guard = lock.write().await;
                    match guard.prepared_read.last() {
                        Some(read) => {
                            if *read < msg.ts() {
                                guard.prepared_write.insert(msg.ts());
                            } else {
                                if ts < *read + 1 {
                                    ts = *read + 1;
                                }
                                guard.prepared_write.insert(msg.ts());
                            }
                        }
                        None => {
                            guard.prepared_write.insert(msg.ts());
                        }
                    }
                }
                None => {
                    return 0;
                }
            }
        }
        return ts;
    }
}

pub async fn get_deps(msg: Msg) -> (bool, Vec<u64>, Vec<ReadStruct>) {
    unsafe {
        let mut deps = Vec::new();
        let mut read_results = Vec::new();

        for read in msg.read_set.iter() {
            let table = &mut DATA[read.table_id as usize];
            match table.get_mut(&read.key) {
                Some(lock) => {
                    let mut guard = lock.write().await;
                    if !deps.contains(&guard.last_accessed) {
                        deps.push(guard.last_accessed);
                    }
                    guard.last_accessed = msg.txn_id;
                    let read_struct = ReadStruct {
                        key: read.key,
                        table_id: read.table_id,
                        value: Some(guard.data.clone()),
                        timestamp: Some(guard.ts),
                    };
                    read_results.push(read_struct);
                }
                None => {
                    return (false, deps, read_results);
                }
            }
        }

        for write in msg.write_set.iter() {
            let table = &mut DATA[write.table_id as usize];
            match table.get_mut(&write.key) {
                Some(lock) => {
                    let mut guard = lock.write().await;
                    if !deps.contains(&guard.last_accessed) {
                        deps.push(guard.last_accessed);
                    }
                    guard.last_accessed = msg.txn_id;
                }
                None => {
                    return (false, deps, read_results);
                }
            }
        }

        return (true, deps, read_results);
    }
}
