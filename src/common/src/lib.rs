// pub mod shmem;
pub mod throughput_statistics;
pub mod txn;

use std::{
    collections::{BTreeSet, HashSet},
    sync::atomic::{AtomicUsize, ATOMIC_USIZE_INIT},
    time::{SystemTime, UNIX_EPOCH},
    vec,
};

use chrono::Local;
use rand::*;
use rpc::common::Msg;
use serde::{Deserialize, Serialize};
use tokio::sync::{mpsc::UnboundedSender, oneshot::Sender as OneShotSender};

pub static GLOBAL_COMMITTED: AtomicUsize = ATOMIC_USIZE_INIT;

pub static UNCERTAINTY: u64 = 7;
pub static LOCAL_UNCERTAINTY: u64 = 1;

pub static SUBSCRIBER_TABLE: u32 = 0;
pub static SPECIAL_FACILITY_TABLE: u32 = 1;
pub static ACCESS_INFO_TABLE: u32 = 2;
pub static CALL_FORWARDING_TABLE: u32 = 3;
pub static SAVING_TABLE: u32 = 0;
pub static CHECKING_TABLE: u32 = 1;
pub static ACCOUNT_TABLE: u32 = 2;

pub static WAREHOUSE_TABLE: u32 = 0;
pub static DISTRICT_TABLE: u32 = 1;
pub static CUSTOMER_TABLE: u32 = 2;
pub static HISTORY_TABLE: u32 = 3;
pub static ORDER_TABLE: u32 = 4;
pub static NEWORDER_TABLE: u32 = 5;
pub static ORDERLINE_TABLE: u32 = 6;
pub static STOCK_TABLE: u32 = 7;
pub static ITEM_TABLE: u32 = 8;
pub static CID_LEN: u32 = 50;

#[derive(Clone)]
pub struct Tuple {
    lock_txn_id: u64, // 0 state for no lock
    read_lock: HashSet<u64>,
    // janus meta
    pub last_accessed: u64,
    pub ts: u64,
    pub data: String,
    // meerkat meta
    pub prepared_read: BTreeSet<u64>,
    pub prepared_write: BTreeSet<u64>,
    pub rts: u64,
}

impl Tuple {
    pub fn new(data: String) -> Self {
        Self {
            lock_txn_id: 0,
            ts: 0,
            data,
            prepared_read: BTreeSet::new(),
            prepared_write: BTreeSet::new(),
            rts: 0,
            read_lock: HashSet::new(),
            last_accessed: 0,
        }
    }

    pub fn is_locked(&self) -> bool {
        if self.lock_txn_id == 0 {
            return false;
        }
        true
    }
    pub fn set_lock(&mut self, txn_id: u64) -> bool {
        // println!("set lock {} {}", txn_id, self.lock_txn_id);
        if self.lock_txn_id == txn_id {
            return true;
        } else if self.lock_txn_id == 0 {
            self.lock_txn_id = txn_id;
            return true;
        }
        false
    }
    pub fn release_lock(&mut self, txn_id: u64) {
        if self.lock_txn_id == txn_id {
            self.lock_txn_id = 0;
        }
    }

    pub fn set_read_lock(&mut self, txn_id: u64) -> bool {
        if self.lock_txn_id != 0 {
            return false;
        }
        self.read_lock.insert(txn_id);
        return true;
    }
    pub fn release_read_lock(&mut self, txn_id: u64) -> bool {
        self.read_lock.remove(&txn_id);
        return true;
    }
}

#[derive(Serialize, Deserialize)]
pub struct ConfigInFile {
    pub db_type: String,
    pub dtx_type: String,
    // pub zipf: f64,
    pub client_num: u64,
    pub read_only: bool,
    pub txns_per_client: u64,
    pub geo: bool,
}

#[derive(PartialEq, Eq, Deserialize, Clone, Debug, Copy)]
pub enum DtxType {
    rocc,
    r2pl,
    spanner,
    ford,
    meerkat,
    janus,
    rjanus,
}

#[derive(Clone, Serialize, Deserialize, Copy)]
pub enum DbType {
    micro,
    tatp,
    smallbank,
    tpcc,
}

#[derive(Clone, Serialize, Deserialize)]
pub struct Config {
    pub server_addr: Vec<String>,
    pub server_public_addr: Vec<String>,
    pub client_addr: Vec<String>,
    pub client_public_addr: Vec<String>,
    pub geo_server_addr: Vec<String>,
    pub geo_server_public_addr: Vec<String>,
    pub geo_client_addr: Vec<String>,
    pub geo_client_public_addr: Vec<String>,
    pub perferred_server: Vec<u64>,
    // pub
    pub executor_num: u64,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            server_addr: vec![
                "192.168.1.88:10001".to_string(), //optane08
                "192.168.1.71:10001".to_string(), //optane11
                "192.168.1.72:10001".to_string(), //optane12
            ],
            server_public_addr: vec![
                "192.168.1.88:10001".to_string(), //optane08
                "192.168.1.71:10001".to_string(), //optane11
                "192.168.1.72:10001".to_string(), //optane12
            ],
            geo_server_public_addr: vec![
                "54.67.65.56:10001".to_string(),   //ca
                "54.92.36.220:10001".to_string(),  //jp
                "18.193.150.27:10001".to_string(), //Frankfurt
            ],
            geo_server_addr: vec![
                "172.31.6.187:10001".to_string(),  //ca
                "172.31.38.214:10001".to_string(), //jp
                "172.31.2.189:10001".to_string(),  //Frankfurt
            ],
            executor_num: 10,
            client_public_addr: vec![
                "192.168.1.70:10001".to_string(), //optane10
                "192.168.1.74:10001".to_string(), //optane14
                "192.168.1.75:10001".to_string(), //optane15
            ],
            client_addr: vec![
                "192.168.1.70:10001".to_string(), //optane10
                "192.168.1.74:10001".to_string(), //optane14
                "192.168.1.75:10001".to_string(), //optane15
            ],
            geo_client_public_addr: vec![
                "54.241.135.222:10001".to_string(),
                "54.95.228.225:10001".to_string(),
                "18.185.144.36:10001".to_string(),
                "54.241.135.222:20002".to_string(),
                "54.95.228.225:20002".to_string(),
                "18.185.144.36:20002".to_string(),
            ],
            geo_client_addr: vec![
                "172.31.2.199:10001".to_string(),
                "172.31.45.90:10001".to_string(),
                "172.31.8.219:10001".to_string(),
                "172.31.2.199:20002".to_string(),
                "172.31.45.90:20002".to_string(),
                "172.31.8.219:20002".to_string(),
            ],
            perferred_server: vec![0, 0, 0],
        }
    }
}

pub fn ip_addr_add_prefix(ip: String) -> String {
    let prefix = String::from("http://");
    prefix + ip.clone().as_str()
}

pub fn u64_rand(lower_bound: u64, upper_bound: u64) -> u64 {
    let mut rng = rand::thread_rng();
    rng.gen_range(lower_bound, upper_bound + 1)
}

pub fn u32_rand(lower_bound: u32, upper_bound: u32) -> u32 {
    let mut rng = rand::thread_rng();
    rng.gen_range(lower_bound, upper_bound + 1)
}

pub fn nurandom(A: u64, x: u64, y: u64) -> u64 {
    return ((u64_rand(0, A)) | (u64_rand(x, y))) % (y - x + 1) + x;
}

pub fn f64_rand(lower_bound: f64, upper_bound: f64, precision: f64) -> f64 {
    let mut rng = rand::thread_rng();
    rng.gen_range(
        (lower_bound / precision) as u64,
        (upper_bound / precision) as u64 + 1,
    ) as f64
        * precision
}

pub struct CoordnatorMsg {
    pub msg: Msg,
    pub call_back: OneShotSender<Msg>,
}

pub fn get_txnid(txnid: u64) -> (u64, u64) {
    let cid = (txnid >> CID_LEN) as u64;
    let tid = txnid - (cid << CID_LEN);
    (cid, tid)
}

pub fn get_currenttime_millis() -> u64 {
    Local::now().timestamp_millis() as u64
}

pub fn get_currenttime_micros() -> u64 {
    Local::now().timestamp_micros() as u64
}
