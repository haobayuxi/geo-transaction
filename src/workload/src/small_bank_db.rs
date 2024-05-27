use common::{u64_rand, Tuple};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use tokio::sync::RwLock;

static TX_HOT_PERCENT: u64 = 90;
static HOTSPOT_SIZE: u64 = 100;

pub static NUM_ACCOUNTS: u64 = 100000;
pub static MIN_BALANCE: u64 = 100000;
pub static MAX_BALANCE: u64 = 500000;

pub fn init_smallbank_db() -> Vec<HashMap<u64, RwLock<Tuple>>> {
    let mut tables = Vec::new();
    let mut saving_table = HashMap::new();
    let mut checking_table = HashMap::new();
    let mut account_table = HashMap::new();
    for i in 0..NUM_ACCOUNTS {
        let saving = Saving {
            customer_id: i,
            balance: u64_rand(MIN_BALANCE, MAX_BALANCE),
        };

        saving_table.insert(
            i,
            RwLock::new(Tuple::new(serde_json::to_string(&saving).unwrap())),
        );

        let checking = Checking {
            customer_id: i,
            balance: u64_rand(MIN_BALANCE, MAX_BALANCE),
        };
        checking_table.insert(
            i,
            RwLock::new(Tuple::new(serde_json::to_string(&checking).unwrap())),
        );
        account_table.insert(
            i,
            RwLock::new(Tuple::new(serde_json::to_string(&i).unwrap())),
        );
    }
    tables.push(saving_table);
    tables.push(checking_table);
    tables.push(account_table);
    tables
}

#[derive(Serialize, Deserialize)]
pub struct Saving {
    pub customer_id: u64,
    pub balance: u64,
}

impl Saving {
    pub fn new(customer_id: u64, balance: u64) -> Self {
        Self {
            customer_id,
            balance,
        }
    }
}

#[derive(Serialize, Deserialize)]
pub struct Checking {
    pub customer_id: u64,
    pub balance: u64,
}

impl Checking {
    pub fn new(customer_id: u64, balance: u64) -> Self {
        Self {
            customer_id,
            balance,
        }
    }
}

pub fn get_c_id() -> u64 {
    let is_hotspot = u64_rand(0, 99) < TX_HOT_PERCENT;
    if is_hotspot {
        return u64_rand(HOTSPOT_SIZE, NUM_ACCOUNTS - 1);
    } else {
        return u64_rand(0, HOTSPOT_SIZE - 1);
    }
}
