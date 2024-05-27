use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

use common::{f64_rand, u64_rand, Tuple};
use rpc::common::ReadStruct;
use tokio::sync::RwLock;

const MicroTableSize: u64 = 100000;

pub fn init_micro_db() -> Vec<HashMap<u64, RwLock<Tuple>>> {
    let value: Vec<char> = vec!['a'; 40];
    let mut write_value = String::from("");
    write_value.extend(value.iter());
    let mut tables = Vec::new();
    let mut table = HashMap::new();
    for i in 0..MicroTableSize {
        table.insert(i, RwLock::new(Tuple::new(write_value.clone())));
    }
    tables.push(table);
    tables
}

fn zeta(n: u64, theta: f64) -> f64 {
    let mut sum: f64 = 0.0;
    for i in 1..(n + 1) {
        sum += f64::powf(1.0 / (i as f64), theta);
    }
    return sum;
}

pub struct MicroQuery {
    zeta_2_theta: f64,
    denom: f64,
    write_value: String,
    req_per_query: i32,
    table_size: u64,
    read_perc: i32,
    theta: f64,
    pub read_only: bool,
}

impl MicroQuery {
    pub fn new(read_only: bool) -> Self {
        let theta = if read_only { 0.0 } else { 0.8 };
        let req_per_query = if read_only { 2 } else { 4 };
        let read_perc = 80;
        let zeta_2_theta = zeta(2, theta);
        let value: Vec<char> = vec!['a'; 40];
        let mut write_value = String::from("");
        write_value.extend(value.iter());
        Self {
            zeta_2_theta,
            denom: zeta(MicroTableSize as u64, theta),
            write_value,
            req_per_query,
            table_size: MicroTableSize,
            read_perc,
            theta,
            read_only,
        }
    }

    pub fn generate(&mut self) -> (Vec<ReadStruct>, Vec<ReadStruct>) {
        let mut read_set = Vec::new();
        let mut write_set = Vec::new();
        let mut keys = Vec::new();
        let op = u64_rand(0, 1) == 0;
        let read_only = if op || self.read_only { true } else { false };
        for _ in 0..self.req_per_query {
            let key = self.zipf(self.table_size, self.theta);

            if keys.contains(&key) {
                continue;
            } else {
                keys.push(key);
            }
            if read_only {
                read_set.push(ReadStruct {
                    key,
                    value: None,
                    timestamp: None,
                    table_id: 0,
                });
            } else {
                write_set.push(ReadStruct {
                    key,
                    value: Some(self.write_value.clone()),
                    table_id: 0,
                    timestamp: None,
                });
            }
        }
        (read_set, write_set)
    }

    fn zipf(&self, n: u64, theta: f64) -> u64 {
        let zetan = self.denom;

        let u = f64_rand(0.0, 1.0, 0.000_000_000_000_001);
        let uz = u * zetan;
        if uz < 1.0 {
            return 0;
        }
        if uz < 1.0 + f64::powf(0.5, theta) {
            return 1;
        }
        let alpha = 1.0 / (1.0 - theta);

        let eta =
            (1.0 - f64::powf(2.0 / n as f64, 1.0 - theta)) / (1.0 - self.zeta_2_theta / zetan);
        let mut v = ((n as f64) * f64::powf(eta * u - eta + 1.0, alpha)) as u64;
        if v >= n {
            v = n - 1;
        }
        v
    }
}
