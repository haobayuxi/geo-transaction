use std::collections::{HashMap, HashSet};

use common::{get_currenttime_millis, nurandom, u64_rand, Tuple};
use serde::{Deserialize, Serialize};
use tokio::sync::RwLock;

pub static MIN_STREET: u64 = 10;
pub static MAX_STREET: u64 = 20;
pub static MIN_CITY: u64 = 10;
pub static MAX_CITY: u64 = 20;
pub static STATE: u64 = 2;
pub static ZIP: u64 = 9;
pub static MIN_NAME: u64 = 6;
pub static MAX_NAME: u64 = 10;
pub static MAX_ITEM: u64 = 10000;
pub static MIN_STOCK_LEVEL_THRESHOLD: u64 = 10;
pub static MAX_STOCK_LEVEL_THRESHOLD: u64 = 20;
pub static MIN_OL_CNT: u64 = 5;
pub static MAX_OL_CNT: u64 = 15;
pub static MIN_CARRIER_ID: u64 = 1;
pub static MAX_CARRIER_ID: u64 = 10;

///////
pub static NUM_CUSTOMER_PER_DISTRICT: u64 = 3000;
pub static NUM_WAREHOUSE: u64 = 10;
pub static NUM_DISTRICT_PER_WAREHOUSE: u64 = 10;
pub static NUM_ITEM: u64 = 1000;
pub static NUM_STOCK_PER_WAREHOUSE: u64 = 100000;

pub fn district_index(d_id: u64, w_id: u64) -> u64 {
    w_id << 20 + d_id
}

pub fn customer_index(c_id: u64, d_id: u64, w_id: u64) -> u64 {
    w_id << 40 + d_id << 30 + c_id
}

pub fn history_index(c_id: u64, d_id: u64, w_id: u64) -> u64 {
    w_id << 40 + d_id << 30 + c_id
}

pub fn order_index(o_id: u64, d_id: u64, w_id: u64) -> u64 {
    w_id << 40 + d_id << 32 + o_id
}

pub fn orderline_index(o_id: u64, d_id: u64, number: u64, w_id: u64) -> u64 {
    w_id << 50 + d_id << 40 + o_id << 30 + number
}

pub fn neworder_index(o_id: u64, d_id: u64, w_id: u64) -> u64 {
    w_id << 40 + d_id << 32 + o_id
}

pub fn init_tpcc_data() -> Vec<HashMap<u64, RwLock<Tuple>>> {
    let mut data = Vec::new();
    let mut warehouse_table: HashMap<u64, RwLock<Tuple>> = HashMap::new();
    let mut district_table: HashMap<u64, RwLock<Tuple>> = HashMap::new();
    let mut customer_table: HashMap<u64, RwLock<Tuple>> = HashMap::new();
    let mut history_table: HashMap<u64, RwLock<Tuple>> = HashMap::new();
    let mut order_table: HashMap<u64, RwLock<Tuple>> = HashMap::new();
    let mut orderline_table: HashMap<u64, RwLock<Tuple>> = HashMap::new();
    let mut neworder_table: HashMap<u64, RwLock<Tuple>> = HashMap::new();
    let mut item_table: HashMap<u64, RwLock<Tuple>> = HashMap::new();
    let mut stock_table: HashMap<u64, RwLock<Tuple>> = HashMap::new();

    // populate warehouse table
    for w_id in 1..=NUM_WAREHOUSE {
        let ware_house_record = Warehouse::new(w_id);
        warehouse_table.insert(
            w_id,
            RwLock::new(Tuple::new(
                serde_json::to_string(&ware_house_record).unwrap(),
            )),
        );

        // populate district table
        for d_id in 1..=NUM_DISTRICT_PER_WAREHOUSE {
            let district_record = District::new(d_id, w_id);
            district_table.insert(
                district_index(d_id, w_id),
                RwLock::new(Tuple::new(serde_json::to_string(&district_record).unwrap())),
            );
        }
        // populate customer and history table
        for d_id in 1..=NUM_DISTRICT_PER_WAREHOUSE {
            for c_id in 1..=NUM_CUSTOMER_PER_DISTRICT {
                let customer_record = Customer::new(c_id, d_id, w_id);
                customer_table.insert(
                    customer_index(c_id, d_id, w_id),
                    RwLock::new(Tuple::new(serde_json::to_string(&customer_record).unwrap())),
                );

                let history_record = History::new(c_id, d_id, w_id);
                history_table.insert(
                    history_index(c_id, d_id, w_id),
                    RwLock::new(Tuple::new(serde_json::to_string(&history_record).unwrap())),
                );
            }
        }

        // populate order, new order, order line tables
        for d_id in 1..=NUM_DISTRICT_PER_WAREHOUSE {
            // generate c_ids
            let mut cid_set = HashSet::new();
            let mut cid_vec = Vec::new();
            while cid_set.len() != NUM_CUSTOMER_PER_DISTRICT as usize {
                let cid = u64_rand(0, NUM_CUSTOMER_PER_DISTRICT) + 1;
                if cid_set.contains(&cid) {
                    continue;
                }
                cid_set.insert(cid);
                cid_vec.push(cid);
            }
            for c in 1..=NUM_CUSTOMER_PER_DISTRICT {
                let c_id = cid_vec[(c - 1) as usize];
                let o_id = order_index(c_id, d_id, w_id);
                let order_record = Order::new(o_id, d_id, w_id, c_id);
                order_table.insert(
                    o_id,
                    RwLock::new(Tuple::new(serde_json::to_string(&order_record).unwrap())),
                );
                if c > 2100 {
                    // new order
                    let neworder_record = NewOrder::new(o_id, d_id);
                    neworder_table.insert(
                        neworder_index(o_id, d_id, w_id),
                        RwLock::new(Tuple::new(serde_json::to_string(&neworder_record).unwrap())),
                    );
                }
                for ol in 1..=order_record.o_ol_cnt {
                    // order line
                    let orderline_record = Orderline::new(o_id, d_id, 0, order_record.o_entry_d);

                    orderline_table.insert(
                        orderline_index(o_id, d_id, ol, w_id),
                        RwLock::new(Tuple::new(
                            serde_json::to_string(&orderline_record).unwrap(),
                        )),
                    );
                }
            }
        }
    }

    // populate item table
    for i_id in 1..=MAX_ITEM {
        let item_record = Item::new(i_id);
        item_table.insert(
            i_id,
            RwLock::new(Tuple::new(serde_json::to_string(&item_record).unwrap())),
        );
    }

    // populate stock table
    for i_id in 1..=MAX_ITEM {
        let stock_record = Stock::new(i_id, 0);
        stock_table.insert(
            i_id,
            RwLock::new(Tuple::new(serde_json::to_string(&stock_record).unwrap())),
        );
    }

    ////////
    data.push(warehouse_table);
    data.push(district_table);
    data.push(customer_table);
    data.push(history_table);
    data.push(order_table);
    data.push(neworder_table);
    data.push(orderline_table);
    data.push(stock_table);
    data.push(item_table);
    data
}

#[derive(Serialize, Deserialize, Clone)]
pub struct Warehouse {
    pub w_id: u64,
    pub w_name: String,
    pub w_street1: String,
    pub w_street2: String,
    pub w_city: String,
    pub w_state: String,
    pub w_zip: String,
    pub w_tax: f64,
    pub w_ytd: f64,
}

impl Warehouse {
    pub fn new(w_id: u64) -> Self {
        Self {
            w_id,
            w_name: u64_rand(MIN_NAME, MAX_NAME).to_string(),
            w_street1: u64_rand(MIN_STREET, MAX_STREET).to_string(),
            w_street2: u64_rand(MIN_STREET, MAX_STREET).to_string(),
            w_city: u64_rand(MIN_CITY, MAX_CITY).to_string(),
            w_state: u64_rand(0, STATE).to_string(),
            w_zip: "12345678".to_string(),
            w_tax: u64_rand(0, 2000) as f64 / 10000.0,
            w_ytd: 300000.0,
        }
    }
}

impl Default for Warehouse {
    fn default() -> Self {
        Self {
            w_id: Default::default(),
            w_name: Default::default(),
            w_street1: Default::default(),
            w_street2: Default::default(),
            w_city: Default::default(),
            w_state: Default::default(),
            w_zip: Default::default(),
            w_tax: Default::default(),
            w_ytd: Default::default(),
        }
    }
}

#[derive(Serialize, Deserialize, Clone)]
pub struct District {
    pub d_id: u64,
    pub d_w_id: u64,
    pub d_name: String,
    pub d_strict1: String,
    pub d_strict2: String,
    pub d_city: String,
    pub d_state: String,
    pub d_zip: String,
    pub d_tax: f64,
    pub d_ytd: f64,
    pub d_next_o_id: u64,
}

impl Default for District {
    fn default() -> Self {
        Self {
            d_id: Default::default(),
            d_w_id: Default::default(),
            d_name: Default::default(),
            d_strict1: Default::default(),
            d_strict2: Default::default(),
            d_city: Default::default(),
            d_state: Default::default(),
            d_zip: Default::default(),
            d_tax: Default::default(),
            d_ytd: Default::default(),
            d_next_o_id: Default::default(),
        }
    }
}

impl District {
    pub fn new(d_id: u64, d_w_id: u64) -> Self {
        Self {
            d_id,
            d_w_id,
            d_name: u64_rand(MIN_NAME, MAX_NAME).to_string(),
            d_strict1: u64_rand(MIN_STREET, MAX_STREET).to_string(),
            d_strict2: u64_rand(MIN_STREET, MAX_STREET).to_string(),
            d_city: u64_rand(MIN_CITY, MAX_CITY).to_string(),
            d_state: u64_rand(0, STATE).to_string(),
            d_zip: "12345678".to_string(),
            d_tax: u64_rand(0, 2000) as f64 / 10000.0,
            d_ytd: 30000.0,
            d_next_o_id: NUM_CUSTOMER_PER_DISTRICT,
        }
    }
}

#[derive(Serialize, Deserialize, Clone)]
pub struct Customer {
    pub c_id: u64,
    pub c_d_id: u64,
    pub c_w_id: u64,
    pub c_first: String,
    pub c_middle: String,
    pub c_last: String,
    pub c_street1: String,
    pub c_street2: String,
    pub c_city: String,
    pub c_state: String,
    pub c_zip: String,
    pub c_phone: String,
    pub c_since: u64,
    pub c_credit: String,
    pub c_credit_lim: i64,
    pub c_discount: f64,
    pub c_balance: f64,
    pub c_ytd_payment: f64,
    pub c_payment_cnt: u64,
    pub c_delivery_cnt: u64,
    pub c_data: String,
}

impl Customer {
    pub fn new(c_id: u64, d_id: u64, w_id: u64) -> Self {
        Self {
            c_id,
            c_d_id: d_id,
            c_w_id: w_id,
            c_first: "firstname".to_string(),
            c_middle: "OE".to_string(),
            c_last: if c_id <= NUM_CUSTOMER_PER_DISTRICT / 3 {
                (c_id - 1).to_string()
            } else {
                nurandom(255, 0, 1000).to_string()
            },
            c_street1: u64_rand(MIN_STREET, MAX_STREET).to_string(),
            c_street2: u64_rand(MIN_STREET, MAX_STREET).to_string(),
            c_city: u64_rand(MIN_CITY, MAX_CITY).to_string(),
            c_state: u64_rand(0, STATE).to_string(),
            c_zip: "11111".to_string(),
            c_phone: "11111".to_string(),
            c_since: 100,
            c_credit: if u64_rand(0, 100) <= 10 {
                "BC".to_string()
            } else {
                "GC".to_string()
            },
            c_credit_lim: 50000,
            c_discount: u64_rand(1, 5000) as f64 / 10000.0,
            c_balance: -10.0,
            c_ytd_payment: 10.0,
            c_payment_cnt: 1,
            c_delivery_cnt: 1,
            c_data: "data".to_string(),
        }
    }
}

impl Default for Customer {
    fn default() -> Self {
        Self {
            c_id: Default::default(),
            c_d_id: Default::default(),
            c_w_id: Default::default(),
            c_first: Default::default(),
            c_middle: Default::default(),
            c_last: Default::default(),
            c_street1: Default::default(),
            c_street2: Default::default(),
            c_city: Default::default(),
            c_state: Default::default(),
            c_zip: Default::default(),
            c_phone: Default::default(),
            c_since: Default::default(),
            c_credit: Default::default(),
            c_credit_lim: Default::default(),
            c_discount: Default::default(),
            c_balance: Default::default(),
            c_ytd_payment: Default::default(),
            c_payment_cnt: Default::default(),
            c_delivery_cnt: Default::default(),
            c_data: Default::default(),
        }
    }
}

#[derive(Serialize, Deserialize, Clone)]
pub struct History {
    pub h_c_id: u64,
    pub h_c_d_id: u64,
    pub h_c_w_id: u64,
    pub h_d_id: u64,
    pub h_w_id: u64,
    pub h_date: u64,
    pub h_amount: f64,
    pub h_data: String,
}

impl History {
    pub fn new(c_id: u64, d_id: u64, w_id: u64) -> Self {
        Self {
            h_c_id: c_id,
            h_c_d_id: d_id,
            h_c_w_id: w_id,
            h_d_id: d_id,
            h_w_id: w_id,
            h_date: get_currenttime_millis(),
            h_amount: 10.0,
            h_data: "data".to_string(),
        }
    }
}

impl Default for History {
    fn default() -> Self {
        Self {
            h_c_id: Default::default(),
            h_c_d_id: Default::default(),
            h_c_w_id: Default::default(),
            h_d_id: Default::default(),
            h_w_id: Default::default(),
            h_date: Default::default(),
            h_amount: Default::default(),
            h_data: Default::default(),
        }
    }
}

#[derive(Serialize, Deserialize, Clone)]
pub struct Historydata {
    pub h_c_id: u64,
    pub h_c_d_id: u64,
    pub h_c_w_id: u64,
    pub h_d_id: u64,
    pub h_w_id: u64,
    pub h_amount: f64,
}

impl Historydata {
    pub fn new(history: History) -> Self {
        Self {
            h_c_id: history.h_c_id,
            h_c_d_id: history.h_c_d_id,
            h_c_w_id: history.h_c_w_id,
            h_d_id: history.h_d_id,
            h_w_id: history.h_w_id,
            h_amount: history.h_amount,
        }
    }
}

#[derive(Serialize, Deserialize, Clone)]
pub struct NewOrder {
    pub no_o_id: u64,
    pub no_d_id: u64,
    pub no_w_id: u64,
}

impl NewOrder {
    pub fn new(o_id: u64, d_id: u64) -> Self {
        Self {
            no_o_id: o_id,
            no_d_id: d_id,
            no_w_id: 0,
        }
    }
}

impl Default for NewOrder {
    fn default() -> Self {
        Self {
            no_o_id: Default::default(),
            no_d_id: Default::default(),
            no_w_id: Default::default(),
        }
    }
}

#[derive(Serialize, Deserialize, Clone)]
pub struct Order {
    pub o_id: u64,
    pub o_d_id: u64,
    pub o_w_id: u64,
    pub o_c_id: u64,
    pub o_entry_d: u64,
    pub o_carried_id: u64,
    pub o_ol_cnt: u64,
    pub o_all_local: u64,
}

impl Order {
    pub fn new(o_id: u64, o_d_id: u64, o_w_id: u64, o_c_id: u64) -> Self {
        Self {
            o_id,
            o_d_id,
            o_w_id,
            o_c_id,
            o_entry_d: get_currenttime_millis(),
            o_carried_id: u64_rand(1, 10),
            o_ol_cnt: u64_rand(5, 15),
            o_all_local: 1,
        }
    }
}

impl Default for Order {
    fn default() -> Self {
        Self {
            o_id: Default::default(),
            o_d_id: Default::default(),
            o_w_id: Default::default(),
            o_c_id: Default::default(),
            o_entry_d: Default::default(),
            o_carried_id: Default::default(),
            o_ol_cnt: Default::default(),
            o_all_local: Default::default(),
        }
    }
}

#[derive(Serialize, Deserialize, Clone)]
pub struct Orderline {
    pub ol_o_id: u64,
    pub ol_d_id: u64,
    pub ol_w_id: u64,
    pub ol_number: u64,
    pub ol_i_id: u64,
    pub ol_supply_w_id: u64,
    pub ol_delivery_d: u64,
    pub ol_quantity: u64,
    pub ol_amount: f64,
    pub ol_dist_info: String,
}

impl Orderline {
    pub fn new(o_id: u64, d_id: u64, w_id: u64, entry_d: u64) -> Self {
        Self {
            ol_o_id: o_id,
            ol_d_id: d_id,
            ol_w_id: w_id,
            ol_number: 0,
            ol_i_id: u64_rand(1, MAX_ITEM),
            ol_supply_w_id: w_id,
            ol_delivery_d: if o_id <= 2100 { entry_d } else { 0 },
            ol_quantity: 5,
            ol_amount: if o_id <= 2100 {
                0.0
            } else {
                u64_rand(1, 1000000) as f64 / 100.0
            },
            ol_dist_info: "dist_info".to_string(),
        }
    }
}

impl Default for Orderline {
    fn default() -> Self {
        Self {
            ol_o_id: Default::default(),
            ol_d_id: Default::default(),
            ol_w_id: Default::default(),
            ol_number: Default::default(),
            ol_i_id: Default::default(),
            ol_supply_w_id: Default::default(),
            ol_delivery_d: Default::default(),
            ol_quantity: Default::default(),
            ol_amount: Default::default(),
            ol_dist_info: Default::default(),
        }
    }
}
#[derive(Serialize, Deserialize, Clone)]
pub struct Item {
    pub i_id: u64,
    pub i_im_id: u64,
    pub i_name: String,
    pub i_price: f64,
    pub i_data: String,
}

impl Item {
    pub fn new(i_id: u64) -> Self {
        Self {
            i_id,
            i_im_id: u64_rand(1, MAX_ITEM),
            i_name: u64_rand(MIN_NAME, MAX_NAME).to_string(),
            i_price: u64_rand(100, 10000) as f64 / 100.0,
            i_data: "item".to_string(),
        }
    }
}

impl Default for Item {
    fn default() -> Self {
        Self {
            i_id: Default::default(),
            i_im_id: Default::default(),
            i_name: Default::default(),
            i_price: Default::default(),
            i_data: Default::default(),
        }
    }
}
#[derive(Serialize, Deserialize, Clone)]
pub struct Stock {
    pub s_i_id: u64,
    pub s_w_id: u64,
    pub s_quantity: u64,
    pub s_dist: Vec<String>,
    pub s_ytd: u64,
    pub s_order_cnt: u64,
    pub s_remote_cnt: u64,
    pub s_data: String,
}

impl Stock {
    pub fn new(i_id: u64, w_id: u64) -> Self {
        Self {
            s_i_id: i_id,
            s_w_id: w_id,
            s_quantity: u64_rand(10, 100),
            s_dist: Vec::new(),
            s_ytd: 0,
            s_order_cnt: 0,
            s_remote_cnt: 0,
            s_data: "stock_data".to_string(),
        }
    }
}

impl Default for Stock {
    fn default() -> Self {
        Self {
            s_i_id: Default::default(),
            s_w_id: Default::default(),
            s_quantity: Default::default(),
            s_dist: Default::default(),
            s_ytd: Default::default(),
            s_order_cnt: Default::default(),
            s_remote_cnt: Default::default(),
            s_data: Default::default(),
        }
    }
}
