use common::{f64_rand, u64_rand, Tuple};
use serde::{Deserialize, Serialize};
use std::{collections::HashMap, sync::Arc};
use tokio::sync::RwLock;

pub fn sf_key(s_id: u64, sf_type: u64) -> u64 {
    s_id << 4 + sf_type
}

pub fn cf_key(s_id: u64, sf_type: u64, start_time: u64) -> u64 {
    (s_id << 4 + sf_type) << 6 + start_time
}

pub fn ai_key(s_id: u64, ai_type: u64) -> u64 {
    s_id << 4 + ai_type
}

pub static mut SUBSCRIBER_FIELDS: Vec<&str> = Vec::new();
pub static mut ACCESS_INFO_FIELDS: Vec<&str> = Vec::new();
pub static mut SPECIAL_FACILITY_FIELDS: Vec<&str> = Vec::new();
pub static mut CALL_FORWARDING_FIELDS: Vec<&str> = Vec::new();

pub fn init_tatp_const() {
    unsafe {
        SUBSCRIBER_FIELDS = vec![
            "s_id",
            "sub_nbr",
            "bit_1",
            "bit_2",
            "bit_3",
            "bit_4",
            "bit_5",
            "bit_6",
            "bit_7",
            "bit_8",
            "bit_9",
            "bit_10",
            "hex_1",
            "hex_2",
            "hex_3",
            "hex_4",
            "hex_5",
            "hex_6",
            "hex_7",
            "hex_8",
            "hex_9",
            "hex_10",
            "bytes2_1",
            "bytes2_2",
            "bytes2_3",
            "bytes2_4",
            "bytes2_5",
            "bytes2_6",
            "bytes2_7",
            "bytes2_8",
            "bytes2_9",
            "bytes2_10",
            "msc_location",
            "vlr_location",
        ];
        ACCESS_INFO_FIELDS = vec!["s_id", "ai_type", "data1", "data2", "data3", "data4"];

        SPECIAL_FACILITY_FIELDS = vec![
            "s_id",
            "sf_type",
            "is_active",
            "error_cntrl",
            "data_a",
            "data_b",
        ];

        CALL_FORWARDING_FIELDS = vec!["s_id", "sf_type", "start_time", "end_time", "numberx"];
    }
}

pub fn rnd(param: &str) -> i64 {
    if param.starts_with("msc_location") || param.starts_with("vlr_location") {
        return u64_rand(0, 10000000) as i64;
    } else if param.starts_with("bit") {
        return u64_rand(0, 1) as i64;
    } else if param.starts_with("is_active") {
        let i = u64_rand(0, 99);
        if i <= 14 {
            return 0;
        } else {
            return 1;
        }
    } else if param.starts_with("hex") {
        return u64_rand(0, 15) as i64;
    } else if param.starts_with("byte") || param.starts_with("data") {
        return u64_rand(0, 255) as i64;
    } else if param.starts_with("start_time") {
        return (u64_rand(0, 2) * 8) as i64;
    } else if param.eq("end_time") {
        return (u64_rand(0, 2) * 8 + u64_rand(0, 7)) as i64;
    } else if param.eq("end_time") {
        return u64_rand(1, 8) as i64;
    } else if param.starts_with("ai_type") || param.starts_with("sf_type") {
        return u64_rand(0, 3) as i64;
    }
    0
}

#[derive(Serialize, Deserialize, Clone)]
pub struct Subscriber {
    pub s_id: u64,
    pub sub_nbr: String,
    pub bit: Vec<bool>,
    // pub bit_1: bool,
    // pub bit_2: bool,
    // pub bit_3: bool,
    // pub bit_4: bool,
    // pub bit_5: bool,
    // pub bit_6: bool,
    // pub bit_7: bool,
    // pub bit_8: bool,
    // pub bit_9: bool,
    // pub bit_10: bool,
    hex: Vec<u8>,
    // pub hex_1: u8,
    // pub hex_2: u8,
    // pub hex_3: u8,
    // pub hex_4: u8,
    // hex_5: u8,
    // hex_6: u8,
    // hex_7: u8,
    // hex_8: u8,
    // hex_9: u8,
    // hex_10: u8,
    bytes: Vec<u8>,
    // bytes2_1: u8,
    // bytes2_2: u8,
    // bytes2_3: u8,
    // bytes2_4: u8,
    // bytes2_5: u8,
    // bytes2_6: u8,
    // bytes2_7: u8,
    // bytes2_8: u8,
    // bytes2_9: u8,
    // bytes2_10: u8,
    pub msc_location: u32,
    pub vlr_location: u32,
}

impl Subscriber {
    pub fn new(s_id: u64) -> Self {
        let a: u16 = s_id as u16;
        let mut bit = Vec::new();
        let mut hex = Vec::new();
        let mut bytes = Vec::new();
        for i in 0..10 {
            bit.push(rnd("bit") != 0);
            hex.push(rnd("hex") as u8);
            bytes.push(rnd("bytes") as u8);
        }
        Self {
            s_id,
            sub_nbr: a.to_string(),
            bit,
            hex,
            bytes,
            msc_location: rnd("msc_location") as u32,
            vlr_location: rnd("vlr_location") as u32,
        }
    }
}

#[derive(Serialize, Deserialize, Clone)]
pub struct AccessInfo {
    pub s_id: u64,
    pub ai_type: u8,
    // data: Vec<u8>,
    pub data1: u8,
    pub data2: u8,
    data3: String,
    data4: String,
}

impl AccessInfo {
    pub fn new(s_id: u64, ai_type: u64) -> Self {
        Self {
            s_id,
            ai_type: ai_type as u8,
            data1: rnd("data") as u8,
            data2: rnd("data") as u8,
            data3: "ABC".to_string(),
            data4: "ABCDE".to_string(),
        }
    }
}

#[derive(Serialize, Deserialize, Clone)]
pub struct SpecialFacility {
    pub s_id: u64,
    pub sf_type: u8,
    pub is_active: bool,
    error_contrl: u8,
    pub data_a: u8,
    data_b: String,
}

impl SpecialFacility {
    pub fn new(s_id: u64, sf_type: u8) -> Self {
        Self {
            s_id,
            sf_type,
            is_active: rnd("is_active") != 0,
            error_contrl: 0,
            data_a: rnd("data") as u8,
            data_b: "ABCDE".to_string(),
        }
    }

    pub fn default() -> Self {
        Self {
            s_id: 0,
            sf_type: 0,
            is_active: false,
            error_contrl: 0,
            data_a: rnd("data") as u8,
            data_b: "ABCDE".to_string(),
        }
    }
}

#[derive(Serialize, Deserialize, Clone)]
pub struct CallForwarding {
    pub s_id: u64,
    pub sf_type: u8,
    pub start_time: u8,
    pub end_time: u8,
    pub numberx: String,
}

impl CallForwarding {
    pub fn new(s_id: u64, sf_type: u8, start_time: u8, end_time: u8) -> Self {
        Self {
            s_id,
            sf_type,
            start_time,
            end_time,
            numberx: "ABCD".to_string(),
        }
    }
}

pub fn init_tatp_data() -> Vec<HashMap<u64, RwLock<Tuple>>> {
    let mut data = Vec::new();
    let mut subscriber: HashMap<u64, RwLock<Tuple>> = HashMap::new();
    let mut access_info: HashMap<u64, RwLock<Tuple>> = HashMap::new();
    let mut special_facility: HashMap<u64, RwLock<Tuple>> = HashMap::new();
    let mut call_forwarding: HashMap<u64, RwLock<Tuple>> = HashMap::new();

    let start_times = vec![0, 8, 16];

    for s_id in 0..65535 {
        // init subscriber
        let subscriber_record = Subscriber::new(s_id);
        subscriber.insert(
            s_id,
            RwLock::new(Tuple::new(
                serde_json::to_string(&subscriber_record).unwrap(),
            )),
        );
        // init access_info
        let ai_record_amount = u64_rand(1, 4);
        let mut ai_type = vec![0; 4];
        for i in 0..ai_record_amount {
            loop {
                let ai = rnd("ai_type") as usize;
                if ai_type[ai] == 0 {
                    let access_info_record = AccessInfo::new(s_id, ai as u64);
                    access_info.insert(
                        s_id,
                        RwLock::new(Tuple::new(
                            serde_json::to_string(&access_info_record).unwrap(),
                        )),
                    );
                    ai_type[ai] = 1;
                    break;
                }
            }
        }
        // init special_facility record
        let subrecord_amount = u64_rand(1, 4);
        let mut sr_type = vec![0; 4];
        for i in 0..subrecord_amount {
            loop {
                let sr = rnd("sf_type") as usize;
                if sr_type[sr] == 0 {
                    // insert special_facility record
                    let special_facility_record = SpecialFacility::new(s_id, sr as u8);
                    special_facility.insert(
                        sf_key(s_id, sr as u64),
                        RwLock::new(Tuple::new(
                            serde_json::to_string(&special_facility_record).unwrap(),
                        )),
                    );

                    // init call_forwarding record
                    let callfw_amount = u64_rand(0, 3);
                    for j in 0..callfw_amount {
                        let start_time = start_times[j as usize];
                        let end_time = start_time + u64_rand(1, 8);
                        let call_forwarding_record =
                            CallForwarding::new(s_id, sr as u8, start_time as u8, end_time as u8);
                        call_forwarding.insert(
                            cf_key(s_id, sr as u64, start_time),
                            RwLock::new(Tuple::new(
                                serde_json::to_string(&call_forwarding_record).unwrap(),
                            )),
                        );
                    }

                    sr_type[sr] = 1;
                    break;
                }
            }
        }
    }
    data.push(subscriber);
    data.push(special_facility);
    data.push(access_info);
    data.push(call_forwarding);
    data
}

static SUBSCRIBER_ROWS: u64 = 10000;
pub fn get_sid() -> u64 {
    return u64_rand(1, SUBSCRIBER_ROWS);
}
