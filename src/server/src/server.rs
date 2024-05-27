use std::{collections::HashMap, env, sync::Arc};

use common::{Config, ConfigInFile, CoordnatorMsg};
use memory_server::{data_server::DataServer, executor::Executor};
use serde::{Deserialize, Serialize};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<String> = env::args().collect();
    let id = args[1].parse::<u32>().unwrap();
    let f = std::fs::File::open("config.yml").unwrap();
    let server_config: ConfigInFile = serde_yaml::from_reader(f).unwrap();
    let db_type: common::DbType = serde_yaml::from_str(&server_config.db_type).unwrap();
    let dtx_type = serde_yaml::from_str(&server_config.dtx_type).unwrap();
    let config = Config::default();
    let client_num = if server_config.geo {
        server_config.client_num * (config.geo_client_addr.len() as u64)
    } else {
        server_config.client_num * (config.client_addr.len() as u64)
    };
    let mut server = DataServer::new(id, config.clone(), client_num, dtx_type, server_config.geo);
    server.init_and_run(db_type, dtx_type).await;
    Ok(())
}
