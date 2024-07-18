use common::{get_currenttime_micros, get_currenttime_millis, get_txnid, CoordnatorMsg, DtxType};
use rpc::common::{data_service_client::DataServiceClient, Msg, TxnOp};
use std::{cmp::min, time::Duration};
use tokio::sync::mpsc::{unbounded_channel, Sender, UnboundedReceiver};
use tokio::{sync::oneshot::Sender as OneShotSender, time::sleep};
use tonic::transport::Channel;

use crate::{
    data::{
        delete, get_deps, get_read_only, get_read_set, insert, lock_write_set, release_read_set,
        releass_locks, update_and_release_locks, validate,
    },
    data_server::{MAX_COMMIT_TS, PEER},
    dep_graph::{Node, TXNS},
};

pub struct GossipServer {
    executor_num: u64,
    addr_to_listen: String,
    sender: Arc<HashMap<u64, UnboundedSender<CoordnatorMsg>>>,
}



