use common::{get_currenttime_micros, get_currenttime_millis, get_txnid, CoordnatorMsg, DtxType};
use rpc::common::GossipMessage;
use rpc::common::{data_service_client::DataServiceClient, Msg, TxnOp};
use std::{cmp::min, time::Duration};
use tokio::sync::mpsc::{unbounded_channel, Sender, UnboundedReceiver};
use tokio::{sync::oneshot::Sender as OneShotSender, time::sleep};
use tonic::transport::Channel;

use crate::data_server::LOCAL_CT;
use crate::{
    data::{
        delete, get_deps, get_read_only, get_read_set, insert, lock_write_set, release_read_set,
        releass_locks, update_and_release_locks, validate,
    },
    data_server::{MAX_COMMIT_TS, PEER},
    dep_graph::{Node, TXNS},
};

pub struct Gossip {
    pub receiver: UnboundedReceiver<GossipMessage>,
}

pub async fn gossip_to_others() {
    unsafe {
        let data_clients = PEER.clone();
        let gossip = GossipMessage{
            ts: 0,
            t_ids: Vec::new(),
        };
        async_broadcast(gossip, data_clients).await;
    }
}

pub async fn advance_local_close_timestamp() {
    unsafe {
        loop {
            sleep(Duration::from_millis(20)).await;
            LOCAL_CT += 20;
            // trigger
        }
    }
}

async fn async_broadcast(gossip: GossipMessage, data_clients: Vec<DataServiceClient<Channel>>) {
    for iter in data_clients.iter() {
        let mut client = iter.clone();
        let msg_ = gossip.clone();
        tokio::spawn(async move {
            client.gossip(msg_).await.unwrap().into_inner();
        });
    }
}
