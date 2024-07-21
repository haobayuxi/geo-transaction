use common::{get_currenttime_micros, get_currenttime_millis, get_txnid, CoordnatorMsg, DtxType};
use rpc::common::GossipMessage;
use rpc::common::{data_service_client::DataServiceClient, Msg, TxnOp};
use std::{cmp::min, time::Duration};
use tokio::sync::mpsc::{unbounded_channel, Sender, UnboundedReceiver};
use tokio::{sync::oneshot::Sender as OneShotSender, time::sleep};
use tonic::transport::Channel;

use crate::data::get_dep_ts;
use crate::data_server::{LOCAL_CT, SAFE, WAITING};
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
    pub ts: Vec<u64>,
    pub id: u32,
    pub dtx: DtxType,
}

impl Gossip {
    pub async fn run(&mut self) {
        loop {
            match self.receiver.recv().await {
                Some(gossip) => {
                    // advance safe close timestamp
                    let from = gossip.from;
                    self.ts[from as usize] = gossip.ts;
                    self.ts.sort();
                    unsafe {
                        SAFE = self.ts[1];
                    }
                    if self.dtx == DtxType::ocean_vista {
                        loop {
                            unsafe {
                                match WAITING.pop_first() {
                                    Some((ts, coor_msg)) => {
                                        if coor_msg.msg.ts() < SAFE {
                                            // get dep
                                            let mut reply = coor_msg.msg.clone();

                                            reply.deps.push(get_dep_ts(reply.clone()).await);
                                            coor_msg.call_back.send(reply);
                                        } else {
                                            WAITING.insert(ts, coor_msg);
                                            break;
                                        }
                                    }
                                    None => break,
                                }
                            }
                        }
                    }
                }
                None => {
                    // error channel close
                }
            }
        }
    }

    pub async fn gossip_to_others(&self) {
        unsafe {
            let data_clients = PEER.clone();
            let gossip = if self.dtx == DtxType::mercury {
                GossipMessage {
                    ts: LOCAL_CT,
                    t_ids: Vec::new(),
                    from: self.id,
                }
            } else {
                GossipMessage {
                    ts: match WAITING.last_key_value() {
                        Some((ts, msg)) => *ts,
                        None => 0,
                    },
                    t_ids: Vec::new(),
                    from: self.id,
                }
            };
            async_broadcast(gossip, data_clients).await;
        }
    }

    pub async fn advance_local_close_timestamp() {
        unsafe {
            loop {
                sleep(Duration::from_millis(20)).await;
                LOCAL_CT += 20;
                // trigger the waiting list
                loop {
                    match WAITING.pop_first() {
                        Some((ts, coor_msg)) => {
                            if coor_msg.msg.ts() < LOCAL_CT {
                                // get dep
                                let mut reply = coor_msg.msg.clone();

                                reply.deps.push(get_dep_ts(reply.clone()).await);
                                coor_msg.call_back.send(reply);
                            } else {
                                WAITING.insert(ts, coor_msg);
                                break;
                            }
                        }
                        None => break,
                    }
                }
            }
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
