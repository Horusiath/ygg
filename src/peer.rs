use crate::{Error, Result, TTL};
use bytes::Bytes;
use futures_util::{Sink, SinkExt, Stream, StreamExt};
use rand::distributions::Alphanumeric;
use rand::{thread_rng, Rng};
use serde::{Deserialize, Serialize};
use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::fmt::Formatter;
use std::sync::{Arc, Weak};
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::sync::{Mutex, RwLock};

pub type PeerId = Arc<str>;

#[derive(Debug)]
pub struct Peer<C>
where
    C: Connector,
{
    connector: Arc<C>,
    connections: Arc<RwLock<HashMap<PeerId, Arc<ActivePeer<C::Sink>>>>>,
    options: Options,
    events: Sender<Event>,
    events_reader: Option<Receiver<Event>>,
}

impl<C> Peer<C>
where
    C: Connector + 'static,
{
    pub fn new(connector: C, options: Options) -> Self {
        let connector = Arc::new(connector);
        let connections = Arc::new(RwLock::new(HashMap::new()));
        let (tx, rx) = tokio::sync::mpsc::channel(8);

        let acceptor_job = {
            let server = connector.clone();
            let connections = Arc::downgrade(&connections);
            let mailbox = tx.clone();
            tokio::spawn(async move {
                loop {
                    match server.accept().await {
                        Ok((peer_info, sink, source)) => {
                            if let Some(connections) = connections.upgrade() {
                                let peer = ActivePeer::new(peer_info.clone(), sink);
                                {
                                    let peer_id = peer_info.id.clone();
                                    let mut connections = connections.write().await;
                                    match connections.entry(peer_id.clone()) {
                                        Entry::Occupied(mut e) => {
                                            log::info!(
                                                "replacing existing peer connection '{peer_id}'"
                                            );
                                            let old = e.insert(peer.clone());

                                            let graceful = if let Err(e) = old.close().await {
                                                log::warn!(
                                                    "failed to gracefully close '{peer_id}': {e}"
                                                );
                                                false
                                            } else {
                                                true
                                            };

                                            let _ = mailbox
                                                .send(Event::down(graceful, peer_info.clone()))
                                                .await;
                                            let _ = mailbox
                                                .send(Event::up(peer_info.clone(), false))
                                                .await;
                                        }
                                        Entry::Vacant(e) => {
                                            e.insert(peer.clone());
                                            let _ = mailbox
                                                .send(Event::up(peer_info.clone(), false))
                                                .await;
                                        }
                                    }
                                }

                                let receiver_loop = tokio::spawn(Self::handle_conn(
                                    source,
                                    peer_info.clone(),
                                    mailbox.clone(),
                                    Arc::downgrade(&connections),
                                ));
                            } else {
                                break;
                            }
                        }
                        Err(e) => {
                            log::error!("failed to accept incoming connection: {e}");
                            break;
                        }
                    }
                }
            })
        };
        Peer {
            connector,
            options,
            connections,
            events: tx,
            events_reader: Some(rx),
        }
    }

    pub fn options(&self) -> &Options {
        &self.options
    }

    pub fn myself(&self) -> &PeerInfo {
        &self.options.myself
    }
    pub fn id(&self) -> &PeerId {
        &self.options.myself.id
    }

    /// Claim the control over the receiver channel, enabled to read incoming events.
    /// Only one owner can claim the control over receiver channel. Subsequent requests will
    /// return `None`.
    pub fn claim(&mut self) -> Option<Receiver<Event>> {
        self.events_reader.take()
    }

    pub(crate) async fn send<M>(&self, recipient: &PeerId, msg: &M) -> Result<()>
    where
        M: Serialize + std::fmt::Debug,
    {
        loop {
            let result = {
                let connections = self.connections.read().await;
                if let Some(conn) = connections.get(recipient) {
                    let data = Bytes::from(serde_json::to_vec(msg)?);
                    conn.send(data).await.map(|_| None)
                } else {
                    let id = self.id();
                    log::trace!("'{id}' establishing new connection to '{recipient}'");
                    let (peer_info, sink, source) = self.connector.connect(recipient).await?;
                    let peer = ActivePeer::new(peer_info, sink);
                    Ok(Some((peer, source)))
                }
            };
            match result {
                Ok(None) => {
                    let id = self.id();
                    log::trace!("'{id}' successfully sent message to '{recipient}': {msg:?}");
                    return Ok(());
                }
                Ok(Some((peer, source))) => {
                    let peer_info = peer.peer_info.clone();
                    {
                        let mut connections = self.connections.write().await;
                        match connections.entry(recipient.clone()) {
                            Entry::Occupied(mut e) => {
                                log::info!("replacing existing peer connection '{recipient}'");
                                let old = e.insert(peer.clone());
                                let graceful = if let Err(e) = old.close().await {
                                    log::warn!("failed to gracefully close '{recipient}': {e}");
                                    false
                                } else {
                                    true
                                };
                                let _ = self
                                    .events
                                    .send(Event::down(graceful, old.peer_info.clone()))
                                    .await;
                            }
                            Entry::Vacant(e) => {
                                e.insert(peer.clone());
                            }
                        }
                    }
                    let _ = self.events.send(Event::up(peer_info.clone(), true)).await;
                    let receiver_loop = tokio::spawn(Self::handle_conn(
                        source,
                        peer_info,
                        self.events.clone(),
                        Arc::downgrade(&self.connections),
                    ));
                    continue;
                }
                Err(e) => {
                    log::warn!("failed to sent message to '{recipient}': {msg:?} - {e}");
                    let conn = {
                        let mut connections = self.connections.write().await;
                        connections.remove(recipient)
                    };
                    if let Some(c) = conn {
                        if let Err(e) = c.close().await {
                            log::warn!(
                                "failed to gracefully close connection to '{recipient}': {e}"
                            );
                        }
                    }
                    return Err(e);
                }
            }
        }
    }

    pub(crate) async fn disconnect(&self, peer: &PeerId) -> Result<()> {
        let mut conns = self.connections.write().await;
        if let Some(conn) = conns.remove(peer) {
            let peer_info = conn.peer_info.clone();
            let graceful = if let Err(e) = conn.close().await {
                log::warn!("failed to gracefully close connection to '{peer}': {e}");
                false
            } else {
                log::info!("disconnected from '{peer}'");
                true
            };
            let _ = self.events.send(Event::down(graceful, peer_info)).await;
        }
        Ok(())
    }

    async fn handle_conn(
        mut source: C::Source,
        peer_info: Arc<PeerInfo>,
        mailbox: Sender<Event>,
        connections: Weak<RwLock<HashMap<Arc<str>, Arc<ActivePeer<C::Sink>>>>>,
    ) {
        let mut graceful = true;
        while let Some(data) = source.next().await {
            match data {
                Ok(data) => {
                    let _ = mailbox.send(Event::msg(data, peer_info.clone())).await;
                }
                Err(e) => {
                    graceful = false;
                    let peer_id = &peer_info.id;
                    log::warn!("failed to receive message from '{peer_id}': {e}");
                    if let Some(connections) = connections.upgrade() {
                        let mut connections = connections.write().await;
                        if let Some(c) = connections.remove(peer_id) {
                            drop(connections);
                            if let Err(e) = c.close().await {
                                log::warn!("failed to close connection to '{peer_id}': {e}");
                            }
                            break;
                        }
                    }
                }
            }
        }
        let _ = mailbox.send(Event::down(graceful, peer_info)).await;
    }
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct Options {
    pub myself: Arc<PeerInfo>,
}

impl Default for Options {
    fn default() -> Self {
        let myself = thread_rng()
            .sample_iter(&Alphanumeric)
            .take(8)
            .map(char::from)
            .collect::<String>()
            .into();
        Options {
            myself: Arc::new(PeerInfo::new(myself)),
        }
    }
}

#[derive(Debug)]
pub struct ActivePeer<S> {
    peer_info: Arc<PeerInfo>,
    sender: Mutex<S>,
}

impl<S> ActivePeer<S>
where
    S: Sink<Bytes, Error = Error> + Unpin,
{
    fn new(peer_info: Arc<PeerInfo>, sender: S) -> Arc<Self> {
        Arc::new(ActivePeer {
            sender: Mutex::new(sender),
            peer_info,
        })
    }

    pub async fn send(&self, bytes: Bytes) -> Result<()> {
        let mut s = self.sender.lock().await;
        s.send(bytes).await
    }

    pub(crate) async fn close(&self) -> Result<()> {
        let mut s = self.sender.lock().await;
        s.close().await
    }
}

impl<S> std::fmt::Display for ActivePeer<S> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "ActivePeer({})", self.peer_info.id)
    }
}

#[derive(Debug, Clone, Eq, PartialEq, Hash, Serialize, Deserialize)]
pub struct PeerInfo {
    pub id: PeerId,
    pub manifest: Vec<u8>,
}

impl PeerInfo {
    pub fn new(id: PeerId) -> Self {
        PeerInfo {
            id,
            manifest: Vec::default(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Event {
    pub sender: Arc<PeerInfo>,
    pub event: EventData,
}

impl Event {
    pub(crate) fn up(sender: Arc<PeerInfo>, initiator: bool) -> Self {
        Event {
            sender,
            event: EventData::Connected(initiator),
        }
    }
    pub(crate) fn down(graceful: bool, sender: Arc<PeerInfo>) -> Self {
        Event {
            sender,
            event: EventData::Disconnected(graceful),
        }
    }
    pub(crate) fn msg(msg: Bytes, sender: Arc<PeerInfo>) -> Self {
        Event {
            sender,
            event: EventData::Message(msg),
        }
    }
}

impl std::fmt::Display for Event {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match &self.event {
            EventData::Message(msg) => write!(f, "MSG({}, {:?})", self.sender.id, msg),
            EventData::Connected(initiator) => write!(
                f,
                "UP({}, {})",
                self.sender.id,
                if *initiator { "initiator" } else { "acceptor" }
            ),
            EventData::Disconnected(alive) => write!(
                f,
                "DOWN({}, {})",
                self.sender.id,
                if *alive { "alive" } else { "dead" }
            ),
        }
    }
}

#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub enum EventData {
    Message(Bytes),
    Connected(bool),
    Disconnected(bool),
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub enum Message {
    /* membership messages */
    Join,
    Neighbor(Arc<PeerInfo>, bool),
    FwdJoin(Arc<PeerInfo>, TTL),
    ShuffleReq(PeerId, TTL, Vec<Arc<PeerInfo>>),
    ShuffleRep(Vec<Arc<PeerInfo>>),
    /* gossiping messages */
}

#[async_trait::async_trait]
pub trait Connector: Send + Sync {
    type Sink: Sink<Bytes, Error = Error> + Send + Sync + Unpin;
    type Source: Stream<Item = Result<Bytes>> + Send + Sync + Unpin;

    async fn accept(&self) -> Result<(Arc<PeerInfo>, Self::Sink, Self::Source)>;
    async fn connect(
        &self,
        remote_endpoint: &str,
    ) -> Result<(Arc<PeerInfo>, Self::Sink, Self::Source)>;
}

#[cfg(test)]
mod test {
    use crate::peer::{Event, Options, Peer, PeerInfo};
    use crate::tcp::Tcp;
    use crate::Result;
    use bytes::Bytes;
    use log::LevelFilter;
    use std::sync::Arc;

    async fn peer(endpoint: &str) -> Result<Peer<Tcp>> {
        let options = Options {
            myself: Arc::new(PeerInfo::new(endpoint.into())),
        };
        let tcp = Tcp::bind(endpoint, options.myself.clone()).await?;
        Ok(Peer::new(tcp, options))
    }

    #[tokio::test]
    async fn peer_connection_lifecycle() -> Result<()> {
        let mut p1 = peer("localhost:12001").await?;
        let mut e1 = p1.claim().unwrap();
        let mut p2 = peer("localhost:12002").await?;
        let mut e2 = p2.claim().unwrap();

        let a = Arc::new(PeerInfo::new("localhost:12001".into()));
        let b = Arc::new(PeerInfo::new("localhost:12002".into()));
        p1.send(&b.id, &"hello").await?;
        p2.send(&a.id, &"world").await?;

        let expected_msg = Bytes::from(serde_json::to_vec(&"world")?);
        assert_eq!(e1.recv().await, Some(Event::up(b.clone(), true)));
        assert_eq!(e1.recv().await, Some(Event::msg(expected_msg, b.clone())));

        let expected_msg = Bytes::from(serde_json::to_vec(&"hello")?);
        assert_eq!(e2.recv().await, Some(Event::up(a.clone(), false)));
        assert_eq!(e2.recv().await, Some(Event::msg(expected_msg, a)));

        drop(e2);
        drop(p2);

        assert_eq!(e1.recv().await, Some(Event::down(true, b)));

        Ok(())
    }

    #[tokio::test]
    async fn peer_disconnect() -> Result<()> {
        let _ = env_logger::builder()
            .filter_level(LevelFilter::Info)
            .is_test(true)
            .try_init();
        let mut p1 = peer("127.0.0.1:12003").await?;
        let mut e1 = p1.claim().unwrap();
        let mut p2 = peer("127.0.0.1:12004").await?;
        let mut e2 = p2.claim().unwrap();

        let a = Arc::new(PeerInfo::new("127.0.0.1:12003".into()));
        let b = Arc::new(PeerInfo::new("127.0.0.1:12004".into()));
        p1.send(&b.id, &"hello").await?;
        p2.send(&a.id, &"world").await?;

        let expected_msg = Bytes::from(serde_json::to_vec(&"world")?);
        assert_eq!(e1.recv().await, Some(Event::up(b.clone(), true)));
        assert_eq!(e1.recv().await, Some(Event::msg(expected_msg, b.clone())));

        let expected_msg = Bytes::from(serde_json::to_vec(&"hello")?);
        assert_eq!(e2.recv().await, Some(Event::up(a.clone(), false)));
        assert_eq!(e2.recv().await, Some(Event::msg(expected_msg, a.clone())));

        p1.disconnect(&b.id).await?;
        assert_eq!(e1.recv().await, Some(Event::down(true, b.clone())));
        assert_eq!(e2.recv().await, Some(Event::down(true, a.clone())));

        Ok(())
    }
}
