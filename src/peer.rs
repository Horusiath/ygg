use crate::tcp::{Tcp, TcpSink, TcpSource};
use crate::{Error, Result};
use bytes::Bytes;
use futures_util::{Sink, SinkExt, Stream, StreamExt};
use rand::distributions::Alphanumeric;
use rand::{thread_rng, Rng};
use serde::{Deserialize, Serialize};
use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::fmt::Formatter;
use std::sync::{Arc, Weak};
use tokio::sync::broadcast::{Receiver, Sender};
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
}

impl Peer<Tcp> {
    pub fn new(connector: Tcp, options: Options) -> Self {
        let connector = Arc::new(connector);
        let connections = Arc::new(RwLock::new(HashMap::new()));
        let (tx, _rx) = tokio::sync::broadcast::channel(8);

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
                                            {
                                                if let Err(e) = old.close().await {
                                                    log::warn!(
                                                    "failed to gracefully close '{peer_id}': {e}"
                                                );
                                                }
                                            }
                                            let _ = mailbox.send(Event::down(peer_id.clone()));
                                            let _ = mailbox.send(Event::up(peer_id.clone()));
                                        }
                                        Entry::Vacant(e) => {
                                            e.insert(peer.clone());
                                            let _ = mailbox.send(Event::up(peer_id.clone()));
                                        }
                                    }
                                }

                                let receiver_loop = {
                                    let mailbox = mailbox.clone();
                                    let id = peer_info.id.clone();
                                    let connections = Arc::downgrade(&connections);
                                    tokio::spawn(Self::handle_conn(
                                        source,
                                        id,
                                        mailbox,
                                        connections,
                                    ))
                                };
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
        }
    }

    pub fn options(&self) -> &Options {
        &self.options
    }

    pub fn events(&self) -> Receiver<Event> {
        self.events.subscribe()
    }

    pub(crate) async fn send<M>(&self, recipient: &PeerId, msg: &M) -> Result<()>
    where
        M: Serialize + std::fmt::Debug,
    {
        loop {
            let result = {
                let connections = self.connections.read().await;
                if let Some(conn) = connections.get(recipient) {
                    let data = Bytes::from(serde_cbor::to_vec(msg)?);
                    conn.send(data).await.map(|_| None)
                } else {
                    log::info!("establishing new connection to '{recipient}'");
                    let (peer_info, sink, source) = self.connector.connect(recipient).await?;
                    let peer = ActivePeer::new(peer_info, sink);
                    Ok(Some((peer, source)))
                }
            };
            match result {
                Ok(None) => {
                    log::info!("successfully sent message to '{recipient}': {msg:?}");
                    return Ok(());
                }
                Ok(Some((peer, source))) => {
                    {
                        let mut connections = self.connections.write().await;
                        match connections.entry(recipient.clone()) {
                            Entry::Occupied(mut e) => {
                                log::info!("replacing existing peer connection '{recipient}'");
                                let old = e.insert(peer.clone());
                                {
                                    if let Err(e) = old.close().await {
                                        log::warn!("failed to gracefully close '{recipient}': {e}");
                                    }
                                }
                                let _ = self.events.send(Event::down(recipient.clone()));
                            }
                            Entry::Vacant(e) => {
                                e.insert(peer.clone());
                            }
                        }
                    }
                    let _ = self.events.send(Event::up(recipient.clone()));
                    let receiver_loop = tokio::spawn(Self::handle_conn(
                        source,
                        recipient.clone(),
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
            if let Err(e) = conn.close().await {
                log::warn!("failed to gracefully close connection to '{peer}': {e}");
            } else {
                log::info!("disconnected from '{peer}'");
            }
            let _ = self.events.send(Event::down(peer.clone()));
        }
        Ok(())
    }

    async fn handle_conn(
        mut source: TcpSource,
        peer_id: PeerId,
        mailbox: Sender<Event>,
        connections: Weak<RwLock<HashMap<Arc<str>, Arc<ActivePeer<TcpSink>>>>>,
    ) {
        while let Some(msg) = source.next().await {
            match msg {
                Ok(msg) => {
                    let _ = mailbox.send(Event::message(msg, peer_id.clone()));
                }
                Err(e) => {
                    log::warn!("failed to receive message from '{peer_id}': {e}");
                    if let Some(connections) = connections.upgrade() {
                        let mut connections = connections.write().await;
                        if let Some(c) = connections.remove(&peer_id) {
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
        let _ = mailbox.send(Event::down(peer_id));
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
    sender: PeerId,
    event: EventData,
}

impl Event {
    pub(crate) fn up(sender: PeerId) -> Self {
        Event {
            sender,
            event: EventData::Up,
        }
    }
    pub(crate) fn down(sender: PeerId) -> Self {
        Event {
            sender,
            event: EventData::Down,
        }
    }
    pub(crate) fn message(msg: Bytes, sender: PeerId) -> Self {
        Event {
            sender,
            event: EventData::Message(msg),
        }
    }
}

#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub enum EventData {
    Message(Bytes),
    Up,
    Down,
}

#[async_trait::async_trait]
pub trait Connector {
    type Sink: Sink<Bytes> + Send + Sync;
    type Source: Stream<Item = Result<Bytes>> + Send + Sync;

    async fn accept(&self) -> Result<(Arc<PeerInfo>, Self::Sink, Self::Source)>;
    async fn connect(
        &self,
        remote_endpoint: &str,
    ) -> Result<(Arc<PeerInfo>, Self::Sink, Self::Source)>;
}

#[cfg(test)]
mod test {
    use crate::peer::{Event, Options, Peer, PeerId, PeerInfo};
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
        let p1 = peer("localhost:12001").await?;
        let mut e1 = p1.events();
        let p2 = peer("localhost:12002").await?;
        let mut e2 = p2.events();

        let a = PeerId::from("localhost:12001");
        let b = PeerId::from("localhost:12002");
        p1.send(&b, &"hello").await?;
        p2.send(&a, &"world").await?;

        let expected_msg = Bytes::from(serde_cbor::to_vec(&"world")?);
        assert_eq!(e1.recv().await, Ok(Event::up(b.clone())));
        assert_eq!(e1.recv().await, Ok(Event::message(expected_msg, b.clone())));

        let expected_msg = Bytes::from(serde_cbor::to_vec(&"hello")?);
        assert_eq!(e2.recv().await, Ok(Event::up(a.clone())));
        assert_eq!(e2.recv().await, Ok(Event::message(expected_msg, a)));

        drop(e2);
        drop(p2);

        assert_eq!(e1.recv().await, Ok(Event::down(b)));

        Ok(())
    }

    #[tokio::test]
    async fn peer_disconnect() -> Result<()> {
        let _ = env_logger::builder()
            .filter_level(LevelFilter::Info)
            .is_test(true)
            .try_init();
        let p1 = peer("127.0.0.1:12003").await?;
        let mut e1 = p1.events();
        let p2 = peer("127.0.0.1:12004").await?;
        let mut e2 = p2.events();

        let a = PeerId::from("127.0.0.1:12003");
        let b = PeerId::from("127.0.0.1:12004");
        p1.send(&b, &"hello").await?;
        p2.send(&a, &"world").await?;

        let expected_msg = Bytes::from(serde_cbor::to_vec(&"world")?);
        assert_eq!(e1.recv().await, Ok(Event::up(b.clone())));
        assert_eq!(e1.recv().await, Ok(Event::message(expected_msg, b.clone())));

        let expected_msg = Bytes::from(serde_cbor::to_vec(&"hello")?);
        assert_eq!(e2.recv().await, Ok(Event::up(a.clone())));
        assert_eq!(e2.recv().await, Ok(Event::message(expected_msg, a.clone())));

        p1.disconnect(&b).await?;
        assert_eq!(e1.recv().await, Ok(Event::down(b.clone())));
        assert_eq!(e2.recv().await, Ok(Event::down(a.clone())));

        Ok(())
    }
}
