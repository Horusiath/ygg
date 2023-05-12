/*
#[async_trait]
pub trait Protocol<S, B>: membership::Protocol<S>
where
    S: Sink<Vec<u8>>,
    B: LogBuffer,
{
    fn gossip_options(&self) -> &Options;
    fn eager_peers(&self) -> &HashMap<Pid, EagerPeer<S>>;
    fn eager_peers_mut(&mut self) -> &mut HashMap<Pid, EagerPeer<S>>;
    fn lazy_peers(&self) -> &HashMap<Pid, LazyPeer<S>>;
    fn lazy_peers_mut(&mut self) -> &mut HashMap<Pid, LazyPeer<S>>;
    fn log_buffer(&self) -> &Arc<RwLock<B>>;

    async fn emit(&mut self, e: PeerEvent) -> Result<(), Error> {
        match e {
            PeerEvent::Up(info) => {
                let peer = self.active_view().get(&info.pid).unwrap();
                let mut eager_peers = self.eager_peers_mut();
                eager_peers.insert(info.pid.clone(), peer.clone().into());
            }
            PeerEvent::Down(info) => {
                let mut eager_peers = self.eager_peers_mut();
                eager_peers.remove(&info.pid);
                let mut lazy_peers = self.lazy_peers_mut();
                lazy_peers.remove(&info.pid);
            }
        }
        Ok(())
    }

    fn on_gossip(&mut self, gossip: &[u8]) -> Result<(), Error> {
        todo!()
    }

    fn on_prune(&mut self, sender: Pid) -> Result<(), Error> {
        self.demote(sender);
        Ok(())
    }

    fn on_ihave(&mut self, sender: Pid, digest: Vec<u8>) -> Result<(), Error> {
        todo!()
    }

    fn on_graft(&mut self, sender: Pid, digest: Vec<u8>) -> Result<(), Error> {
        todo!()
    }

    fn promote(&mut self, sender: Pid) {
        if let Some(lazy) = self.lazy_peers_mut().remove(&sender) {
            self.eager_peers_mut().insert(sender, lazy.into());
        }
    }

    fn demote(&mut self, sender: Pid) {
        self.eager_peers_mut().remove(&sender);
        if let Some(peer) = self.active_view().get(&sender) {
            let mut lazy_peers = self.lazy_peers_mut();
            let log_buffer = Arc::downgrade(self.log_buffer());
            let peer: ActivePeer<S> = peer.clone();
            let lazy = LazyPeer::new(peer, log_buffer, self.gossip_options().ihave_delay);
            lazy_peers.insert(sender, lazy);
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct Options {
    /// Delay before sending IHave message to lazy peers.
    pub ihave_delay: Duration,
    /// Delay since receiving IHave message to confirm that related digest has not been received.
    pub graft_delay: Duration,
}

impl Default for Options {
    fn default() -> Self {
        Options {
            ihave_delay: Duration::from_secs(15),
            graft_delay: Duration::from_secs(15),
        }
    }
}

#[derive(Debug, Clone)]
pub struct EagerPeer<S> {
    peer: ActivePeer<S>,
}

impl<S> From<ActivePeer<S>> for EagerPeer<S> {
    #[inline]
    fn from(peer: ActivePeer<S>) -> Self {
        EagerPeer { peer }
    }
}

#[derive(Debug)]
pub struct LazyPeer<S> {
    peer: ActivePeer<S>,
    ihave_interval: JoinHandle<Result<(), Error>>,
}

impl<S> LazyPeer<S>
where
    S: Sink<Vec<u8>> + Unpin,
{
    fn new<B>(peer: ActivePeer<S>, log_buffer: Weak<RwLock<B>>, interval: Duration) -> Self
    where
        B: LogBuffer,
    {
        let peer_id = peer.info.pid.clone();
        let sender = Arc::downgrade(&peer.connection);
        let ihave_interval = tokio::spawn(async move {
            while let Some(sender) = sender.upgrade() {
                if let Some(log_buffer) = log_buffer.upgrade() {
                    let digest = { log_buffer.read().await.digest() };
                    let mut buf = Vec::with_capacity(peer_id.len() + digest.len() + 5);
                    Message::ihave(peer_id.clone(), digest).encode(&mut buf);
                    let mut sender = sender.lock().await;
                    sender.send(buf).await?;
                } else {
                    break;
                }
                sleep(interval).await;
            }
            Ok(())
        });
        LazyPeer {
            peer,
            ihave_interval,
        }
    }
}

impl<S> Into<EagerPeer<S>> for LazyPeer<S> {
    fn into(self) -> EagerPeer<S> {
        EagerPeer { peer: self.peer }
    }
}

pub trait LogBuffer {
    fn digest(&self) -> Vec<u8>;
}
*/
