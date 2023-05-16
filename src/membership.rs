use crate::peer::{Connector, Event, EventData, Peer, PeerId, PeerInfo};
use crate::utils::RngExt;
use crate::view::View;
use crate::Result;
use rand::{random, thread_rng, RngCore, SeedableRng};
use rand_chacha::{ChaCha20Rng, ChaCha8Rng};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex;
use tokio::task::JoinHandle;
use tokio::time::interval;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Options {
    /// Maximum number of active peers (peers we have an ongoing connection to).
    pub active_view_capacity: u32,
    /// Maximum number of passive peers (backup peers we know how to connect to, but not having
    /// an ongoing connections to).
    pub passive_view_capacity: u32,
    pub active_random_walk_len: TTL,
    pub passive_random_walk_len: TTL,
    pub shuffle_active_view_size: u32,
    pub shuffle_passive_view_size: u32,
    pub shuffle_random_walk_len: TTL,
    pub events_buffer_capacity: u32,
    pub shuffle_interval: Duration,
}

impl Default for Options {
    fn default() -> Self {
        Options {
            active_view_capacity: 4,
            passive_view_capacity: 24,
            active_random_walk_len: 5,
            passive_random_walk_len: 2,
            shuffle_active_view_size: 2,
            shuffle_passive_view_size: 2,
            shuffle_random_walk_len: 2,
            events_buffer_capacity: 1,
            shuffle_interval: Duration::from_secs(60),
        }
    }
}

pub struct Membership<C>
where
    C: Connector,
{
    peer: Arc<Peer<C>>,
    state: Arc<Mutex<MembershipState>>,
    msg_handler: JoinHandle<()>,
    shuffle: JoinHandle<()>,
}

impl<C> Membership<C>
where
    C: Connector + 'static,
{
    pub fn new<R>(peer: Arc<Peer<C>>) -> Self
    where
        R: RngCore + Default,
    {
        Self::with_options::<R>(peer, Options::default())
    }

    pub fn with_options<R>(peer: Arc<Peer<C>>, options: Options) -> Self
    where
        R: RngCore + Default,
    {
        let active_view = View::new(options.active_view_capacity as usize);
        let passive_view = View::new(options.passive_view_capacity as usize);
        let state = Arc::new(Mutex::new(MembershipState {
            active_view,
            passive_view,
        }));
        let mut rng = ChaCha20Rng::from_rng(&mut R::default()).unwrap();
        let msg_handler = {
            let mut events = peer.events();
            let mut rng = rng.clone();
            let state = Arc::downgrade(&state);
            let options = options.clone();
            let peer = peer.clone();
            tokio::spawn(async move {
                while let Ok(e) = events.recv().await {
                    if let Some(state) = state.upgrade() {
                        let mut state = state.lock().await;
                        let mut ctx = MessageExecutionContext::new(&mut state, &peer, &options);
                        if let Err(cause) = ctx.handle(e, &mut rng).await {
                            let id = &peer.options().myself.id;
                            log::error!("'{}' failed to handle incoming event: {}", id, cause);
                            break;
                        }
                    }
                }
            })
        };
        let shuffle = {
            let state = Arc::downgrade(&state);
            let peer = peer.clone();
            tokio::spawn(async move {
                let mut interval = interval(options.shuffle_interval);
                loop {
                    let _ = interval.tick().await;
                    if let Some(state) = state.upgrade() {
                        let mut state = state.lock().await;
                        let mut ctx = MessageExecutionContext::new(&mut state, &peer, &options);
                        if let Err(cause) = ctx.shuffle(&mut rng).await {
                            let id = &peer.options().myself.id;
                            log::error!("'{}' failed to send shuffle request: {}", id, cause);
                            break;
                        }
                    } else {
                        break;
                    }
                }
            })
        };
        let m = Membership {
            peer,
            state,
            msg_handler,
            shuffle,
        };
        m
    }

    pub fn myself(&self) -> &PeerInfo {
        &self.peer.options().myself
    }
}

struct MembershipState {
    active_view: View<Arc<PeerInfo>>,
    passive_view: View<Arc<PeerInfo>>,
}

struct MessageExecutionContext<'a, C>
where
    C: Connector,
{
    state: &'a mut MembershipState,
    peer: &'a Arc<Peer<C>>,
    options: &'a Options,
}

impl<'a, C> MessageExecutionContext<'a, C>
where
    C: Connector + 'static,
{
    fn new(state: &'a mut MembershipState, peer: &'a Arc<Peer<C>>, options: &'a Options) -> Self {
        MessageExecutionContext {
            state,
            peer,
            options,
        }
    }

    fn myself(&self) -> &PeerInfo {
        &self.peer.options().myself
    }

    pub async fn handle<R: RngCore>(&mut self, e: Event, rng: &mut R) -> Result<()> {
        match e.event {
            EventData::Message(bytes) => {
                let msg: Message = serde_cbor::from_slice(&bytes)?;
                match msg {
                    Message::Join => self.on_join(e.sender, rng).await,
                    Message::FwdJoin(peer, ttl) => {
                        let sender = e.sender.id.clone();
                        self.on_forward_join(peer, sender, ttl, rng).await
                    }
                    Message::Neighbor(neighbor, high_priority) => {
                        self.on_neighbor(neighbor, high_priority, rng).await
                    }
                    Message::ShuffleReq(origin, ttl, peers) => {
                        let sender = e.sender.id.clone();
                        self.on_shuffle_request(origin, sender, ttl, peers, rng)
                            .await
                    }
                    Message::ShuffleRep(peers) => Ok(self.on_shuffle_response(&peers, rng)),
                }
            }
            EventData::Up => Ok(()),
            EventData::Down(alive) => self.on_disconnected(e.sender, alive, rng).await,
        }
    }

    async fn add_active<R: RngCore>(
        &mut self,
        info: Arc<PeerInfo>,
        high_priority: bool,
        rng: &mut R,
    ) -> Result<bool> {
        if info.id == self.myself().id || self.state.active_view.contains(&info.id) {
            return Ok(false);
        }

        let msg = Message::Neighbor(info.clone(), high_priority);
        self.peer.send(&info.id, &msg).await?;

        self.state.passive_view.remove(&info.id);
        let removed = self
            .state
            .active_view
            .insert_replace(info.id.clone(), info, rng);
        if let Some((pid, peer)) = removed {
            if let Err(cause) = self.peer.disconnect(&pid).await {
                log::warn!("couldn't close peer {pid}: {cause}");
            }
        }
        Ok(true)
    }

    async fn on_join<R: RngCore>(&mut self, info: Arc<PeerInfo>, rng: &mut R) -> Result<()> {
        self.add_active(info.clone(), true, rng).await?;
        let ttl = self.options.active_random_walk_len;
        let fwd = Message::FwdJoin(info.clone(), ttl);

        let mut fails = Vec::new();
        {
            for info in self.state.active_view.values_mut() {
                if info.id != info.id {
                    if let Err(cause) = self.peer.send(&info.id, &fwd).await {
                        log::warn!("couldn't send forward join to {}: {}", info.id, cause);
                        fails.push(info.id.clone());
                    }
                }
            }
        }

        for pid in fails {
            if let Some(info) = self.state.active_view.remove(&pid) {
                if let Err(cause) = self.peer.disconnect(&info.id).await {
                    log::warn!("couldn't disconnect peer {}: {}", info.id, cause);
                }
            }
        }

        Ok(())
    }

    async fn on_disconnected<R: RngCore>(
        &mut self,
        info: Arc<PeerInfo>,
        graceful: bool,
        rng: &mut R,
    ) -> Result<()> {
        let mut promoted = if graceful {
            // if shutdown was graceful, we demote peer into passive view for future use
            let id = info.id.clone();
            if let Some(promoted) = self.state.passive_view.insert_replace(id, info, rng) {
                Some(promoted)
            } else {
                self.state.passive_view.remove_at(rng)
            }
        } else {
            self.state.passive_view.remove_at(rng)
        };

        // promote passive peer into active one
        while let Some((id, peer)) = promoted.take() {
            let high_priority = self.state.active_view.is_empty();
            match self.add_active(peer, high_priority, rng).await {
                Ok(true) => {
                    break;
                }
                Ok(false) => {}
                Err(cause) => log::warn!("couldn't connect to {id}: {cause}"),
            }
            promoted = self.state.passive_view.remove_at(rng);
        }
        Ok(())
    }

    async fn on_neighbor<R: RngCore>(
        &mut self,
        peer: Arc<PeerInfo>,
        high_priority: bool,
        rng: &mut R,
    ) -> Result<()> {
        if high_priority || !self.state.active_view.is_full() {
            self.add_active(peer, high_priority, rng).await?;
        }
        Ok(())
    }

    async fn on_forward_join<R: RngCore>(
        &mut self,
        peer: Arc<PeerInfo>,
        sender: PeerId,
        ttl: u32,
        rng: &mut R,
    ) -> Result<()> {
        if ttl == 0 || self.state.active_view.is_empty() {
            self.add_active(peer, true, rng).await?;
        } else {
            if ttl == self.options.passive_random_walk_len {
                if !self.already_known(&peer.id) {
                    let peer = peer.clone();
                    self.state
                        .passive_view
                        .insert_replace(peer.id.clone(), peer.into(), rng);
                }
            }
            if let Some(info) = self
                .state
                .active_view
                .peek_value_mut(random(), |v| v.id != sender)
            {
                let msg = Message::FwdJoin(peer, ttl - 1);
                self.peer.send(&info.id, &msg).await?;
            }
        }
        Ok(())
    }

    async fn shuffle<R: RngCore>(&mut self, rng: &mut R) -> Result<()> {
        let recipient = self.state.active_view.peek_key(rng).cloned();
        if let Some(recipient) = recipient {
            let alen = (self.options.shuffle_active_view_size as usize)
                .min(self.state.active_view.len() - 1);
            let plen = (self.options.shuffle_passive_view_size as usize)
                .min(self.state.passive_view.len() - 1);
            let mut nodes = Vec::with_capacity(alen + plen);
            {
                let mut peers: Vec<_> = self
                    .state
                    .active_view
                    .iter()
                    .map(|(_, v)| v.clone())
                    .collect();
                if let Some(i) = peers.iter().position(|peer| peer.id == recipient) {
                    peers.remove(i);
                }
                thread_rng().shuffle(&mut peers);
                for pid in &peers[0..alen] {
                    nodes.push(pid.clone());
                }
            }
            {
                let mut peers: Vec<_> = self
                    .state
                    .passive_view
                    .iter()
                    .map(|(_, v)| v.clone())
                    .collect();
                thread_rng().shuffle(&mut peers);
                for pid in &peers[0..plen] {
                    nodes.push(pid.clone());
                }
            }
            let myself = self.myself().id.clone();
            let msg = Message::ShuffleReq(myself, self.options.shuffle_random_walk_len, nodes);
            if let Some(info) = self.state.active_view.get_mut(&recipient) {
                self.peer.send(&info.id, &msg).await?;
            }
        }
        Ok(())
    }

    async fn on_shuffle_request<R: RngCore>(
        &mut self,
        origin: PeerId,
        sender: PeerId,
        ttl: u32,
        nodes: Vec<Arc<PeerInfo>>,
        rng: &mut R,
    ) -> Result<()> {
        if ttl == 0 {
            let exchange_len = self.state.passive_view.len().min(nodes.len());
            let mut exchange_nodes = Vec::with_capacity(exchange_len);
            for node in nodes {
                if !self.already_known(&node.id) {
                    if let Some((_, info)) =
                        self.state
                            .passive_view
                            .insert_replace(node.id.clone(), node.into(), rng)
                    {
                        exchange_nodes.push(info);
                    }
                }
            }
            if !exchange_nodes.is_empty() {
                if let Some(origin) = self.state.active_view.get_mut(&origin) {
                    self.peer
                        .send(&origin.id, &Message::ShuffleRep(exchange_nodes))
                        .await?;
                }
            }
        } else {
            let peer = self
                .state
                .active_view
                .peek_value_mut(random(), |v| v.id != origin && v.id != sender);
            if let Some(info) = peer {
                let msg = Message::ShuffleReq(origin, ttl - 1, nodes);
                self.peer.send(&info.id, &msg).await?;
            }
        }
        Ok(())
    }

    fn on_shuffle_response<R: RngCore>(&mut self, nodes: &[Arc<PeerInfo>], rng: &mut R) {
        for node in nodes {
            if !self.already_known(&node.id) {
                self.state
                    .passive_view
                    .insert_replace(node.id.clone(), node.clone().into(), rng);
            }
        }
    }

    fn already_known(&self, pid: &PeerId) -> bool {
        &self.myself().id == pid
            || self.state.active_view.contains(pid)
            || self.state.passive_view.contains(pid)
    }
}

pub type TTL = u32;

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

#[cfg(test)]
mod test {
    use crate::peer::{Options, Peer, PeerInfo};
    use crate::tcp::Tcp;
    use crate::Result;
    use std::sync::Arc;

    async fn peer(endpoint: &str) -> Result<Peer<Tcp>> {
        let options = Options {
            myself: Arc::new(PeerInfo::new(endpoint.into())),
        };
        let tcp = Tcp::bind(endpoint, options.myself.clone()).await?;
        Ok(Peer::new(tcp, options))
    }
}
