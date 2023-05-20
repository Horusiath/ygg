use crate::peer::{Connector, Event, EventData, Message, Peer, PeerId, PeerInfo};
use crate::utils::RngExt;
use crate::view::View;
use crate::{Result, TTL};
use rand::{RngCore, SeedableRng};
use rand_chacha::ChaCha20Rng;
use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Weak};
use std::time::Duration;
use tokio::sync::{Mutex, Notify, RwLock};
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
    pub shuffle_interval: Option<Duration>,
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
            shuffle_interval: Some(Duration::from_secs(60)),
        }
    }
}

pub struct Membership<C>
where
    C: Connector,
{
    peer: Arc<Peer<C>>,
    state: Arc<RwLock<MembershipState>>,
    msg_handler: JoinHandle<()>,
    shuffle: Option<JoinHandle<()>>,
    membership_events: tokio::sync::broadcast::Sender<MembershipEvent>,
}

impl<C> Membership<C>
where
    C: Connector + 'static,
{
    pub fn new<R>(peer: Peer<C>) -> Self
    where
        R: RngCore + Default,
    {
        Self::with_options::<R>(peer, Options::default())
    }

    pub fn with_options<R>(mut peer: Peer<C>, options: Options) -> Self
    where
        R: RngCore + Default,
    {
        let mut events = peer.claim().unwrap();
        let peer = Arc::new(peer);
        let state = Arc::new(RwLock::new(MembershipState {
            active_view: View::new(options.active_view_capacity as usize),
            passive_view: View::new(options.passive_view_capacity as usize),
            joining: HashMap::new(),
        }));
        let (membership_events, _) =
            tokio::sync::broadcast::channel(options.active_view_capacity as usize);
        let mut rng = ChaCha20Rng::from_rng(&mut R::default()).unwrap();
        let msg_handler = {
            let mut rng = rng.clone();
            let state = Arc::downgrade(&state);
            let options = options.clone();
            let peer = peer.clone();
            let membership_events = membership_events.clone();
            tokio::spawn(async move {
                while let Some(e) = events.recv().await {
                    if let Some(state) = state.upgrade() {
                        let mut state = state.write().await;
                        log::trace!("'{}' received event: {}", peer.id(), e);
                        let mut ctx = MessageExecutionContext::new(
                            &mut state,
                            &peer,
                            &options,
                            &membership_events,
                        );
                        if let Err(cause) = ctx.handle(e, &mut rng).await {
                            let id = peer.id();
                            log::warn!("'{}' failed to handle incoming event: {}", id, cause);
                            break;
                        }
                    }
                }
            })
        };
        let shuffle = if let Some(interval) = &options.shuffle_interval {
            let state = Arc::downgrade(&state);
            let peer = peer.clone();
            Some(tokio::spawn(Self::shuffle_task(
                state, *interval, peer, options, rng,
            )))
        } else {
            None
        };
        let m = Membership {
            peer,
            state,
            msg_handler,
            shuffle,
            membership_events,
        };
        m
    }

    async fn shuffle_task(
        state: Weak<RwLock<MembershipState>>,
        shuffle_interval: Duration,
        peer: Arc<Peer<C>>,
        options: Options,
        mut rng: ChaCha20Rng,
    ) {
        let mut interval = interval(shuffle_interval);
        loop {
            let _ = interval.tick().await;
            if let Some(state) = state.upgrade() {
                let res = {
                    let mut state = state.read().await;
                    Self::shuffle(&mut state, &peer, &options, &mut rng)
                };
                if let Some((id, msg)) = res {
                    if let Err(cause) = peer.send(&id, &msg).await {
                        let id = peer.id();
                        log::error!("'{}' failed to send shuffle request: {}", id, cause);
                    }
                }
            } else {
                break;
            }
        }
    }

    fn shuffle(
        state: &MembershipState,
        peer: &Peer<C>,
        options: &Options,
        rng: &mut ChaCha20Rng,
    ) -> Option<(PeerId, Message)> {
        let recipient = state.active_view.peek_key(rng).cloned();
        if let Some(recipient) = recipient {
            let alen = (options.shuffle_active_view_size as usize).min(state.active_view.len() - 1);
            let plen =
                (options.shuffle_passive_view_size as usize).min(state.passive_view.len() - 1);
            let mut nodes = Vec::with_capacity(alen + plen);
            {
                let mut peers: Vec<_> = state.active_view.iter().map(|(_, v)| v.clone()).collect();
                if let Some(i) = peers.iter().position(|peer| peer.id == recipient) {
                    peers.remove(i);
                }
                rng.shuffle(&mut peers);
                for pid in &peers[0..alen] {
                    nodes.push(pid.clone());
                }
            }
            {
                let mut peers: Vec<_> = state.passive_view.iter().map(|(_, v)| v.clone()).collect();
                rng.shuffle(&mut peers);
                for pid in &peers[0..plen] {
                    nodes.push(pid.clone());
                }
            }
            let myself = peer.id().clone();
            let msg = Message::ShuffleReq(myself, options.shuffle_random_walk_len, nodes);
            Some((recipient, msg))
        } else {
            None
        }
    }

    pub async fn join<A>(&self, peer_id: A) -> Result<()>
    where
        A: Into<PeerId>,
    {
        let contact = peer_id.into();
        let notify = {
            let notify = Arc::new(Notify::new());
            let mut state = self.state.write().await;
            state.joining.insert(contact.clone(), notify.clone());
            notify
        };
        self.peer.send(&contact, &Message::Join).await?;
        notify.notified().await;
        Ok(())
    }

    pub fn myself(&self) -> &PeerInfo {
        &self.peer.options().myself
    }

    pub fn peer(&self) -> &Peer<C> {
        &self.peer
    }

    pub fn membership_events(&self) -> tokio::sync::broadcast::Receiver<MembershipEvent> {
        self.membership_events.subscribe()
    }

    pub async fn state_snapshot(&self) -> MembershipSnapshot {
        let state = self.state.read().await;
        MembershipSnapshot::new(&state)
    }
}

#[derive(Debug)]
struct MembershipState {
    active_view: View<Arc<PeerInfo>>,
    passive_view: View<Arc<PeerInfo>>,
    joining: HashMap<PeerId, Arc<Notify>>,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct MembershipSnapshot {
    pub active_view: HashSet<Arc<PeerInfo>>,
    pub passive_view: HashSet<Arc<PeerInfo>>,
}

impl MembershipSnapshot {
    fn new(state: &MembershipState) -> Self {
        MembershipSnapshot {
            active_view: state.active_view.iter().map(|(_, i)| i.clone()).collect(),
            passive_view: state.passive_view.iter().map(|(_, i)| i.clone()).collect(),
        }
    }
}

struct MessageExecutionContext<'a, C>
where
    C: Connector,
{
    state: &'a mut MembershipState,
    peer: &'a Arc<Peer<C>>,
    options: &'a Options,
    membership_events: &'a tokio::sync::broadcast::Sender<MembershipEvent>,
}

impl<'a, C> MessageExecutionContext<'a, C>
where
    C: Connector + 'static,
{
    fn new(
        state: &'a mut MembershipState,
        peer: &'a Arc<Peer<C>>,
        options: &'a Options,
        membership_events: &'a tokio::sync::broadcast::Sender<MembershipEvent>,
    ) -> Self {
        MessageExecutionContext {
            state,
            peer,
            options,
            membership_events,
        }
    }

    fn id(&self) -> &PeerId {
        &self.peer.options().myself.id
    }

    fn myself(&self) -> &PeerInfo {
        &self.peer.options().myself
    }

    pub async fn handle<R: RngCore>(&mut self, e: Event, rng: &mut R) -> Result<()> {
        match e.event {
            EventData::Message(data) => {
                let msg = serde_json::from_slice(&data)?;
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
            EventData::Connected(false) => Ok(()),
            EventData::Connected(true) => {
                Ok(()) // todo: what now?
            }
            EventData::Disconnected(alive) => self.on_disconnected(e.sender, alive, rng).await,
        }
    }

    async fn add_active<R: RngCore>(
        &mut self,
        info: Arc<PeerInfo>,
        high_priority: bool,
        rng: &mut R,
    ) -> Result<bool> {
        if &info.id == self.id() || self.state.active_view.contains(&info.id) {
            return Ok(false);
        }

        let msg = Message::Neighbor(self.peer.options().myself.clone(), high_priority);
        self.peer.send(&info.id, &msg).await?;

        log::trace!("'{}' adding '{}' as active peer", self.id(), &info.id);
        self.state.passive_view.remove(&info.id);
        let pid = info.id.clone();
        let removed = self
            .state
            .active_view
            .insert_replace(pid.clone(), info, rng);
        let _ = self.membership_events.send(MembershipEvent::Up(pid));
        if let Some((pid, peer)) = removed {
            let _ = self
                .membership_events
                .send(MembershipEvent::Down(pid.clone()));
            if let Err(cause) = self.peer.disconnect(&pid).await {
                log::warn!("couldn't close peer {pid}: {cause}");
            }
        }
        Ok(true)
    }

    async fn on_join<R: RngCore>(&mut self, info: Arc<PeerInfo>, rng: &mut R) -> Result<()> {
        self.add_active(info.clone(), true, rng).await?;
        let ttl = self.options.active_random_walk_len;

        let mut fails = Vec::new();
        {
            for ap in self.state.active_view.values() {
                if ap.id != info.id {
                    let fwd = Message::FwdJoin(ap.clone(), ttl);
                    if let Err(cause) = self.peer.send(&info.id, &fwd).await {
                        log::warn!("couldn't send forward join to {}: {}", ap.id, cause);
                        fails.push(ap.id.clone());
                    }
                }
            }
        }

        for pid in fails {
            if let Some(info) = self.state.active_view.remove(&pid) {
                let _ = self.membership_events.send(MembershipEvent::Down(pid));
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
                self.state.passive_view.remove_one(rng)
            }
        } else {
            self.state.passive_view.remove_one(rng)
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
            promoted = self.state.passive_view.remove_one(rng);
        }
        Ok(())
    }

    async fn on_neighbor<R: RngCore>(
        &mut self,
        peer: Arc<PeerInfo>,
        high_priority: bool,
        rng: &mut R,
    ) -> Result<()> {
        let pid = if high_priority || !self.state.active_view.is_full() {
            let pid = peer.id.clone();
            self.add_active(peer, high_priority, rng).await?;
            pid
        } else {
            // active peer set is full
            self.peer.disconnect(&peer.id).await?;
            peer.id.clone()
        };
        if let Some(joined) = self.state.joining.remove(&pid) {
            joined.notify_waiters();
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
            Ok(())
        } else {
            if ttl == self.options.passive_random_walk_len {
                if !self.already_known(&peer.id) {
                    let peer = peer.clone();
                    self.state
                        .passive_view
                        .insert_replace(peer.id.clone(), peer.into(), rng);
                }
            }
            let id = self.id().clone();
            let next = self.state.active_view.peek_value(rng, [&sender, &peer.id]);
            match next {
                None => {
                    self.add_active(peer, true, rng).await?;
                    Ok(())
                }
                Some(info) => {
                    let msg = Message::FwdJoin(peer, ttl - 1);
                    self.peer.send(&info.id, &msg).await
                }
            }
        }
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
            let peer = self.state.active_view.peek_value(rng, [&origin, &sender]);
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
        self.id() == pid
            || self.state.active_view.contains(pid)
            || self.state.passive_view.contains(pid)
    }
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub enum MembershipEvent {
    Up(PeerId),
    Down(PeerId),
}

#[cfg(test)]
mod test {
    use crate::membership::Membership;
    use crate::peer::{Connector, Options, Peer, PeerId, PeerInfo};
    use crate::tcp::Tcp;
    use crate::Result;
    use futures_util::future::try_join_all;
    use log::LevelFilter;
    use rand::prelude::ThreadRng;
    use std::collections::{HashMap, HashSet};
    use std::sync::Arc;
    use std::time::Duration;
    use tokio::time::sleep;

    async fn peer(endpoint: &str) -> Result<Peer<Tcp>> {
        let options = Options {
            myself: Arc::new(PeerInfo::new(endpoint.into())),
        };
        let tcp = Tcp::bind(endpoint, options.myself.clone()).await?;
        Ok(Peer::new(tcp, options))
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 10)]
    async fn active_view_is_symetric() -> Result<()> {
        let _ = env_logger::builder()
            .filter_level(LevelFilter::Error)
            .is_test(true)
            .try_init();

        const PEER_COUNT: usize = 6;
        let mut members = Vec::with_capacity(PEER_COUNT);
        let mut events = Vec::with_capacity(PEER_COUNT);
        for i in 0..PEER_COUNT {
            let peer = peer(&format!("127.0.0.1:{}", 13000 + i)).await?;
            let m = Membership::new::<ThreadRng>(peer);
            events.push(m.membership_events());
            members.push(m);
        }
        let mut fut = Vec::with_capacity(PEER_COUNT - 1);
        for i in 1..PEER_COUNT {
            let m = &members[i];
            fut.push(m.join("127.0.0.1:13000"));
        }
        try_join_all(fut).await?;
        sleep(Duration::from_millis(1500)).await;

        assert_symmetric_peers(&members).await;

        Ok(())
    }

    /// Assert that for every active peer of X->{Y0..Yn}, X also exists in an active set of {Y0..Yn}.
    async fn assert_symmetric_peers<'a, I, C>(members: I)
    where
        I: IntoIterator<Item = &'a Membership<C>>,
        C: Connector + 'static,
    {
        let mut views = HashMap::new();
        for m in members {
            let id = m.peer().id().clone();
            let state = m.state_snapshot().await;
            let active = state
                .active_view
                .into_iter()
                .map(|i| i.id.clone())
                .collect::<HashSet<PeerId>>();
            views.insert(id, active);
        }
        for (x, active_view) in views.iter() {
            assert!(!active_view.is_empty(), "{} has not active peers", x);
            for y in active_view.iter() {
                let peers = &views[y];
                assert!(
                    peers.contains(x),
                    "missing symmetric connection between:\n\t\"{}\" -> {:?}\nand\n\t\"{}\" -> {:?}",
                    x,
                    active_view,
                    y,
                    peers
                );
            }
        }
    }
}
