use crate::peer::{Connector, Peer, PeerId, PeerInfo};
use crate::view::View;
use crate::Result;
use futures_util::SinkExt;
use rand::{random, thread_rng};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::time::Duration;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Options {
    /// Maximum number of active peers (peers we have an ongoing connection to).
    pub active_view_capacity: u32,
    /// Maximum number of passive peers (backup peers we know how to connect to, but not having
    /// an ongoing connections to).
    pub passive_view_capacity: u32,
    pub active_random_walk_len: u32,
    pub passive_random_walk_len: u32,
    pub shuffle_active_view_size: u32,
    pub shuffle_passive_view_size: u32,
    pub shuffle_random_walk_len: u32,
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
    options: Options,
    active_view: View<PeerId>,
    passive_view: View<PeerId>,
}

impl<C: Connector> Membership<C> {
    pub fn new(peer: Arc<Peer<C>>) -> Self {
        Self::with_options(peer, Options::default())
    }

    pub fn with_options(peer: Arc<Peer<C>>, options: Options) -> Self {
        let active_view = View::new(options.active_view_capacity as usize);
        let passive_view = View::new(options.passive_view_capacity as usize);
        Membership {
            peer,
            options,
            active_view,
            passive_view,
        }
    }

    pub fn myself(&self) -> &PeerInfo {
        &self.peer.options().myself
    }

    pub fn options(&self) -> &Options {
        &self.options
    }

    async fn add_active(&mut self, mut info: PeerInfo, high_priority: bool) -> Result<bool> {
        if info.id == self.myself().id || self.active_view().contains(&info.id) {
            return Ok(false);
        }

        id.send(&Message::neighbor(info.clone(), high_priority))
            .await?;
        self.passive_view_mut().remove(&info.id);
        let removed =
            self.active_view_mut()
                .insert_replace(info.id.clone(), info, &mut thread_rng());
        if let Some((pid, mut peer)) = removed {
            if let Err(cause) = self.disconnect(&mut peer, true).await {
                log::warn!("couldn't close peer {pid}: {cause}");
            }
            self.emit(PeerEvent::Down(peer.info)).await?;
        }
        self.emit(PeerEvent::Up(info)).await?;
        Ok(true)
    }

    async fn on_join(&mut self, peer: ActivePeer<S>) -> Result<()> {
        let info = peer.info.clone();
        self.add_active(peer, true).await?;
        let ttl = self.membership_options().active_random_walk_len;
        let fwd = Message::forward_join(info.clone(), self.myself(), ttl);

        let mut failed_peers = Vec::new();
        {
            let active_view = self.active_view_mut();
            for peer in active_view.values_mut() {
                if peer.info.id != info.id {
                    if let Err(cause) = peer.send(&fwd).await {
                        log::warn!("couldn't send forward join to {}: {}", info.id, cause);
                        failed_peers.push(peer.info.id.clone());
                    }
                }
            }
        }

        for pid in failed_peers {
            if let Some(mut peer) = self.active_view_mut().remove(&pid) {
                if let Err(cause) = self.disconnect(&mut peer, true).await {
                    log::warn!("couldn't disconnect peer {}: {}", peer.info.id, cause);
                }
            }
        }

        Ok(())
    }

    async fn on_disconnected(&mut self, peer: PeerId, graceful: bool) -> Result<()> {
        if let Some(mut peer) = self.active_view_mut().remove(&peer) {
            let _ = peer.close().await; // it doesn't matter if we confirm close, as it may be already disconnected
            if graceful {
                // if shutdown was graceful, we demote peer into passive view for future use
                let info = peer.info.id.clone();
                self.passive_view_mut()
                    .insert_replace(info, peer.into(), &mut thread_rng());
            }

            loop {
                // promote passive peer into active one
                if let Some((pid, promoted)) = self.passive_view_mut().remove_at(random()) {
                    match self.connect(promoted.into()).await {
                        Ok(peer) => {
                            let high_priority = self.active_view().is_empty();
                            self.add_active(peer, high_priority);
                            break;
                        }
                        Err(cause) => {
                            log::warn!("couldn't connect to {pid}: {cause}");
                        }
                    }
                }
            }
        }
        Ok(())
    }

    async fn on_neighbor(&mut self, peer: Arc<PeerInfo>, high_priority: bool) -> Result<()> {
        if high_priority || !self.active_view_mut().is_full() {
            let peer = self.connect(peer).await?;
            self.add_active(peer, high_priority).await?;
        }
        Ok(())
    }

    async fn on_forward_join(
        &mut self,
        peer: Arc<PeerInfo>,
        sender: PeerId,
        ttl: u32,
    ) -> Result<()> {
        if ttl == 0 || self.active_view().is_empty() {
            let peer = self.connect(peer.clone()).await?;
            self.add_active(peer, true).await?;
        } else {
            let myself = self.myself();
            if ttl == self.membership_options().passive_random_walk_len {
                if !self.already_known(&peer.id) {
                    let peer = peer.clone();
                    self.passive_view_mut().insert_replace(
                        peer.id.clone(),
                        peer.into(),
                        &mut thread_rng(),
                    );
                }
            }
            if let Some(conn) = self
                .active_view_mut()
                .peek_value_mut(random(), |v| v.info.id != sender)
            {
                let msg = Message::forward_join(peer.into(), myself, ttl - 1);
                conn.send(&msg).await?;
            }
        }
        Ok(())
    }

    async fn shuffle(&mut self) -> Result<()> {
        let recipient = self.active_view_mut().peek_key(random()).cloned();
        if let Some(recipient) = recipient {
            let o = self.membership_options();
            let alen = (o.shuffle_active_view_size as usize).min(self.active_view().len() - 1);
            let plen = (o.shuffle_passive_view_size as usize).min(self.passive_view().len() - 1);
            let mut nodes = Vec::with_capacity(alen + plen);
            {
                let mut peers: Vec<_> = self
                    .active_view()
                    .iter()
                    .map(|(_, v)| v.info.clone())
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
                    .passive_view()
                    .iter()
                    .map(|(_, v)| v.info.clone())
                    .collect();
                thread_rng().shuffle(&mut peers);
                for pid in &peers[0..plen] {
                    nodes.push(pid.clone());
                }
            }
            let myself = self.myself();
            let msg =
                Message::shuffle_request(myself.clone(), myself, o.shuffle_random_walk_len, nodes);
            if let Some(peer) = self.active_view_mut().get_mut(&recipient) {
                peer.send(&msg).await?;
            }
        }
        Ok(())
    }

    async fn on_shuffle_request(
        &mut self,
        origin: PeerId,
        sender: PeerId,
        ttl: u32,
        nodes: Vec<Arc<PeerInfo>>,
    ) -> Result<()> {
        if ttl == 0 {
            let exchange_len = self.passive_view().len().min(nodes.len());
            let mut exchange_nodes = Vec::with_capacity(exchange_len);
            for node in nodes {
                if !self.already_known(&node.id) {
                    if let Some((_, peer)) = self.passive_view_mut().insert_replace(
                        node.id.clone(),
                        node.into(),
                        &mut thread_rng(),
                    ) {
                        exchange_nodes.push(peer.info);
                    }
                }
            }
            if !exchange_nodes.is_empty() {
                if let Some(origin) = self.active_view_mut().get_mut(&origin) {
                    origin
                        .send(&Message::shuffle_response(exchange_nodes))
                        .await?;
                }
            }
        } else {
            let myself = self.myself();
            let peer = self
                .active_view_mut()
                .peek_value_mut(random(), |v| v.info.id != origin && v.info.id != sender);
            if let Some(peer) = peer {
                let msg = Message::shuffle_request(origin, myself, ttl - 1, nodes);
                peer.send(&msg).await?;
            }
        }
        Ok(())
    }

    fn on_shuffle_response(&mut self, nodes: &[Arc<PeerInfo>]) {
        let mut rng = thread_rng();
        for node in nodes {
            if !self.already_known(&node.id) {
                self.passive_view_mut().insert_replace(
                    node.id.clone(),
                    node.clone().into(),
                    &mut rng,
                );
            }
        }
    }

    fn already_known(&self, pid: &PeerId) -> bool {
        self.myself_ref().eq(pid)
            || self.active_view().contains(pid)
            || self.passive_view().contains(pid)
    }
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub enum Message {}

/*
#[async_trait]
pub trait Protocol<S>
where
    S: Sink<Bytes, Error = Error> + Send + Sync + Unpin + 'static,
{
    fn membership_options(&self) -> &Options;
    fn active_view(&self) -> &View<ActivePeer<S>>;
    fn passive_view(&self) -> &View<PassivePeer>;
    fn active_view_mut(&mut self) -> &mut View<ActivePeer<S>>;
    fn passive_view_mut(&mut self) -> &mut View<PassivePeer>;
    fn myself_ref(&self) -> &PeerId;

    fn myself(&self) -> PeerId {
        self.myself_ref().clone()
    }

    async fn emit(&mut self, e: PeerEvent) -> Result<()>;
    async fn connect(&mut self, peer: Arc<PeerInfo>) -> Result<ActivePeer<S>, Error>;

    async fn add_active(
        &mut self,
        mut peer: ActivePeer<S>,
        high_priority: bool,
    ) -> Result<bool, Error> {
        let info = peer.info.clone();
        if info.id.eq(self.myself_ref()) || self.active_view().contains(&info.id) {
            return Ok(false);
        }

        peer.send(&Message::neighbor(info.clone(), high_priority))
            .await?;
        self.passive_view_mut().remove(&info.id);
        let removed =
            self.active_view_mut()
                .insert_replace(info.id.clone(), peer, &mut thread_rng());
        if let Some((pid, mut peer)) = removed {
            if let Err(cause) = self.disconnect(&mut peer, true).await {
                log::warn!("couldn't close peer {pid}: {cause}");
            }
            self.emit(PeerEvent::Down(peer.info)).await?;
        }
        self.emit(PeerEvent::Up(info)).await?;
        Ok(true)
    }

    async fn on_join(&mut self, peer: ActivePeer<S>) -> Result<()> {
        let info = peer.info.clone();
        self.add_active(peer, true).await?;
        let ttl = self.membership_options().active_random_walk_len;
        let fwd = Message::forward_join(info.clone(), self.myself(), ttl);

        let mut failed_peers = Vec::new();
        {
            let active_view = self.active_view_mut();
            for peer in active_view.values_mut() {
                if peer.info.id != info.id {
                    if let Err(cause) = peer.send(&fwd).await {
                        log::warn!("couldn't send forward join to {}: {}", info.id, cause);
                        failed_peers.push(peer.info.id.clone());
                    }
                }
            }
        }

        for pid in failed_peers {
            if let Some(mut peer) = self.active_view_mut().remove(&pid) {
                if let Err(cause) = self.disconnect(&mut peer, true).await {
                    log::warn!("couldn't disconnect peer {}: {}", peer.info.id, cause);
                }
            }
        }

        Ok(())
    }

    async fn on_disconnected(&mut self, peer: PeerId, graceful: bool) -> Result<()> {
        if let Some(mut peer) = self.active_view_mut().remove(&peer) {
            let _ = peer.close().await; // it doesn't matter if we confirm close, as it may be already disconnected
            if graceful {
                // if shutdown was graceful, we demote peer into passive view for future use
                let info = peer.info.id.clone();
                self.passive_view_mut()
                    .insert_replace(info, peer.into(), &mut thread_rng());
            }

            loop {
                // promote passive peer into active one
                if let Some((pid, promoted)) = self.passive_view_mut().remove_at(random()) {
                    match self.connect(promoted.into()).await {
                        Ok(peer) => {
                            let high_priority = self.active_view().is_empty();
                            self.add_active(peer, high_priority);
                            break;
                        }
                        Err(cause) => {
                            log::warn!("couldn't connect to {pid}: {cause}");
                        }
                    }
                }
            }
        }
        Ok(())
    }

    async fn on_neighbor(&mut self, peer: Arc<PeerInfo>, high_priority: bool) -> Result<()> {
        if high_priority || !self.active_view_mut().is_full() {
            let peer = self.connect(peer).await?;
            self.add_active(peer, high_priority).await?;
        }
        Ok(())
    }

    async fn on_forward_join(
        &mut self,
        peer: Arc<PeerInfo>,
        sender: PeerId,
        ttl: u32,
    ) -> Result<()> {
        if ttl == 0 || self.active_view().is_empty() {
            let peer = self.connect(peer.clone()).await?;
            self.add_active(peer, true).await?;
        } else {
            let myself = self.myself();
            if ttl == self.membership_options().passive_random_walk_len {
                if !self.already_known(&peer.id) {
                    let peer = peer.clone();
                    self.passive_view_mut().insert_replace(
                        peer.id.clone(),
                        peer.into(),
                        &mut thread_rng(),
                    );
                }
            }
            if let Some(conn) = self
                .active_view_mut()
                .peek_value_mut(random(), |v| v.info.id != sender)
            {
                let msg = Message::forward_join(peer.into(), myself, ttl - 1);
                conn.send(&msg).await?;
            }
        }
        Ok(())
    }

    async fn shuffle(&mut self) -> Result<()> {
        let recipient = self.active_view_mut().peek_key(random()).cloned();
        if let Some(recipient) = recipient {
            let o = self.membership_options();
            let alen = (o.shuffle_active_view_size as usize).min(self.active_view().len() - 1);
            let plen = (o.shuffle_passive_view_size as usize).min(self.passive_view().len() - 1);
            let mut nodes = Vec::with_capacity(alen + plen);
            {
                let mut peers: Vec<_> = self
                    .active_view()
                    .iter()
                    .map(|(_, v)| v.info.clone())
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
                    .passive_view()
                    .iter()
                    .map(|(_, v)| v.info.clone())
                    .collect();
                thread_rng().shuffle(&mut peers);
                for pid in &peers[0..plen] {
                    nodes.push(pid.clone());
                }
            }
            let myself = self.myself();
            let msg =
                Message::shuffle_request(myself.clone(), myself, o.shuffle_random_walk_len, nodes);
            if let Some(peer) = self.active_view_mut().get_mut(&recipient) {
                peer.send(&msg).await?;
            }
        }
        Ok(())
    }

    async fn on_shuffle_request(
        &mut self,
        origin: PeerId,
        sender: PeerId,
        ttl: u32,
        nodes: Vec<Arc<PeerInfo>>,
    ) -> Result<()> {
        if ttl == 0 {
            let exchange_len = self.passive_view().len().min(nodes.len());
            let mut exchange_nodes = Vec::with_capacity(exchange_len);
            for node in nodes {
                if !self.already_known(&node.id) {
                    if let Some((_, peer)) = self.passive_view_mut().insert_replace(
                        node.id.clone(),
                        node.into(),
                        &mut thread_rng(),
                    ) {
                        exchange_nodes.push(peer.info);
                    }
                }
            }
            if !exchange_nodes.is_empty() {
                if let Some(origin) = self.active_view_mut().get_mut(&origin) {
                    origin
                        .send(&Message::shuffle_response(exchange_nodes))
                        .await?;
                }
            }
        } else {
            let myself = self.myself();
            let peer = self
                .active_view_mut()
                .peek_value_mut(random(), |v| v.info.id != origin && v.info.id != sender);
            if let Some(peer) = peer {
                let msg = Message::shuffle_request(origin, myself, ttl - 1, nodes);
                peer.send(&msg).await?;
            }
        }
        Ok(())
    }

    fn on_shuffle_response(&mut self, nodes: &[Arc<PeerInfo>]) {
        let mut rng = thread_rng();
        for node in nodes {
            if !self.already_known(&node.id) {
                self.passive_view_mut().insert_replace(
                    node.id.clone(),
                    node.clone().into(),
                    &mut rng,
                );
            }
        }
    }

    fn already_known(&self, pid: &PeerId) -> bool {
        self.myself_ref().eq(pid)
            || self.active_view().contains(pid)
            || self.passive_view().contains(pid)
    }

    async fn disconnect(&mut self, peer: &mut ActivePeer<S>, graceful: bool) -> Result<()> {
        let myself = self.myself();
        peer.send(&Message::disconnected(myself, graceful)).await?;
        peer.close().await?;
        Ok(())
    }
}

#[derive(Debug, Clone)]
pub enum PeerEvent {
    Up(Arc<PeerInfo>),
    Down(Arc<PeerInfo>),
}

#[derive(Debug, Clone)]
pub struct ActivePeer<S> {
    pub(crate) info: Arc<PeerInfo>,
    pub(crate) connection: Arc<Mutex<S>>,
}

impl<S> ActivePeer<S>
where
    S: Sink<Bytes, Error = Error> + Unpin,
{
    pub async fn send(&mut self, msg: &Message) -> Result<()> {
        let mut buf = Vec::new();
        msg.encode(&mut buf);
        let mut sink = self.connection.lock().await;
        sink.send(buf.into()).await?;
        Ok(())
    }

    pub async fn close(&mut self) -> Result<()> {
        let mut sink = self.connection.lock().await;
        sink.close().await?;
        Ok(())
    }
}

impl<S> Into<PassivePeer> for ActivePeer<S> {
    fn into(self) -> PassivePeer {
        PassivePeer { info: self.info }
    }
}

#[derive(Debug, Clone)]
pub struct PassivePeer {
    pub info: Arc<PeerInfo>,
}

#[derive(Debug, Eq, PartialEq, Hash)]
pub struct PeerInfo {
    pub pid: PeerId,
    pub manifest: Manifest,
}

impl PeerInfo {
    pub fn new(pid: PeerId, manifest: Manifest) -> Self {
        PeerInfo { pid, manifest }
    }

    pub fn write<W: Write>(&self, w: &mut W) {
        w.write_string(self.id.as_ref());
        w.write_buf(&self.manifest);
    }

    pub fn read<R: Read>(r: &mut R) -> Result<Self, lib0::error::Error> {
        let pid = Arc::from(r.read_string()?);
        let manifest = Manifest::from(r.read_buf()?);
        Ok(PeerInfo { pid, manifest })
    }
}

impl Into<Arc<PeerInfo>> for PassivePeer {
    fn into(self) -> Arc<PeerInfo> {
        self.info
    }
}

impl From<Arc<PeerInfo>> for PassivePeer {
    fn from(value: Arc<PeerInfo>) -> Self {
        PassivePeer { info: value }
    }
}

pub type Manifest = Vec<u8>;
*/
