use crate::peer::{Connector, PeerInfo};
use crate::{Error, Result};
use bytes::Bytes;
use futures_util::{Sink, Stream};
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

pub struct Webrtc {}

impl Webrtc {
    pub fn new() -> Result<()> {
        todo!()
    }
}

#[async_trait::async_trait]
impl Connector for Webrtc {
    type Sink = WebrtcSink;
    type Source = WebrtcSource;

    async fn accept(&self) -> crate::Result<(Arc<PeerInfo>, Self::Sink, Self::Source)> {
        todo!()
    }

    async fn connect(
        &self,
        remote_endpoint: &str,
    ) -> crate::Result<(Arc<PeerInfo>, Self::Sink, Self::Source)> {
        todo!()
    }
}

pub struct WebrtcSink;

impl Sink<Bytes> for WebrtcSink {
    type Error = Error;

    fn poll_ready(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<()>> {
        todo!()
    }

    fn start_send(self: Pin<&mut Self>, item: Bytes) -> Result<()> {
        todo!()
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<()>> {
        todo!()
    }

    fn poll_close(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<()>> {
        todo!()
    }
}

pub struct WebrtcSource;

impl Stream for WebrtcSource {
    type Item = Result<Bytes>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        todo!()
    }
}
