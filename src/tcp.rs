use crate::peer::{Connector, PeerInfo};
use crate::Result;
use async_trait::async_trait;
use bytes::Bytes;
use futures_util::{ready, Sink, SinkExt, Stream, StreamExt};
use std::fmt::Formatter;
use std::net::SocketAddr;
use std::pin::Pin;
use std::str::FromStr;
use std::sync::Arc;
use std::task::{Context, Poll};
use tokio_util::codec::{FramedRead, FramedWrite, LengthDelimitedCodec};

//#[cfg(not(test))]
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
//#[cfg(not(test))]
use tokio::net::{TcpListener, TcpStream};

//#[cfg(test)]
//use turmoil::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
//#[cfg(test)]
//use turmoil::net::{TcpListener, TcpStream};

pub struct Tcp {
    peer_info: Bytes,
    server: TcpListener,
    public_endpoint: String,
}

impl Tcp {
    pub async fn bind(addr: &str, info: Arc<PeerInfo>) -> Result<Self> {
        let server = TcpListener::bind(SocketAddr::from_str(addr)?).await?;
        let public_endpoint = server.local_addr()?.to_string();
        let peer_info = Bytes::from(serde_cbor::to_vec(&info)?);
        Ok(Tcp {
            server,
            peer_info,
            public_endpoint,
        })
    }

    pub fn public_endpoint(&self) -> &str {
        &self.public_endpoint
    }

    async fn init_connection(
        &self,
        sink: &mut TcpSink,
        source: &mut TcpSource,
        addr: &str,
    ) -> Result<Arc<PeerInfo>> {
        let peer_info = self.peer_info.clone();
        sink.send(peer_info).await?;
        if let Some(res) = source.next().await {
            let remote_peer_info = serde_cbor::from_slice::<PeerInfo>(&res?)?;
            Ok(Arc::new(remote_peer_info))
        } else {
            Err(format!("remote peer receiver '{}' closed unexpectedly", addr).into())
        }
    }
}

impl std::fmt::Debug for Tcp {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Tcp")
            .field("public_endpoint", &self.public_endpoint)
            .field("peer_info", &self.peer_info)
            .finish()
    }
}

#[async_trait]
impl Connector for Tcp {
    type Sink = TcpSink;
    type Source = TcpSource;

    async fn accept(&self) -> Result<(Arc<PeerInfo>, TcpSink, TcpSource)> {
        let (stream, addr) = self.server.accept().await?;
        let (source, sink) = stream.into_split();
        let mut sink = TcpSink::new(sink);
        let mut source = TcpSource::new(source);
        let peer_info = self
            .init_connection(&mut sink, &mut source, &addr.to_string())
            .await?;
        log::info!(
            "'{}' accepted connection from '{}' (peer id: {})",
            self.public_endpoint(),
            addr.to_string(),
            peer_info.id
        );
        Ok((peer_info, sink, source))
    }

    async fn connect(&self, remote_endpoint: &str) -> Result<(Arc<PeerInfo>, TcpSink, TcpSource)> {
        let stream = TcpStream::connect(SocketAddr::from_str(remote_endpoint)?).await?;
        let (source, sink) = stream.into_split();
        let mut sink = TcpSink::new(sink);
        let mut source = TcpSource::new(source);
        let peer_info = self
            .init_connection(&mut sink, &mut source, &remote_endpoint)
            .await?;
        log::info!(
            "'{}' connected to '{}' (peer id: {})",
            self.public_endpoint(),
            remote_endpoint,
            peer_info.id
        );
        Ok((peer_info, sink, source))
    }
}

#[derive(Debug)]
pub struct TcpSink(FramedWrite<OwnedWriteHalf, LengthDelimitedCodec>);

impl TcpSink {
    fn new(w: OwnedWriteHalf) -> Self {
        TcpSink(FramedWrite::new(w, LengthDelimitedCodec::default()))
    }
}

impl Sink<Bytes> for TcpSink {
    type Error = crate::Error;

    fn poll_ready(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<()>> {
        let s = unsafe { Pin::new_unchecked(&mut self.0) };
        let res = ready!(s.poll_ready(cx)).map_err(|e| e.into());
        Poll::Ready(res)
    }

    fn start_send(mut self: Pin<&mut Self>, item: Bytes) -> Result<()> {
        let s = unsafe { Pin::new_unchecked(&mut self.0) };
        s.start_send(item).map_err(|e| e.into())
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<()>> {
        let s = unsafe { Pin::new_unchecked(&mut self.0) };
        let res = ready!(s.poll_flush(cx)).map_err(|e| e.into());
        Poll::Ready(res)
    }

    fn poll_close(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<()>> {
        let s = unsafe { Pin::new_unchecked(&mut self.0) };
        let res = ready!(s.poll_close(cx)).map_err(|e| e.into());
        Poll::Ready(res)
    }
}

#[derive(Debug)]
pub struct TcpSource(FramedRead<OwnedReadHalf, LengthDelimitedCodec>);

impl TcpSource {
    fn new(r: OwnedReadHalf) -> Self {
        TcpSource(FramedRead::new(r, LengthDelimitedCodec::default()))
    }
}

impl Stream for TcpSource {
    type Item = Result<Bytes>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let s = unsafe { Pin::new_unchecked(&mut self.0) };
        if let Some(res) = ready!(s.poll_next(cx)) {
            match res {
                Ok(bytes) => Poll::Ready(Some(Ok(Bytes::from(bytes)))),
                Err(err) => Poll::Ready(Some(Err(err.into()))),
            }
        } else {
            Poll::Ready(None)
        }
    }
}
