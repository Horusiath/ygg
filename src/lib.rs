mod gossip;
mod membership;
mod peer;
mod tcp;
mod view;
mod webrtc;

pub type Error = Box<dyn std::error::Error + Sync + Send>;
pub type Result<T> = std::result::Result<T, Error>;
