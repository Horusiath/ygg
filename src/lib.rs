mod gossip;
mod membership;
mod peer;
mod tcp;
mod utils;
mod view;
mod webrtc;

pub type Error = Box<dyn std::error::Error + Sync + Send>;
pub type Result<T> = std::result::Result<T, Error>;

/// Time-to-live.
pub type TTL = u32;
