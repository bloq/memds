use std::collections::HashSet;

mod codec;
mod error;

pub const DEF_PORT: u16 = 16900;

#[derive(Clone)]
pub enum Atom {
    String(Vec<u8>),
    List(Vec<Vec<u8>>),
    Set(HashSet<Vec<u8>>),
}

pub mod memds_api;
pub mod memds_api_grpc;
pub mod util;

pub use codec::MemdsCodec;
pub use error::MemdsError;
