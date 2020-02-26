extern crate bytes;

use bytes::{Buf, BufMut, BytesMut};
use protobuf::parse_from_bytes;
use protobuf::Message;
use std::io::Cursor;
use tokio_util::codec::{Decoder, Encoder};

use crate::error::*;
use crate::memds_api::MemdsMessage;

const HDR_SIZE: usize = 4; // [1 byte magic][3 byte size]
const MAGIC: u32 = 0x4D; // ASCII 'M'
const MAGIC_SHIFT: usize = 24;
const MSG_SIZE_MASK: u32 = 0xffffff; // lower 24 bits; thus max msg sz = 16M

#[derive(Debug, Clone, Copy, Eq, PartialEq, Ord, PartialOrd, Hash)]
enum DecodeState {
    Head,
    Data(usize),
}

#[derive(Copy, Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct MemdsCodec {
    state: DecodeState,
}

impl MemdsCodec {
    pub fn new() -> MemdsCodec {
        MemdsCodec {
            state: DecodeState::Head,
        }
    }

    fn decode_head(&mut self, src: &mut BytesMut) -> Result<Option<usize>, MemdsError> {
        // check hdr len + 1 (+1 for "something else follows")
        if src.len() <= HDR_SIZE {
            return Ok(None);
        }

        let msg_size = {
            let mut src = Cursor::new(&mut *src);
            let header = src.get_uint(4) as u32;

            let magic = header >> MAGIC_SHIFT;
            if magic != MAGIC {
                return Err(MemdsError::InvalidFrame);
            }

            (header & MSG_SIZE_MASK) as usize
        };

        self.state = DecodeState::Data(msg_size);
        src.reserve(msg_size);
        Ok(Some(msg_size))
    }

    fn decode_data(
        &mut self,
        msg_size: usize,
        src: &mut BytesMut,
    ) -> Result<Option<MemdsMessage>, MemdsError> {
        if src.len() < msg_size {
            return Ok(None);
        }

        let data = src.split_to(msg_size);
        match parse_from_bytes::<MemdsMessage>(&data) {
            Err(_e) => Err(MemdsError::ProtobufDecode),
            Ok(req) => {
                self.state = DecodeState::Head;
                src.reserve(HDR_SIZE);
                Ok(Some(req))
            }
        }
    }
}

impl Decoder for MemdsCodec {
    type Item = MemdsMessage;
    type Error = MemdsError;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<MemdsMessage>, MemdsError> {
        let msg_size = match self.state {
            DecodeState::Head => match self.decode_head(src) {
                Err(e) => return Err(e),
                Ok(opt) => match opt {
                    None => return Ok(None),
                    Some(n) => n,
                },
            },
            DecodeState::Data(n) => n,
        };

        match self.decode_data(msg_size, src) {
            Err(e) => Err(e),
            Ok(opt) => match opt {
                None => Ok(None),
                Some(msg) => Ok(Some(msg)),
            },
        }
    }
}

impl Encoder for MemdsCodec {
    type Item = MemdsMessage;
    type Error = MemdsError;

    fn encode(&mut self, msg: MemdsMessage, dst: &mut BytesMut) -> Result<(), MemdsError> {
        let msg_bytes = msg.write_to_bytes().unwrap();
        if msg_bytes.len() > MSG_SIZE_MASK as usize {
            return Err(MemdsError::InvalidFrame);
        }

        let msg_len: u32 = msg_bytes.len() as u32;

        let header = (msg_len & MSG_SIZE_MASK) | (MAGIC << 24);

        dst.reserve(HDR_SIZE + msg_len as usize);
        dst.put_uint(header as u64, 4);
        dst.extend_from_slice(&msg_bytes);
        Ok(())
    }
}
