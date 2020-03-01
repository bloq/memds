// Encoder/decoder for our protobuf-based wire protocol
//
// Framing:
// [4-byte header; 1 byte magic, 3 bytes size]
// [4-byte checksum]
// [message data]
//
// Checksumming:
// [4-byte CRC from previous frame]
// [4-byte header]
// [message data]
//
// All integers stored in big endian (aka network byte order).
//

use bytes::{Buf, BufMut, BytesMut};
use crc::{crc32, Hasher32};
use protobuf::parse_from_bytes;
use protobuf::Message;
use std::io::Cursor;
use tokio_util::codec::{Decoder, Encoder};

use crate::error::*;
use crate::memds_api::MemdsMessage;

#[cfg(test)]
use crate::util;

const HDR_SIZE: usize = 4; // [1 byte magic][3 byte size]
const MAGIC: u32 = 0x4D; // ASCII 'M'
const MAGIC_SHIFT: usize = 24;
const MSG_SIZE_MASK: u32 = 0xffffff; // lower 24 bits; thus max msg sz = 16M
const CRC32_GENESIS: u32 = 0xdeadbeef;

#[derive(Debug, Clone, Copy, Eq, PartialEq, Ord, PartialOrd, Hash)]
enum DecodeState {
    Head,
    Data(usize),
}

#[derive(Copy, Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct MemdsCodec {
    state: DecodeState,
    last_dec_crc: u32,
    last_enc_crc: u32,
    expect_crc: u32,
    hdr_buf: [u8; HDR_SIZE],
}

impl MemdsCodec {
    pub fn new() -> MemdsCodec {
        MemdsCodec {
            state: DecodeState::Head,
            last_dec_crc: CRC32_GENESIS,
            last_enc_crc: CRC32_GENESIS,
            expect_crc: 0,
            hdr_buf: [0; HDR_SIZE],
        }
    }

    fn decode_head(&mut self, src: &mut BytesMut) -> Result<Option<usize>, MemdsError> {
        // check hdr len + 1 (+1 for "something else follows")
        if src.len() <= HDR_SIZE {
            return Ok(None);
        }

        // parse header:  [4-byte magic/size][4-byte checksum]
        let msg_size = {
            let mut src = Cursor::new(&mut *src);

            let header = src.get_uint(HDR_SIZE) as u32;

            let magic = header >> MAGIC_SHIFT;
            if magic != MAGIC {
                return Err(MemdsError::InvalidFrame);
            }

            let crc = src.get_uint(HDR_SIZE) as u32;
            self.expect_crc = crc;

            (header & MSG_SIZE_MASK) as usize
        };

        // advance cursor past header
        let hdr_buf = src.split_to(HDR_SIZE);
        src.advance(4); // skip crc

        // remember top HDR_SIZE bytes, by copying hdr_bytes -> self.hdr_buf
        let hdr_bytes = hdr_buf.clone().freeze();
        for (&x, p) in hdr_bytes.iter().zip(self.hdr_buf.iter_mut()) {
            *p = x;
        }

        // switch to data state, and prepare for msg_size packet
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

        // carve away our portion of the frame
        let data = src.split_to(msg_size);

        // build BE buffer containing last-frame-crc
        let crc_buf = self.last_dec_crc.to_be_bytes();

        // build CRC for current frame
        let mut digest = crc32::Digest::new(crc32::IEEE);
        digest.write(&crc_buf);
        digest.write(&self.hdr_buf);
        digest.write(&data);
        self.last_dec_crc = digest.sum32();

        // verify CRC matches expected
        if self.last_dec_crc != self.expect_crc {
            return Err(MemdsError::InvalidChecksum);
        }

        // execute protobuf decode of full frame
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
        // parse header; exit early if incomplete or error
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

        // parse data
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

        // build header
        let msg_len: u32 = msg_bytes.len() as u32;
        let header = (msg_len & MSG_SIZE_MASK) | (MAGIC << 24);
        let hdr_buf = header.to_be_bytes();

        // canonical encoding of last-frame-CRC
        let crc_buf = self.last_enc_crc.to_be_bytes();

        // build CRC of current frame
        let mut digest = crc32::Digest::new(crc32::IEEE);
        digest.write(&crc_buf);
        digest.write(&hdr_buf);
        digest.write(&msg_bytes);
        self.last_enc_crc = digest.sum32();

        // assemble frame parts in linear buffer
        dst.reserve(HDR_SIZE + 4 + msg_len as usize);
        dst.put_uint(header as u64, HDR_SIZE);
        dst.put_uint(self.last_enc_crc as u64, 4);
        dst.extend_from_slice(&msg_bytes);

        Ok(())
    }
}

#[test]
fn basic_codec() {
    let mut codec = MemdsCodec::new();

    // message #1
    let enc_msg = util::resp_err(-404, "not found");
    let enc_msg_raw = &mut BytesMut::new();
    codec.encode(enc_msg.clone(), enc_msg_raw).unwrap();

    let dec_msg = codec.decode(enc_msg_raw).unwrap().unwrap();
    assert_eq!(enc_msg, dec_msg);

    // message #2
    let enc_msg = util::resp_err(-500, "server error");
    let enc_msg_raw = &mut BytesMut::new();
    codec.encode(enc_msg.clone(), enc_msg_raw).unwrap();

    let dec_msg = codec.decode(enc_msg_raw).unwrap().unwrap();
    assert_eq!(enc_msg, dec_msg);
}

#[test]
fn invalid_checksum() {
    let mut codec = MemdsCodec::new();

    // encode message
    let enc_msg = util::resp_err(-404, "not found");
    let enc_msg_raw = &mut BytesMut::new();
    codec.encode(enc_msg.clone(), enc_msg_raw).unwrap();

    // change last char of message data
    let last_pos = enc_msg_raw.len() - 1;
    let last_char = enc_msg_raw[last_pos];
    enc_msg_raw[last_pos] = last_char + 1;

    // decode should indicate bad csum
    let res = codec.decode(enc_msg_raw);
    match res {
        Ok(_) => assert!(false),
        Err(e) => match e {
            MemdsError::InvalidChecksum => {}
            _ => {
                assert!(false);
            }
        },
    }
}
