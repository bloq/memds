mod memds_api;

use protobuf::Message;

pub use memds_api::GetRequest;
pub use memds_api::GetResponse;
pub use memds_api::Response;

pub fn req_get(key: &[u8], length_only: bool) -> Vec<u8> {
    let mut out_msg = GetRequest::new();
    out_msg.set_key(key.to_vec());
    out_msg.want_length = length_only;

    out_msg.write_to_bytes().unwrap()
}
