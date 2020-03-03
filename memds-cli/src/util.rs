
use futures::Future;
use std::io::{self, Error, ErrorKind};

use memds_proto::memds_api::*;
use memds_proto::memds_api_grpc::MemdsClient;

pub fn rpc_exec(client: &MemdsClient, req: &RequestMsg) -> io::Result<ResponseMsg> {
    let exec = client.exec_async(&req).unwrap();
    match exec.wait() {
        Err(e) => {
            let msg = format!("RPC.Exec failed: {:?}", e);
            Err(Error::new(ErrorKind::Other, msg))
        }
        Ok(resp) => Ok(resp),
    }
}

