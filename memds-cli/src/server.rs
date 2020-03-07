use std::io::{self, Error, ErrorKind};

use memds_proto::memds_api::*;
use memds_proto::memds_api_grpc::MemdsClient;

use crate::util;

pub fn time(client: &MemdsClient) -> io::Result<()> {
    let mut op = Operation::new();
    op.otype = OpType::SRV_TIME;

    let mut req = RequestMsg::new();
    req.ops.push(op);

    let resp = util::rpc_exec(&client, &req)?;

    if !resp.ok {
        let msg = format!("Batch failure {}: {}", resp.err_code, resp.err_message);
        return Err(Error::new(ErrorKind::Other, msg));
    }

    let results = resp.get_results();
    assert!(results.len() == 1);

    let result = &results[0];
    if !result.ok {
        let msg = format!("server-time: {}", result.err_message);
        return Err(Error::new(ErrorKind::Other, msg));
    }

    let time_res = results[0].get_srv_time();
    println!("{:?}", time_res);
    Ok(())
}

pub mod args {
    use clap::{App, SubCommand};

    pub fn time() -> App<'static, 'static> {
        SubCommand::with_name("time").about("Server.Time: Retrieve server time")
    }
}
