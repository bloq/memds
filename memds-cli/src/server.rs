use std::io::{self, Error, ErrorKind};

use memds_proto::memds_api::*;
use memds_proto::memds_api_grpc::MemdsClient;

use crate::util;

pub fn dbsize(client: &MemdsClient) -> io::Result<()> {
    let mut op = Operation::new();
    op.otype = OpType::SRV_DBSIZE;

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
        let msg = format!("dbsize: {}", result.err_message);
        return Err(Error::new(ErrorKind::Other, msg));
    }

    let count_res = results[0].get_count();
    println!("{}", count_res.n);
    Ok(())
}

pub fn flush(client: &MemdsClient, flush_all: bool) -> io::Result<()> {
    let mut op = Operation::new();
    op.otype = match flush_all {
        true => OpType::SRV_FLUSHALL,
        false => OpType::SRV_FLUSHDB,
    };

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
        let msg = format!("flush: {}", result.err_message);
        return Err(Error::new(ErrorKind::Other, msg));
    }

    println!("ok");
    Ok(())
}

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

    pub fn dbsize() -> App<'static, 'static> {
        SubCommand::with_name("dbsize")
            .about("Server.DBSize: Retrieve item count of current database")
    }

    pub fn flushdb() -> App<'static, 'static> {
        SubCommand::with_name("flushdb").about("Server.FlushDB: Empty current database")
    }

    pub fn flushall() -> App<'static, 'static> {
        SubCommand::with_name("flushall").about("Server.FlushAll: Empty all databases")
    }

    pub fn time() -> App<'static, 'static> {
        SubCommand::with_name("time").about("Server.Time: Retrieve server time")
    }
}
