use std::io::{self, Error, ErrorKind};

use memds_proto::memds_api::*;
use memds_proto::memds_api_grpc::MemdsClient;

use crate::util;

pub fn info(client: &MemdsClient, key: &str) -> io::Result<()> {
    let mut key_req = KeyOp::new();
    key_req.set_key(key.as_bytes().to_vec());

    let mut op = Operation::new();
    op.otype = OpType::SET_INFO;
    op.set_key(key_req);

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
        let msg = format!("{}: {}", key, result.err_message);
        return Err(Error::new(ErrorKind::Other, msg));
    }

    let info_res = results[0].get_set_info();
    println!("{}", info_res.length);
    Ok(())
}

pub fn add(client: &MemdsClient, key: &str, elems: &Vec<&str>) -> io::Result<()> {
    let mut op_req = SetAddOp::new();
    op_req.set_key(key.as_bytes().to_vec());
    for elem in elems.iter() {
        op_req.elements.push(elem.as_bytes().to_vec());
    }

    let mut op = Operation::new();
    op.otype = OpType::SET_ADD;
    op.set_set_add(op_req);

    let mut req = RequestMsg::new();
    req.ops.push(op);

    let resp = util::rpc_exec(&client, &req)?;

    if !resp.ok {
        let msg = format!("Batch failure {}: {}", resp.err_code, resp.err_message);
        return Err(Error::new(ErrorKind::Other, msg));
    }

    let results = resp.get_results();
    assert_eq!(results.len(), 1);

    let result = &results[0];
    if !result.ok {
        let msg = format!("{}: {}", key, result.err_message);
        return Err(Error::new(ErrorKind::Other, msg));
    }

    let count_res = results[0].get_count();
    println!("{}", count_res.n);
    Ok(())
}

pub mod args {
    use clap::{App, Arg, SubCommand};

    pub fn sadd() -> App<'static, 'static> {
        SubCommand::with_name("sadd")
            .about("Set.Add: Store item in set")
            .arg(
                Arg::with_name("key")
                    .help("Key of set to store")
                    .required(true),
            )
            .arg(
                Arg::with_name("element")
                    .help("Value of item to store")
                    .required(true)
                    .multiple(true),
            )
    }

    pub fn scard() -> App<'static, 'static> {
        SubCommand::with_name("scard")
            .about("Set.Card: Set metadata")
            .arg(
                Arg::with_name("key")
                    .help("Key of set to query")
                    .required(true),
            )
    }
}
