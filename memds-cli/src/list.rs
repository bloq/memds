use std::io::{self, Error, ErrorKind, Write};

use memds_proto::memds_api::*;
use memds_proto::memds_api_grpc::MemdsClient;

use crate::util;

pub fn lindex(client: &MemdsClient, key: &str, index: i32) -> io::Result<()> {
    let mut op_req = ListIndexOp::new();
    op_req.set_key(key.as_bytes().to_vec());
    op_req.index = index;

    let mut op = Operation::new();
    op.otype = OpType::LIST_INDEX;
    op.set_lindex(op_req);

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

    let list_res = results[0].get_list();
    if list_res.elements.len() == 0 {
        println!("not found");
    } else {
        for element in list_res.elements.iter() {
            io::stdout().write_all(element)?;
        }
    }
    Ok(())
}

pub fn llen(client: &MemdsClient, key: &str) -> io::Result<()> {
    let mut key_req = KeyOp::new();
    key_req.set_key(key.as_bytes().to_vec());

    let mut op = Operation::new();
    op.otype = OpType::LIST_INFO;
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

    let info_res = results[0].get_list_info();
    println!("{}", info_res.length);
    Ok(())
}

pub fn pop(client: &MemdsClient, key: &str, at_head: bool) -> io::Result<()> {
    let mut op_req = ListPopOp::new();
    op_req.set_key(key.as_bytes().to_vec());
    op_req.at_head = at_head;

    let mut op = Operation::new();
    op.otype = OpType::LIST_POP;
    op.set_lpop(op_req);

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

    let list_res = results[0].get_list();
    if list_res.elements.len() == 0 {
        println!("not found");
    } else {
        for element in list_res.elements.iter() {
            io::stdout().write_all(element)?;
        }
    }
    Ok(())
}

pub fn push(
    client: &MemdsClient,
    key: &str,
    elems: &Vec<&str>,
    at_head: bool,
    require_exist: bool,
) -> io::Result<()> {
    let mut op_req = ListPushOp::new();
    op_req.set_key(key.as_bytes().to_vec());
    op_req.at_head = at_head;
    op_req.if_exists = require_exist;
    for elem in elems.iter() {
        op_req.elements.push(elem.as_bytes().to_vec());
    }

    let mut op = Operation::new();
    op.otype = OpType::LIST_PUSH;
    op.set_lpush(op_req);

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

    pub fn lindex() -> App<'static, 'static> {
        SubCommand::with_name("lindex")
            .about("List.Index: Query item at given index")
            .arg(
                Arg::with_name("key")
                    .help("Key of list to query")
                    .required(true),
            )
            .arg(
                Arg::with_name("index")
                    .help("Index of item to query")
                    .required(true),
            )
    }

    pub fn rpop() -> App<'static, 'static> {
        SubCommand::with_name("rpop")
            .about("List.RPop: Remove and return list end")
            .arg(
                Arg::with_name("key")
                    .help("Key of list to pop")
                    .required(true),
            )
    }

    pub fn lpop() -> App<'static, 'static> {
        SubCommand::with_name("lpop")
            .about("List.LPop: Remove and return list head")
            .arg(
                Arg::with_name("key")
                    .help("Key of list to pop")
                    .required(true),
            )
    }

    pub fn llen() -> App<'static, 'static> {
        SubCommand::with_name("llen")
            .about("List.Length: List metadata: length")
            .arg(
                Arg::with_name("key")
                    .help("Key of list to query")
                    .required(true),
            )
    }

    pub fn lpush() -> App<'static, 'static> {
        SubCommand::with_name("lpush")
            .about("List.LPush: Store item at list head")
            .arg(
                Arg::with_name("key")
                    .help("Key of list to store")
                    .required(true),
            )
            .arg(
                Arg::with_name("element")
                    .help("Value of item to store")
                    .required(true)
                    .multiple(true),
            )
    }

    pub fn lpushx() -> App<'static, 'static> {
        SubCommand::with_name("lpushx")
            .about("List.LPushX: Store item at list head, iff list exists")
            .arg(
                Arg::with_name("key")
                    .help("Key of list to store")
                    .required(true),
            )
            .arg(
                Arg::with_name("element")
                    .help("Value of item to store")
                    .required(true)
                    .multiple(true),
            )
    }

    pub fn rpush() -> App<'static, 'static> {
        SubCommand::with_name("rpush")
            .about("List.RPush: Store item at list end")
            .arg(
                Arg::with_name("key")
                    .help("Key of list to store")
                    .required(true),
            )
            .arg(
                Arg::with_name("element")
                    .help("Value of item to store")
                    .required(true)
                    .multiple(true),
            )
    }

    pub fn rpushx() -> App<'static, 'static> {
        SubCommand::with_name("rpushx")
            .about("List.RPushX: Store item at list end, iff list exists")
            .arg(
                Arg::with_name("key")
                    .help("Key of list to store")
                    .required(true),
            )
            .arg(
                Arg::with_name("element")
                    .help("Value of item to store")
                    .required(true)
                    .multiple(true),
            )
    }
}
