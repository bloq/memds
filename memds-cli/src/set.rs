use std::io::{self, Error, ErrorKind, Write};

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

pub fn add_del(
    client: &MemdsClient,
    key: &str,
    elems: &Vec<&str>,
    do_delete: bool,
) -> io::Result<()> {
    let mut op_req = KeyedListOp::new();
    op_req.set_key(key.as_bytes().to_vec());
    for elem in elems.iter() {
        op_req.elements.push(elem.as_bytes().to_vec());
    }

    let mut op = Operation::new();
    op.otype = match do_delete {
        true => OpType::SET_DEL,
        false => OpType::SET_ADD,
    };
    op.set_keyed_list(op_req);

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

pub fn diff(client: &MemdsClient, keys: &Vec<&str>, store_key: &str) -> io::Result<()> {
    let mut op_req = CmpStoreOp::new();
    for key in keys.iter() {
        op_req.keys.push(key.as_bytes().to_vec());
    }
    let have_store_key = {
        if store_key.len() > 0 {
            op_req.set_store_key(store_key.as_bytes().to_vec());
            true
        } else {
            false
        }
    };

    let mut op = Operation::new();
    op.otype = OpType::SET_DIFF;
    op.set_cmp_stor(op_req);

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
        let msg = format!("{:?}: {}", keys, result.err_message);
        return Err(Error::new(ErrorKind::Other, msg));
    }

    if have_store_key {
        let count_res = results[0].get_count();
        println!("{}", count_res.n);
    } else {
        let list_res = results[0].get_list();
        for element in list_res.elements.iter() {
            io::stdout().write_all(element)?;
            io::stdout().write_all(b"\n")?;
        }
    }

    Ok(())
}

pub fn is_member(client: &MemdsClient, key: &str, elems: &Vec<&str>) -> io::Result<()> {
    let mut op_req = KeyedListOp::new();
    op_req.set_key(key.as_bytes().to_vec());
    for elem in elems.iter() {
        op_req.elements.push(elem.as_bytes().to_vec());
    }

    let mut op = Operation::new();
    op.otype = OpType::SET_ISMEMBER;
    op.set_keyed_list(op_req);

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

pub fn members(client: &MemdsClient, key: &str) -> io::Result<()> {
    let mut key_req = KeyOp::new();
    key_req.set_key(key.as_bytes().to_vec());

    let mut op = Operation::new();
    op.otype = OpType::SET_MEMBERS;
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

    let list_res = results[0].get_list();
    for element in list_res.elements.iter() {
        io::stdout().write_all(element)?;
        io::stdout().write_all(b"\n")?;
    }
    Ok(())
}

pub mod args {
    use clap::{App, Arg, SubCommand};

    pub fn sadd() -> App<'static, 'static> {
        SubCommand::with_name("sadd")
            .about("Set.Add: Store items in set")
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

    pub fn sdiff() -> App<'static, 'static> {
        SubCommand::with_name("sdiff")
            .about("Set.Diff: Diff sets")
            .arg(
                Arg::with_name("key1")
                    .help("1st Set for difference")
                    .required(true),
            )
            .arg(
                Arg::with_name("keys")
                    .help("List of subtractive sets")
                    .multiple(true),
            )
    }

    pub fn sdiffstore() -> App<'static, 'static> {
        SubCommand::with_name("sdiffstore")
            .about("Set.DiffStore: Diff sets, and store result")
            .arg(
                Arg::with_name("destination")
                    .help("Set receiving difference results")
                    .required(true),
            )
            .arg(
                Arg::with_name("key1")
                    .help("1st Set for difference")
                    .required(true),
            )
            .arg(
                Arg::with_name("keys")
                    .help("List of subtractive sets")
                    .multiple(true),
            )
    }

    pub fn sismember() -> App<'static, 'static> {
        SubCommand::with_name("sismember")
            .about("Set.IsMember: Test existence of items in a set")
            .arg(
                Arg::with_name("key")
                    .help("Key of set to query")
                    .required(true),
            )
            .arg(
                Arg::with_name("element")
                    .help("Value of item to test")
                    .required(true)
                    .multiple(true),
            )
    }

    pub fn smembers() -> App<'static, 'static> {
        SubCommand::with_name("smembers")
            .about("Set.Members: Query all Set members")
            .arg(
                Arg::with_name("key")
                    .help("Key of set to query")
                    .required(true),
            )
    }

    pub fn srem() -> App<'static, 'static> {
        SubCommand::with_name("srem")
            .about("Set.Remove: Remove items from set")
            .arg(
                Arg::with_name("key")
                    .help("Key of set to update")
                    .required(true),
            )
            .arg(
                Arg::with_name("element")
                    .help("Value of item to remove")
                    .required(true)
                    .multiple(true),
            )
    }
}
