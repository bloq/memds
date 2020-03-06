use std::io::{self, Error, ErrorKind};

use memds_proto::memds_api::*;
use memds_proto::memds_api_grpc::MemdsClient;

use crate::util;

pub fn del_exist(client: &MemdsClient, keys: &Vec<&str>, remove_it: bool) -> io::Result<()> {
    let mut op_req = KeyListOp::new();
    for key in keys {
        op_req.keys.push(key.as_bytes().to_vec());
    }

    let mut op = Operation::new();
    op.otype = match remove_it {
        true => OpType::KEYS_DEL,
        false => OpType::KEYS_EXIST,
    };
    op.set_key_list(op_req);

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
        let msg = format!("{}...: {}", keys[0], result.err_message);
        return Err(Error::new(ErrorKind::Other, msg));
    }

    let count_res = results[0].get_count();
    println!("{}", count_res.n);
    Ok(())
}

pub fn typ(client: &MemdsClient, key: &str) -> io::Result<()> {
    let mut key_req = KeyOp::new();
    key_req.set_key(key.as_bytes().to_vec());

    let mut op = Operation::new();
    op.otype = OpType::KEYS_TYPE;
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

    let type_res = results[0].get_typ();
    println!("{:?}", type_res.typ);
    Ok(())
}

pub mod args {
    use clap::{App, Arg, SubCommand};

    pub fn del() -> App<'static, 'static> {
        SubCommand::with_name("del")
            .about("Keys.Del: Delete listed keys")
            .arg(
                Arg::with_name("key")
                    .help("Key to delete")
                    .required(true)
                    .multiple(true),
            )
    }

    pub fn exists() -> App<'static, 'static> {
        SubCommand::with_name("exists")
            .about("Keys.Exists: Count existing listed keys")
            .arg(
                Arg::with_name("key")
                    .help("Key to test")
                    .required(true)
                    .multiple(true),
            )
    }

    pub fn typ() -> App<'static, 'static> {
        SubCommand::with_name("type")
            .about("Keys.Type: Query item data type")
            .arg(
                Arg::with_name("key")
                    .help("Key of item to query")
                    .required(true),
            )
    }
}
