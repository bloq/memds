use std::fs::File;
use std::io::{self, Error, ErrorKind, Read, Write};

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

pub fn rename(
    client: &MemdsClient,
    old_key: &str,
    new_key: &str,
    create_excl: bool,
) -> io::Result<()> {
    let mut key_req = KeyRenameOp::new();
    key_req.set_old_key(old_key.as_bytes().to_vec());
    key_req.set_new_key(new_key.as_bytes().to_vec());
    key_req.create_excl = create_excl;

    let mut op = Operation::new();
    op.otype = OpType::KEYS_RENAME;
    op.set_rename(key_req);

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
        let msg = format!("keys-rename: {}", result.err_message);
        return Err(Error::new(ErrorKind::Other, msg));
    }

    println!("ok");
    Ok(())
}

pub fn dump(client: &MemdsClient, key: &str) -> io::Result<()> {
    let mut key_req = KeyOp::new();
    key_req.set_key(key.as_bytes().to_vec());

    let mut op = Operation::new();
    op.otype = OpType::KEY_DUMP;
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

    let get_res = results[0].get_get();
    let value = get_res.get_value();
    io::stdout().write_all(value)?;
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

fn read_file(in_fn: &str) -> io::Result<Vec<u8>> {
    let mut f = File::open(in_fn)?;
    let mut buffer = Vec::new();
    f.read_to_end(&mut buffer)?;
    Ok(buffer)
}

pub fn restore(client: &MemdsClient, key: Option<&str>, restore_fn: &str) -> io::Result<()> {
    let mut set_req = StrSetOp::new();
    if key.is_some() {
        set_req.set_key(key.unwrap().as_bytes().to_vec());
    }

    let wire_data = read_file(restore_fn)?;
    set_req.set_value(wire_data);

    let mut op = Operation::new();
    op.otype = OpType::KEY_RESTORE;
    op.set_set(set_req);

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
    if result.ok {
        io::stdout().write_all(b"ok\n")?;
        Ok(())
    } else {
        let msg = format!("{:?}: {}", key, result.err_message);
        Err(Error::new(ErrorKind::Other, msg))
    }
}

pub mod args {
    use clap::{App, Arg, SubCommand};

    pub fn del() -> App<'static> {
        SubCommand::with_name("del")
            .about("Keys.Del: Delete listed keys")
            .arg(
                Arg::with_name("key")
                    .help("Key to delete")
                    .required(true)
                    .multiple(true),
            )
    }

    pub fn dump() -> App<'static> {
        SubCommand::with_name("dump")
            .about("Keys.Dump: Dump listed key")
            .arg(Arg::with_name("key").help("Key to dump").required(true))
    }

    pub fn exists() -> App<'static> {
        SubCommand::with_name("exists")
            .about("Keys.Exists: Count existing listed keys")
            .arg(
                Arg::with_name("key")
                    .help("Key to test")
                    .required(true)
                    .multiple(true),
            )
    }

    pub fn rename() -> App<'static> {
        SubCommand::with_name("rename")
            .about("Keys.Rename: Rename item key")
            .arg(
                Arg::with_name("old_key")
                    .help("Source Key of item to rename")
                    .required(true),
            )
            .arg(
                Arg::with_name("new_key")
                    .help("Destination Key of item")
                    .required(true),
            )
    }

    pub fn renamenx() -> App<'static> {
        SubCommand::with_name("renamenx")
            .about("Keys.RenameNX: Rename item key, iff new key does not exist")
            .arg(
                Arg::with_name("old_key")
                    .help("Source Key of item to rename")
                    .required(true),
            )
            .arg(
                Arg::with_name("new_key")
                    .help("Destination Key of item")
                    .required(true),
            )
    }

    pub fn restore() -> App<'static> {
        SubCommand::with_name("restore")
            .about("Keys.Restore: Restore item from dumpfile")
            .arg(
                Arg::with_name("file")
                    .help("Pathname of item to restore")
                    .required(true),
            )
            .arg(
                Arg::with_name("key")
                    .help("Key of item to query")
                    .short('k')
                    .long("key")
                    .value_name("string"),
            )
    }

    pub fn typ() -> App<'static> {
        SubCommand::with_name("type")
            .about("Keys.Type: Query item data type")
            .arg(
                Arg::with_name("key")
                    .help("Key of item to query")
                    .required(true),
            )
    }
}
