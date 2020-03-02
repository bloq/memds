use futures::Future;
use std::io::{self, Error, ErrorKind, Write};

use memds_proto::memds_api::*;
use memds_proto::memds_api_grpc::MemdsClient;

fn rpc_exec(client: &MemdsClient, req: &RequestMsg) -> io::Result<ResponseMsg> {
    let exec = client.exec_async(&req).unwrap();
    match exec.wait() {
        Err(e) => {
            let msg = format!("RPC.Exec failed: {:?}", e);
            Err(Error::new(ErrorKind::Other, msg))
        }
        Ok(resp) => Ok(resp),
    }
}

pub fn get(client: &MemdsClient, key: &str) -> io::Result<()> {
    let mut get_req = StrGetOp::new();
    get_req.set_key(key.as_bytes().to_vec());

    let mut op = Operation::new();
    op.otype = OpType::STR_GET;
    op.set_get(get_req);

    let mut req = RequestMsg::new();
    req.ops.push(op);

    let resp = rpc_exec(&client, &req)?;

    if !resp.ok {
        let msg = format!("Batch failure {}: {}", resp.err_code, resp.err_message);
        return Err(Error::new(ErrorKind::Other, msg));
    }

    let results = resp.get_results();
    assert!(results.len() == 1);

    let result = &results[0];
    if result.ok {
        let get_res = results[0].get_get();
        let value = get_res.get_value();
        io::stdout().write_all(value)?;
        Ok(())
    } else {
        let msg = format!("{}: {}", key, result.err_message);
        Err(Error::new(ErrorKind::Other, msg))
    }
}

pub fn set(
    client: &MemdsClient,
    key: &str,
    value: &str,
    return_old: bool,
    append: bool,
) -> io::Result<()> {
    let mut set_req = StrSetOp::new();
    set_req.set_key(key.as_bytes().to_vec());
    set_req.set_value(value.as_bytes().to_vec());
    set_req.return_old = return_old;

    let mut op = Operation::new();
    op.otype = match append {
        false => OpType::STR_SET,
        true => OpType::STR_APPEND,
    };
    op.set_set(set_req);

    let mut req = RequestMsg::new();
    req.ops.push(op);

    let resp = rpc_exec(&client, &req)?;

    if !resp.ok {
        let msg = format!("Batch failure {}: {}", resp.err_code, resp.err_message);
        return Err(Error::new(ErrorKind::Other, msg));
    }

    let results = resp.get_results();
    assert!(results.len() == 1);

    let result = &results[0];
    if result.ok {
        if return_old {
            let set_res = results[0].get_set();
            io::stdout().write_all(set_res.get_old_value())?;
        } else {
            io::stdout().write_all(b"ok\n")?;
        }
        Ok(())
    } else {
        let msg = format!("{}: {}", key, result.err_message);
        Err(Error::new(ErrorKind::Other, msg))
    }
}

pub fn incrdecr(client: &MemdsClient, otype: OpType, key: &str, n: i64) -> io::Result<()> {
    let mut num_req = NumOp::new();
    num_req.set_key(key.as_bytes().to_vec());
    num_req.n = n;

    let mut op = Operation::new();
    op.otype = otype;
    op.set_num(num_req);

    let mut req = RequestMsg::new();
    req.ops.push(op);

    let resp = rpc_exec(&client, &req)?;

    if !resp.ok {
        let msg = format!("Batch failure {}: {}", resp.err_code, resp.err_message);
        return Err(Error::new(ErrorKind::Other, msg));
    }

    let results = resp.get_results();
    assert!(results.len() == 1);

    let result = &results[0];
    if result.ok {
        let num_res = results[0].get_num();
        let old_value = num_res.get_old_value();
        println!("{}", old_value);
        Ok(())
    } else {
        let msg = format!("{}: {}", key, result.err_message);
        Err(Error::new(ErrorKind::Other, msg))
    }
}

pub fn strlen(client: &MemdsClient, key: &str) -> io::Result<()> {
    let mut get_req = StrGetOp::new();
    get_req.set_key(key.as_bytes().to_vec());
    get_req.want_length = true;

    let mut op = Operation::new();
    op.otype = OpType::STR_GET;
    op.set_get(get_req);

    let mut req = RequestMsg::new();
    req.ops.push(op);

    let resp = rpc_exec(&client, &req)?;

    if !resp.ok {
        let msg = format!("Batch failure {}: {}", resp.err_code, resp.err_message);
        return Err(Error::new(ErrorKind::Other, msg));
    }

    let results = resp.get_results();
    assert!(results.len() == 1);

    let result = &results[0];
    if result.ok {
        let get_res = results[0].get_get();
        let value_length = get_res.value_length;
        println!("{}", value_length);
        Ok(())
    } else {
        let msg = format!("{}: {}", key, result.err_message);
        Err(Error::new(ErrorKind::Other, msg))
    }
}

pub mod args {
    use clap::{App, Arg, SubCommand};

    pub fn append() -> App<'static, 'static> {
        SubCommand::with_name("append")
            .about("String.Append: Append to item")
            .arg(
                Arg::with_name("key")
                    .help("Key of item to store")
                    .required(true),
            )
            .arg(
                Arg::with_name("value")
                    .help("Value of item to append")
                    .required(true),
            )
    }

    pub fn decr() -> App<'static, 'static> {
        SubCommand::with_name("decr")
            .about("String.Decr: Decrement numeric item by 1")
            .arg(
                Arg::with_name("key")
                    .help("Key of item to update")
                    .required(true),
            )
    }

    pub fn decrby() -> App<'static, 'static> {
        SubCommand::with_name("decrby")
            .about("String.DecrBy: Decrement numeric item")
            .arg(
                Arg::with_name("key")
                    .help("Key of item to update")
                    .required(true),
            )
            .arg(
                Arg::with_name("n")
                    .help("Numeric delta for operation (default: 1, if invalid number provided)")
                    .required(true),
            )
    }

    pub fn get() -> App<'static, 'static> {
        SubCommand::with_name("get")
            .about("String.Get: Retrieve item")
            .arg(
                Arg::with_name("key")
                    .help("Key of item to retrieve")
                    .required(true),
            )
    }

    pub fn getset() -> App<'static, 'static> {
        SubCommand::with_name("getset")
            .about("String.GetSet: Store item, return old value")
            .arg(
                Arg::with_name("key")
                    .help("Key of item to retrieve+store")
                    .required(true),
            )
            .arg(
                Arg::with_name("value")
                    .help("Value of item to store")
                    .required(true),
            )
    }

    pub fn incr() -> App<'static, 'static> {
        SubCommand::with_name("incr")
            .about("String.Incr: Increment numeric item by 1")
            .arg(
                Arg::with_name("key")
                    .help("Key of item to update")
                    .required(true),
            )
    }

    pub fn incrby() -> App<'static, 'static> {
        SubCommand::with_name("incrby")
            .about("String.IncrBy: Increment numeric item")
            .arg(
                Arg::with_name("key")
                    .help("Key of item to update")
                    .required(true),
            )
            .arg(
                Arg::with_name("n")
                    .help("Numeric delta for operation (default: 1, if invalid number provided)")
                    .required(true),
            )
    }

    pub fn set() -> App<'static, 'static> {
        SubCommand::with_name("set")
            .about("String.Set: Store item")
            .arg(
                Arg::with_name("key")
                    .help("Key of item to store")
                    .required(true),
            )
            .arg(
                Arg::with_name("value")
                    .help("Value of item to store")
                    .required(true),
            )
    }

    pub fn strlen() -> App<'static, 'static> {
        SubCommand::with_name("strlen")
            .about("String.Strlen: Retrieve item length")
            .arg(
                Arg::with_name("key")
                    .help("Key of item whose length shall be retrieved")
                    .required(true),
            )
    }
}
