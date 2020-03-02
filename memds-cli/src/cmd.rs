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

pub fn set(client: &MemdsClient, key: &str, value: &str, append: bool) -> io::Result<()> {
    let mut set_req = StrSetOp::new();
    set_req.set_key(key.as_bytes().to_vec());
    set_req.set_value(value.as_bytes().to_vec());

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
        io::stdout().write_all(b"ok\n")?;
        Ok(())
    } else {
        let msg = format!("{}: {}", key, result.err_message);
        Err(Error::new(ErrorKind::Other, msg))
    }
}
