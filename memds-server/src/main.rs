extern crate futures;
extern crate grpcio;
extern crate memds_proto;
#[macro_use]
extern crate log;

use std::collections::HashMap;
use std::io::Read;
use std::sync::Arc;
use std::sync::Mutex;
use std::{io, thread};

use futures::sync::oneshot;
use futures::Future;
use grpcio::{Environment, RpcContext, ServerBuilder, UnarySink};

use memds_proto::memds_api::{OpResult, OpType, RequestMsg, ResponseMsg};
use memds_proto::memds_api_grpc::{self, Memds};
use memds_proto::util::result_err;

mod string;

/// The in-memory database shared amongst all clients.
///
/// This database will be shared via `Arc`, so to mutate the internal map we're
/// going to use a `Mutex` for interior mutability.
#[derive(Clone)]
struct MemdsService {
    map: Arc<Mutex<HashMap<Vec<u8>, Vec<u8>>>>,
}

impl Memds for MemdsService {
    fn exec(&mut self, ctx: RpcContext, msg_req: RequestMsg, sink: UnarySink<ResponseMsg>) {
        let mut out_resp = ResponseMsg::new();
        out_resp.ok = true;

        // lock db
        let mut db = self.map.lock().unwrap();

        // handle requests
        let ops = msg_req.get_ops();
        for op in ops.iter() {
            match op.otype {
                OpType::STR_GET => {
                    if !op.has_get() {
                        out_resp.results.push(result_err(-400, "Invalid op"));
                        continue;
                    }
                    let get_req = op.get_get();
                    let op_res = string::get(&mut db, get_req);
                    out_resp.results.push(op_res);
                }

                OpType::STR_SET | OpType::STR_APPEND => {
                    if !op.has_set() {
                        out_resp.results.push(result_err(-400, "Invalid op"));
                        continue;
                    }

                    let set_req = op.get_set();
                    let op_res = {
                        if op.otype == OpType::STR_SET {
                            string::set(&mut db, set_req)
                        } else {
                            string::append(&mut db, set_req)
                        }
                    };
                    out_resp.results.push(op_res);
                }

                OpType::STR_DECR | OpType::STR_DECRBY | OpType::STR_INCR | OpType::STR_INCRBY => {
                    if !op.has_num() {
                        out_resp.results.push(result_err(-400, "Invalid op"));
                        continue;
                    }

                    let num_req = op.get_num();
                    let op_res = string::incrdecr(&mut db, op.otype, num_req);
                    out_resp.results.push(op_res);
                }

                _ => {
                    let mut res = OpResult::new();
                    res.ok = false;
                    res.err_code = -400;
                    res.err_message = String::from("Invalid op");
                    out_resp.results.push(res);
                }
            }
        }

        let f = sink
            .success(out_resp)
            .map_err(|e| error!("exec req failed: {:?}", e));
        ctx.spawn(f)
    }
}

fn main() {
    let env = Arc::new(Environment::new(1));

    let mut initial_db = HashMap::new();
    initial_db.insert(b"foo".to_vec(), b"bar".to_vec());

    let service = memds_api_grpc::create_memds(MemdsService {
        map: Arc::new(Mutex::new(initial_db)),
    });
    let mut server = ServerBuilder::new(env)
        .register_service(service)
        .bind("127.0.0.1", memds_proto::DEF_PORT)
        .build()
        .unwrap();
    server.start();
    for &(ref host, port) in server.bind_addrs() {
        println!("listening on {}:{}", host, port);
    }
    let (tx, rx) = oneshot::channel();
    thread::spawn(move || {
        println!("Press ENTER to exit...");
        let _ = io::stdin().read(&mut [0]).unwrap();
        tx.send(())
    });
    let _ = rx.wait();
    let _ = server.shutdown().wait();
}
