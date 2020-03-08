extern crate clap;
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

use clap::value_t;
use futures::sync::oneshot;
use futures::Future;
use grpcio::{Environment, RpcContext, ServerBuilder, UnarySink};

use memds_proto::memds_api::{OpResult, OpType, RequestMsg, ResponseMsg};
use memds_proto::memds_api_grpc::{self, Memds};
use memds_proto::util::result_err;
use memds_proto::Atom;

mod keys;
mod list;
mod server;
mod set;
mod string;

const APPNAME: &'static str = "memds-server";
const VERSION: &'static str = env!("CARGO_PKG_VERSION");
const DEF_BIND_ADDR: &'static str = "127.0.0.1";

/// The in-memory database shared amongst all clients.
///
/// This database will be shared via `Arc`, so to mutate the internal map we're
/// going to use a `Mutex` for interior mutability.

#[derive(Clone)]
struct MemdsService {
    map: Arc<Mutex<HashMap<Vec<u8>, Atom>>>,
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
                OpType::KEYS_DEL | OpType::KEYS_EXIST => {
                    if !op.has_key_list() {
                        out_resp.results.push(result_err(-400, "Invalid op"));
                        continue;
                    }
                    let keys_req = op.get_key_list();
                    let remove_it = op.otype == OpType::KEYS_DEL;
                    let op_res = keys::del_exist(&mut db, keys_req, remove_it);
                    out_resp.results.push(op_res);
                }

                OpType::KEYS_RENAME => {
                    if !op.has_rename() {
                        out_resp.results.push(result_err(-400, "Invalid op"));
                        continue;
                    }
                    let rn_req = op.get_rename();
                    let op_res = keys::rename(&mut db, rn_req);
                    out_resp.results.push(op_res);
                }

                OpType::KEYS_TYPE => {
                    if !op.has_key() {
                        out_resp.results.push(result_err(-400, "Invalid op"));
                        continue;
                    }
                    let key_req = op.get_key();
                    let op_res = keys::typ(&mut db, key_req);
                    out_resp.results.push(op_res);
                }

                OpType::SET_ADD | OpType::SET_DEL | OpType::SET_ISMEMBER => {
                    if !op.has_keyed_list() {
                        out_resp.results.push(result_err(-400, "Invalid op"));
                        continue;
                    }
                    let op_req = op.get_keyed_list();
                    let op_res = {
                        if op.otype == OpType::SET_ISMEMBER {
                            set::is_member(&mut db, op_req)
                        } else {
                            set::add_del(&mut db, op_req, op.otype)
                        }
                    };
                    out_resp.results.push(op_res);
                }

                OpType::SET_INFO | OpType::SET_MEMBERS => {
                    if !op.has_key() {
                        out_resp.results.push(result_err(-400, "Invalid op"));
                        continue;
                    }
                    let op_req = op.get_key();
                    let op_res = {
                        if op.otype == OpType::SET_INFO {
                            set::info(&mut db, op_req)
                        } else {
                            set::members(&mut db, op_req)
                        }
                    };
                    out_resp.results.push(op_res);
                }

                OpType::SRV_DBSIZE => {
                    let op_res = server::dbsize(&mut db);
                    out_resp.results.push(op_res);
                }

                OpType::SRV_FLUSHDB | OpType::SRV_FLUSHALL => {
                    let op_res = server::flush(&mut db, op.otype);
                    out_resp.results.push(op_res);
                }

                OpType::SRV_TIME => {
                    let op_res = server::time();
                    out_resp.results.push(op_res);
                }

                OpType::STR_GET | OpType::STR_GETRANGE => {
                    if !op.has_get() {
                        out_resp.results.push(result_err(-400, "Invalid op"));
                        continue;
                    }
                    let get_req = op.get_get();
                    let op_res = string::get(&mut db, get_req, op.otype);
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

                OpType::LIST_PUSH => {
                    if !op.has_lpush() {
                        out_resp.results.push(result_err(-400, "Invalid op"));
                        continue;
                    }

                    let lpush_req = op.get_lpush();
                    let op_res = list::push(&mut db, lpush_req);
                    out_resp.results.push(op_res);
                }

                OpType::LIST_POP => {
                    if !op.has_lpop() {
                        out_resp.results.push(result_err(-400, "Invalid op"));
                        continue;
                    }

                    let lpop_req = op.get_lpop();
                    let op_res = list::pop(&mut db, lpop_req);
                    out_resp.results.push(op_res);
                }

                OpType::LIST_INFO => {
                    if !op.has_key() {
                        out_resp.results.push(result_err(-400, "Invalid op"));
                        continue;
                    }

                    let key_req = op.get_key();
                    let op_res = list::info(&mut db, key_req);
                    out_resp.results.push(op_res);
                }

                OpType::LIST_INDEX => {
                    if !op.has_lindex() {
                        out_resp.results.push(result_err(-400, "Invalid op"));
                        continue;
                    }

                    let lindex_req = op.get_lindex();
                    let op_res = list::index(&mut db, lindex_req);
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

    // parse command line
    let cli_matches = clap::App::new(APPNAME)
        .version(VERSION)
        .about("Memory Database Service")
        .arg(
            clap::Arg::with_name("bind-addr")
                .short("a")
                .long("bind-addr")
                .value_name("IP-ADDRESS")
                .help(&format!("socket bind address (default: {})", DEF_BIND_ADDR))
                .takes_value(true),
        )
        .arg(
            clap::Arg::with_name("bind-port")
                .short("p")
                .long("bind-port")
                .value_name("PORT")
                .help(&format!(
                    "socket bind port (default: {})",
                    memds_proto::DEF_PORT
                ))
                .takes_value(true),
        )
        .get_matches();

    let bind_addr = cli_matches.value_of("bind-addr").unwrap_or(DEF_BIND_ADDR);
    let bind_port = value_t!(cli_matches, "bind-port", u16).unwrap_or(memds_proto::DEF_PORT);

    let mut initial_db = HashMap::new();
    initial_db.insert(b"foo".to_vec(), Atom::String(b"bar".to_vec()));

    let service = memds_api_grpc::create_memds(MemdsService {
        map: Arc::new(Mutex::new(initial_db)),
    });
    let mut server = ServerBuilder::new(env)
        .register_service(service)
        .bind(bind_addr, bind_port)
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
