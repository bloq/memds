use std::collections::HashMap;
use std::sync::Arc;
use std::sync::Mutex;

use futures::Future;
use grpcio::{RpcContext, UnarySink};

use memds_proto::memds_api::{OpResult, OpType, RequestMsg, ResponseMsg};
use memds_proto::memds_api_grpc::Memds;
use memds_proto::util::result_err;
use memds_proto::Atom;

use crate::keys;
use crate::list;
use crate::server;
use crate::set;
use crate::string;

/// The in-memory database shared amongst all clients.
///
/// This database will be shared via `Arc`, so to mutate the internal map we're
/// going to use a `Mutex` for interior mutability.

#[derive(Clone)]
pub struct MemdsService {
    pub map: Arc<Mutex<HashMap<Vec<u8>, Atom>>>,
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
                OpType::KEY_DUMP => {
                    if !op.has_key() {
                        out_resp.results.push(result_err(-400, "Invalid op"));
                        continue;
                    }
                    let op_req = op.get_key();
                    let op_res = keys::dump(&mut db, op_req);
                    out_resp.results.push(op_res);
                }

                OpType::KEY_RESTORE => {
                    if !op.has_set() {
                        out_resp.results.push(result_err(-400, "Invalid op"));
                        continue;
                    }

                    let op_req = op.get_set();
                    let op_res = keys::restore(&mut db, op_req);
                    out_resp.results.push(op_res);
                }

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

                OpType::SET_DIFF | OpType::SET_UNION | OpType::SET_INTERSECT => {
                    if !op.has_cmp_stor() {
                        out_resp.results.push(result_err(-400, "Invalid op"));
                        continue;
                    }
                    let op_req = op.get_cmp_stor();
                    let op_res = match op.otype {
                        OpType::SET_DIFF => set::diff(&mut db, op_req),
                        OpType::SET_UNION => set::union(&mut db, op_req),
                        OpType::SET_INTERSECT => set::intersect(&mut db, op_req),
                        _ => unreachable!(),
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

                OpType::SET_MOVE => {
                    if !op.has_set_move() {
                        out_resp.results.push(result_err(-400, "Invalid op"));
                        continue;
                    }
                    let op_req = op.get_set_move();
                    let op_res = set::mov(&mut db, op_req);
                    out_resp.results.push(op_res);
                }

                OpType::SRV_BGSAVE => {
                    let op_res = server::bgsave(&mut db);
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
