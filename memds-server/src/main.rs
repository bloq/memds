#![warn(rust_2018_idioms)]

use tokio::net::TcpListener;
use tokio::stream::StreamExt;
use tokio_util::codec::Framed;

use futures::SinkExt;
use std::collections::HashMap;
use std::env;
use std::error::Error;
use std::sync::{Arc, Mutex};

mod op;

use memds_proto::util::{resp_err, result_err};
use memds_proto::{MemdsCodec, MemdsMessage, MemdsMessage_MsgType, OpResult, OpType, ResponseMsg};

/// The in-memory database shared amongst all clients.
///
/// This database will be shared via `Arc`, so to mutate the internal map we're
/// going to use a `Mutex` for interior mutability.
struct Database {
    map: Mutex<HashMap<Vec<u8>, Vec<u8>>>,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    // Parse the address we're going to run this server on
    // and set up our TCP listener to accept connections.
    let addr = env::args()
        .nth(1)
        .unwrap_or_else(|| "127.0.0.1:8080".to_string());

    let mut listener = TcpListener::bind(&addr).await?;
    println!("Listening on: {}", addr);

    // Create the shared state of this server that will be shared amongst all
    // clients. We populate the initial database and then create the `Database`
    // structure. Note the usage of `Arc` here which will be used to ensure that
    // each independently spawned client will have a reference to the in-memory
    // database.
    let mut initial_db = HashMap::new();
    initial_db.insert(b"foo".to_vec(), b"bar".to_vec());
    let db = Arc::new(Database {
        map: Mutex::new(initial_db),
    });

    loop {
        match listener.accept().await {
            Ok((socket, _)) => {
                // After getting a new connection first we see a clone of the database
                // being created, which is creating a new reference for this connected
                // client to use.
                let db = db.clone();

                // Like with other small servers, we'll `spawn` this client to ensure it
                // runs concurrently with all other clients. The `move` keyword is used
                // here to move ownership of our db handle into the async closure.
                tokio::spawn(async move {
                    // Since our protocol is line-based we use `tokio_codecs`'s `LineCodec`
                    // to convert our stream of bytes, `socket`, into a `Stream` of lines
                    // as well as convert our line based responses into a stream of bytes.
                    let mut msgs = Framed::new(socket, MemdsCodec::new());

                    // Here for every line we get back from the `Framed` decoder,
                    // we parse the request, and if it's valid we generate a response
                    // based on the values in the database.
                    while let Some(result) = msgs.next().await {
                        match result {
                            Ok(msg) => {
                                let response = handle_request(&msg, &db);

                                if let Err(e) = msgs.send(response).await {
                                    println!("error on sending response; error = {:?}", e);
                                }
                            }
                            Err(e) => {
                                println!("error on decoding from socket; error = {:?}", e);
                            }
                        }
                    }

                    // The connection will be closed at this point as `msgs.next()` has returned `None`.
                });
            }
            Err(e) => println!("error accepting socket; error = {:?}", e),
        }
    }
}

fn handle_request(msg: &MemdsMessage, db: &Arc<Database>) -> MemdsMessage {
    // pre-db-lock checks
    if msg.mtype != MemdsMessage_MsgType::REQ || !msg.has_req() || msg.has_resp() {
        return resp_err(-400, "REQ required");
    }

    let mut out_resp = ResponseMsg::new();

    // lock db
    let mut db = db.map.lock().unwrap();

    // handle requests
    let msg_req = msg.get_req();
    let ops = msg_req.get_ops();
    for op in ops.iter() {
        match op.otype {
            OpType::STR_GET => {
                if !op.has_get() {
                    out_resp.results.push(result_err(-400, "Invalid op"));
                    continue;
                }
                let get_req = op.get_get();
                out_resp.results.push(op::string::get(&mut db, get_req));
            }

            OpType::STR_SET => {
                if !op.has_set() {
                    out_resp.results.push(result_err(-400, "Invalid op"));
                    continue;
                }

                let set_req = op.get_set();
                out_resp.results.push(op::string::set(&mut db, set_req));
            }

            OpType::DECR | OpType::DECRBY | OpType::INCR | OpType::INCRBY => {
                if !op.has_num() {
                    out_resp.results.push(result_err(-400, "Invalid op"));
                    continue;
                }

                let num_req = op.get_num();
                let op_res = op::string::incrdecr(&mut db, op.otype, num_req);
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

    let mut out_msg = MemdsMessage::new();
    out_msg.set_resp(out_resp);

    out_msg
}
