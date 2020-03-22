extern crate futures;
extern crate grpcio;
extern crate memds_proto;
#[macro_use]
extern crate log;

use std::collections::HashMap;
use std::fs::File;
use std::io::{self, Error, ErrorKind, Read};
use std::sync::Arc;
use std::sync::Mutex;

use bytes::BytesMut;
use futures::sync::oneshot;
use futures::Future;
use grpcio::{Environment, ServerBuilder};
use tokio_util::codec::Decoder;

use memds_proto::memds_api::MemdsMessage_MsgType;
use memds_proto::memds_api_grpc;
use memds_proto::{Atom, MemdsCodec};

mod config;
mod keys;
mod list;
mod rpcservice;
mod server;
mod set;
mod string;

fn init_db(cfg: &config::Config) -> io::Result<HashMap<Vec<u8>, Atom>> {
    let mut db = HashMap::new();

    // get filename; if missing, return success
    let import_fn = match &cfg.fs.import {
        None => {
            return Ok(db);
        }
        Some(n) => n,
    };

    // open file and set up import & decode
    let mut codec = MemdsCodec::new();
    let mut f = File::open(import_fn)?;
    let mut accum = BytesMut::with_capacity(4096);
    let mut buf = [0; 4096];

    // read each chunk of data from the file
    loop {
        // read next 4096-byte chunk
        let n_read = f.read(&mut buf)?;
        if n_read == 0 {
            // EOF
            break;
        }

        // append to accumulated buffer
        accum.extend_from_slice(&buf[0..n_read]);

        // for each decodable record...
        loop {
            // attempt to decode record from accumulated bytes
            match codec.decode(&mut accum) {
                // decode err
                Err(_) => {
                    return Err(Error::new(ErrorKind::Other, "protobuf decode"));
                }

                // incomplete record
                Ok(None) => break,

                // complete record; import it.
                Ok(Some(msg)) => {
                    // EOF record; return success
                    if msg.mtype == MemdsMessage_MsgType::END {
                        return Ok(db);

                    // require DBVAL records
                    } else if (msg.mtype != MemdsMessage_MsgType::DBVAL) || !msg.has_dbv() {
                        return Err(Error::new(ErrorKind::Other, "unexpected record type"));
                    }

                    // import record
                    if !keys::import_dbv(&mut db, None, msg.get_dbv()) {
                        return Err(Error::new(ErrorKind::Other, "record import failed"));
                    }
                }
            }
        }
    }

    Err(Error::new(
        ErrorKind::Other,
        "truncated file; EOF without terminator",
    ))
}

fn main() {
    let env = Arc::new(Environment::new(1));

    let cfg = config::get();

    let initial_db = init_db(&cfg).unwrap();

    let service = memds_api_grpc::create_memds(rpcservice::MemdsService {
        map: Arc::new(Mutex::new(initial_db)),
    });
    let mut server = ServerBuilder::new(env)
        .register_service(service)
        .bind(cfg.network.bind_addr, cfg.network.bind_port)
        .build()
        .unwrap();
    server.start();
    for &(ref host, port) in server.bind_addrs() {
        println!("listening on {}:{}", host, port);
    }
    let (tx, rx) = oneshot::channel();
    std::thread::spawn(move || {
        println!("Press ENTER to exit...");
        let _ = io::stdin().read(&mut [0]).unwrap();
        tx.send(())
    });
    let _ = rx.wait();
    let _ = server.shutdown().wait();
}
