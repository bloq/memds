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
use grpcio::{Environment, ServerBuilder};

use memds_proto::memds_api_grpc;
use memds_proto::Atom;

mod config;
mod keys;
mod list;
mod rpcservice;
mod server;
mod set;
mod string;

fn main() {
    let env = Arc::new(Environment::new(1));

    let cfg = config::get();

    let mut initial_db = HashMap::new();
    initial_db.insert(b"foo".to_vec(), Atom::String(b"bar".to_vec()));

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
    thread::spawn(move || {
        println!("Press ENTER to exit...");
        let _ = io::stdin().read(&mut [0]).unwrap();
        tx.send(())
    });
    let _ = rx.wait();
    let _ = server.shutdown().wait();
}
