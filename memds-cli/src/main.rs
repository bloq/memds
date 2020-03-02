extern crate clap;

use clap::{Arg, SubCommand};

use futures::Future;
use grpcio::*;
use memds_proto::memds_api::*;
use memds_proto::memds_api_grpc::MemdsClient;
use std::io::{self, Error, ErrorKind, Write};
use std::sync::Arc;

const APPNAME: &'static str = "memds-cli";
const VERSION: &'static str = env!("CARGO_PKG_VERSION");
const DEF_BIND_HOST: &'static str = "127.0.0.1";

fn rpc_exec(client: &MemdsClient, req: &RequestMsg) -> io::Result<ResponseMsg> {
    let exec = client.exec_async(&req).unwrap();
    match exec.wait() {
        Err(e) => panic!("RPC.Exec failed: {:?}", e),
        Ok(resp) => Ok(resp),
    }
}

fn main() -> io::Result<()> {
    // parse command line
    let cli_matches = clap::App::new(APPNAME)
        .version(VERSION)
        .about("Memds CLI")
        .subcommand(
            SubCommand::with_name("decr")
                .about("String.Decr: Decrement numeric item by 1")
                .arg(
                    Arg::with_name("key")
                        .help("Key of item to update")
                        .required(true),
                ),
        )
        .subcommand(
            SubCommand::with_name("decrby")
                .about("String.DecrBy: Decrement numeric item")
                .arg(
                    Arg::with_name("key")
                        .help("Key of item to update")
                        .required(true),
                )
                .arg(
                    Arg::with_name("n")
                        .help("Numeric delta for operation")
                        .required(true),
                ),
        )
        .subcommand(
            SubCommand::with_name("incr")
                .about("String.IncrBy: Increment numeric item by 1")
                .arg(
                    Arg::with_name("key")
                        .help("Key of item to update")
                        .required(true),
                ),
        )
        .subcommand(
            SubCommand::with_name("incrby")
                .about("String.Incr: Increment numeric item")
                .arg(
                    Arg::with_name("key")
                        .help("Key of item to update")
                        .required(true),
                )
                .arg(
                    Arg::with_name("n")
                        .help("Numeric delta for operation")
                        .required(true),
                ),
        )
        .subcommand(
            SubCommand::with_name("get")
                .about("String.Get: Retrieve item")
                .arg(
                    Arg::with_name("key")
                        .help("Key of item to retrieve")
                        .required(true),
                ),
        )
        .subcommand(
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
                ),
        )
        .subcommand(
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
                ),
        )
        .get_matches();

    let endpoint = format!("{}:{}", DEF_BIND_HOST, memds_proto::DEF_PORT);

    let env = Arc::new(Environment::new(2));
    let channel = ChannelBuilder::new(env).connect(&endpoint);
    let client = MemdsClient::new(channel);

    match cli_matches.subcommand() {
        ("decr", Some(matches)) => {
            let key = matches.value_of("key").unwrap();
            println!("ACTION: str.decr {}", key);
        }
        ("decrby", Some(matches)) => {
            let key = matches.value_of("key").unwrap();
            let n = matches.value_of("n").unwrap();
            println!("ACTION: str.decrby {} {}", key, n);
        }
        ("incr", Some(matches)) => {
            let key = matches.value_of("key").unwrap();
            println!("ACTION: str.incr {}", key);
        }
        ("incrby", Some(matches)) => {
            let key = matches.value_of("key").unwrap();
            let n = matches.value_of("n").unwrap();
            println!("ACTION: str.incrby {} {}", key, n);
        }
        ("get", Some(matches)) => {
            let key = matches.value_of("key").unwrap();

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
            } else {
                println!("get failed {}: {}", result.err_code, result.err_message);
            }
        }
        ("getset", Some(matches)) => {
            let key = matches.value_of("key").unwrap();
            let value = matches.value_of("value").unwrap();
            println!("ACTION: str.getset {}={}", key, value);
        }
        ("set", Some(matches)) => {
            let key = matches.value_of("key").unwrap();
            let value = matches.value_of("value").unwrap();
            println!("ACTION: str.set {}={}", key, value);
        }
        ("", None) => println!("No subcommand specified.  Run with --help for help."),
        _ => unreachable!(),
    }

    Ok(())
}
