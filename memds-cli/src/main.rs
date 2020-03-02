extern crate clap;

use clap::{value_t, Arg, SubCommand};

use grpcio::*;
use memds_proto::memds_api::OpType;
use memds_proto::memds_api_grpc::MemdsClient;
use std::io;
use std::sync::Arc;

mod cmd;

const APPNAME: &'static str = "memds-cli";
const VERSION: &'static str = env!("CARGO_PKG_VERSION");
const DEF_BIND_HOST: &'static str = "127.0.0.1";

fn main() -> io::Result<()> {
    // parse command line
    let cli_matches = clap::App::new(APPNAME)
        .version(VERSION)
        .about("Memds CLI")
        .subcommand(
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
                ),
        )
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
                        .help(
                            "Numeric delta for operation (default: 1, if invalid number provided)",
                        )
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
                        .help(
                            "Numeric delta for operation (default: 1, if invalid number provided)",
                        )
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
        ("append", Some(matches)) => {
            let key = matches.value_of("key").unwrap();
            let value = matches.value_of("value").unwrap();
            cmd::set(&client, key, value, false, true)
        }
        ("decr", Some(matches)) => {
            let key = matches.value_of("key").unwrap();
            cmd::incrdecr(&client, OpType::STR_DECR, key, 1)
        }
        ("decrby", Some(matches)) => {
            let key = matches.value_of("key").unwrap();
            let n = value_t!(matches, "n", i64).unwrap_or(1);
            cmd::incrdecr(&client, OpType::STR_DECRBY, key, n)
        }
        ("incr", Some(matches)) => {
            let key = matches.value_of("key").unwrap();
            cmd::incrdecr(&client, OpType::STR_INCR, key, 1)
        }
        ("incrby", Some(matches)) => {
            let key = matches.value_of("key").unwrap();
            let n = value_t!(matches, "n", i64).unwrap_or(1);
            cmd::incrdecr(&client, OpType::STR_INCRBY, key, n)
        }
        ("get", Some(matches)) => {
            let key = matches.value_of("key").unwrap();
            cmd::get(&client, key)
        }
        ("getset", Some(matches)) => {
            let key = matches.value_of("key").unwrap();
            let value = matches.value_of("value").unwrap();
            cmd::set(&client, key, value, true, false)
        }
        ("set", Some(matches)) => {
            let key = matches.value_of("key").unwrap();
            let value = matches.value_of("value").unwrap();
            cmd::set(&client, key, value, false, false)
        }
        ("", None) => {
            println!("No subcommand specified.  Run with --help for help.");
            Ok(())
        }
        _ => unreachable!(),
    }
}
