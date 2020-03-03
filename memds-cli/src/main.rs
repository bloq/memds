extern crate clap;

use clap::value_t;

use grpcio::*;
use memds_proto::memds_api::OpType;
use memds_proto::memds_api_grpc::MemdsClient;
use std::io;
use std::sync::Arc;

mod list;
mod string;
mod util;

const APPNAME: &'static str = "memds-cli";
const VERSION: &'static str = env!("CARGO_PKG_VERSION");
const DEF_BIND_HOST: &'static str = "127.0.0.1";

fn main() -> io::Result<()> {
    // parse command line
    let cli_matches = clap::App::new(APPNAME)
        .version(VERSION)
        .about("Memds CLI")
        .subcommand(string::args::append())
        .subcommand(string::args::decr())
        .subcommand(string::args::decrby())
        .subcommand(string::args::get())
        .subcommand(string::args::getset())
        .subcommand(string::args::incr())
        .subcommand(string::args::incrby())
        .subcommand(string::args::set())
        .subcommand(string::args::strlen())
        .subcommand(list::args::lindex())
        .subcommand(list::args::llen())
        .subcommand(list::args::rpush())
        .get_matches();

    let endpoint = format!("{}:{}", DEF_BIND_HOST, memds_proto::DEF_PORT);

    let env = Arc::new(Environment::new(2));
    let channel = ChannelBuilder::new(env).connect(&endpoint);
    let client = MemdsClient::new(channel);

    match cli_matches.subcommand() {
        ("append", Some(matches)) => {
            let key = matches.value_of("key").unwrap();
            let value = matches.value_of("value").unwrap();
            string::set(&client, key, value, false, true)
        }
        ("decr", Some(matches)) => {
            let key = matches.value_of("key").unwrap();
            string::incrdecr(&client, OpType::STR_DECR, key, 1)
        }
        ("decrby", Some(matches)) => {
            let key = matches.value_of("key").unwrap();
            let n = value_t!(matches, "n", i64).unwrap_or(1);
            string::incrdecr(&client, OpType::STR_DECRBY, key, n)
        }
        ("incr", Some(matches)) => {
            let key = matches.value_of("key").unwrap();
            string::incrdecr(&client, OpType::STR_INCR, key, 1)
        }
        ("incrby", Some(matches)) => {
            let key = matches.value_of("key").unwrap();
            let n = value_t!(matches, "n", i64).unwrap_or(1);
            string::incrdecr(&client, OpType::STR_INCRBY, key, n)
        }
        ("get", Some(matches)) => {
            let key = matches.value_of("key").unwrap();
            string::get(&client, key)
        }
        ("getset", Some(matches)) => {
            let key = matches.value_of("key").unwrap();
            let value = matches.value_of("value").unwrap();
            string::set(&client, key, value, true, false)
        }
        ("lindex", Some(matches)) => {
            let key = matches.value_of("key").unwrap();
            let n = value_t!(matches, "index", i32).unwrap();
            list::lindex(&client, key, n)
        }
        ("llen", Some(matches)) => {
            let key = matches.value_of("key").unwrap();
            list::llen(&client, key)
        }
        ("rpush", Some(matches)) => {
            let key = matches.value_of("key").unwrap();
            let elem = matches.value_of("element").unwrap();
            list::push(&client, key, elem, false)
        }
        ("set", Some(matches)) => {
            let key = matches.value_of("key").unwrap();
            let value = matches.value_of("value").unwrap();
            string::set(&client, key, value, false, false)
        }
        ("strlen", Some(matches)) => {
            let key = matches.value_of("key").unwrap();
            string::strlen(&client, key)
        }
        ("", None) => {
            println!("No subcommand specified.  Run with --help for help.");
            Ok(())
        }
        _ => unreachable!(),
    }
}
