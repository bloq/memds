extern crate clap;

use clap::value_t;

use grpcio::*;
use memds_proto::memds_api::OpType;
use memds_proto::memds_api_grpc::MemdsClient;
use std::io;
use std::sync::Arc;

mod keys;
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
        .subcommand(keys::args::del())
        .subcommand(keys::args::exists())
        .subcommand(list::args::lindex())
        .subcommand(list::args::llen())
        .subcommand(list::args::lpop())
        .subcommand(list::args::lpush())
        .subcommand(list::args::lpushx())
        .subcommand(list::args::rpop())
        .subcommand(list::args::rpush())
        .subcommand(list::args::rpushx())
        .subcommand(string::args::append())
        .subcommand(string::args::decr())
        .subcommand(string::args::decrby())
        .subcommand(string::args::get())
        .subcommand(string::args::getrange())
        .subcommand(string::args::getset())
        .subcommand(string::args::incr())
        .subcommand(string::args::incrby())
        .subcommand(string::args::set())
        .subcommand(string::args::setnx())
        .subcommand(string::args::strlen())
        .get_matches();

    let endpoint = format!("{}:{}", DEF_BIND_HOST, memds_proto::DEF_PORT);

    let env = Arc::new(Environment::new(2));
    let channel = ChannelBuilder::new(env).connect(&endpoint);
    let client = MemdsClient::new(channel);

    match cli_matches.subcommand() {
        ("append", Some(matches)) => {
            let key = matches.value_of("key").unwrap();
            let value = matches.value_of("value").unwrap();
            string::set(&client, key, value, false, true, false)
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
        ("del", Some(matches)) => {
            let keys: Vec<_> = matches.values_of("key").unwrap().collect();
            keys::del_exist(&client, &keys, true)
        }
        ("exists", Some(matches)) => {
            let keys: Vec<_> = matches.values_of("key").unwrap().collect();
            keys::del_exist(&client, &keys, false)
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
        ("getrange", Some(matches)) => {
            let key = matches.value_of("key").unwrap();
            let start = value_t!(matches, "start", i32).unwrap_or(0);
            let end = value_t!(matches, "end", i32).unwrap_or(0);
            string::getrange(&client, key, start, end)
        }
        ("getset", Some(matches)) => {
            let key = matches.value_of("key").unwrap();
            let value = matches.value_of("value").unwrap();
            string::set(&client, key, value, true, false, false)
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
        ("lpop", Some(matches)) => {
            let key = matches.value_of("key").unwrap();
            list::pop(&client, key, true)
        }
        ("lpush", Some(matches)) => {
            let key = matches.value_of("key").unwrap();
            let elems: Vec<_> = matches.values_of("element").unwrap().collect();
            list::push(&client, key, &elems, true, false)
        }
        ("lpushx", Some(matches)) => {
            let key = matches.value_of("key").unwrap();
            let elems: Vec<_> = matches.values_of("element").unwrap().collect();
            list::push(&client, key, &elems, true, true)
        }
        ("rpop", Some(matches)) => {
            let key = matches.value_of("key").unwrap();
            list::pop(&client, key, false)
        }
        ("rpush", Some(matches)) => {
            let key = matches.value_of("key").unwrap();
            let elems: Vec<_> = matches.values_of("element").unwrap().collect();
            list::push(&client, key, &elems, false, false)
        }
        ("rpushx", Some(matches)) => {
            let key = matches.value_of("key").unwrap();
            let elems: Vec<_> = matches.values_of("element").unwrap().collect();
            list::push(&client, key, &elems, false, true)
        }
        ("set", Some(matches)) => {
            let key = matches.value_of("key").unwrap();
            let value = matches.value_of("value").unwrap();
            string::set(&client, key, value, false, false, false)
        }
        ("setnx", Some(matches)) => {
            let key = matches.value_of("key").unwrap();
            let value = matches.value_of("value").unwrap();
            string::set(&client, key, value, false, false, true)
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
