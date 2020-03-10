extern crate clap;

use clap::value_t;

use grpcio::*;
use memds_proto::memds_api::OpType;
use memds_proto::memds_api_grpc::MemdsClient;
use std::io;
use std::sync::Arc;

mod keys;
mod list;
mod server;
mod set;
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
        .subcommand(keys::args::rename())
        .subcommand(keys::args::renamenx())
        .subcommand(keys::args::typ())
        .subcommand(list::args::lindex())
        .subcommand(list::args::llen())
        .subcommand(list::args::lpop())
        .subcommand(list::args::lpush())
        .subcommand(list::args::lpushx())
        .subcommand(list::args::rpop())
        .subcommand(list::args::rpush())
        .subcommand(list::args::rpushx())
        .subcommand(server::args::dbsize())
        .subcommand(server::args::flushall())
        .subcommand(server::args::flushdb())
        .subcommand(server::args::time())
        .subcommand(set::args::sadd())
        .subcommand(set::args::scard())
        .subcommand(set::args::sdiff())
        .subcommand(set::args::sdiffstore())
        .subcommand(set::args::sinter())
        .subcommand(set::args::sinterstore())
        .subcommand(set::args::sismember())
        .subcommand(set::args::smembers())
        .subcommand(set::args::srem())
        .subcommand(set::args::sunion())
        .subcommand(set::args::sunionstore())
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
        ("dbsize", Some(_matches)) => server::dbsize(&client),
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
        ("flushall", Some(_matches)) => server::flush(&client, true),
        ("flushdb", Some(_matches)) => server::flush(&client, false),
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
        ("rename", Some(matches)) => {
            let old_key = matches.value_of("old_key").unwrap();
            let new_key = matches.value_of("new_key").unwrap();
            keys::rename(&client, old_key, new_key, false)
        }
        ("renamenx", Some(matches)) => {
            let old_key = matches.value_of("old_key").unwrap();
            let new_key = matches.value_of("new_key").unwrap();
            keys::rename(&client, old_key, new_key, true)
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
        ("sadd", Some(matches)) => {
            let key = matches.value_of("key").unwrap();
            let elems: Vec<_> = matches.values_of("element").unwrap().collect();
            set::add_del(&client, key, &elems, false)
        }
        ("scard", Some(matches)) => {
            let key = matches.value_of("key").unwrap();
            set::info(&client, key)
        }
        ("sdiff", Some(matches)) => {
            let key1 = matches.value_of("key1").unwrap();
            let mut keys: Vec<_> = matches.values_of("keys").unwrap().collect();
            keys.insert(0, key1);
            let empty = String::from("");
            set::cmpstore(&client, &keys, &empty, OpType::SET_DIFF)
        }
        ("sdiffstore", Some(matches)) => {
            let store_key = matches.value_of("destination").unwrap();
            let key1 = matches.value_of("key1").unwrap();
            let mut keys: Vec<_> = matches.values_of("keys").unwrap().collect();
            keys.insert(0, key1);
            set::cmpstore(&client, &keys, &store_key, OpType::SET_DIFF)
        }
        ("sinter", Some(matches)) => {
            let key1 = matches.value_of("key1").unwrap();
            let mut keys: Vec<_> = matches.values_of("keys").unwrap().collect();
            keys.insert(0, key1);
            let empty = String::from("");
            set::cmpstore(&client, &keys, &empty, OpType::SET_INTERSECT)
        }
        ("sinterstore", Some(matches)) => {
            let store_key = matches.value_of("destination").unwrap();
            let key1 = matches.value_of("key1").unwrap();
            let mut keys: Vec<_> = matches.values_of("keys").unwrap().collect();
            keys.insert(0, key1);
            set::cmpstore(&client, &keys, &store_key, OpType::SET_INTERSECT)
        }
        ("sismember", Some(matches)) => {
            let key = matches.value_of("key").unwrap();
            let elems: Vec<_> = matches.values_of("element").unwrap().collect();
            set::is_member(&client, key, &elems)
        }
        ("smembers", Some(matches)) => {
            let key = matches.value_of("key").unwrap();
            set::members(&client, key)
        }
        ("srem", Some(matches)) => {
            let key = matches.value_of("key").unwrap();
            let elems: Vec<_> = matches.values_of("element").unwrap().collect();
            set::add_del(&client, key, &elems, true)
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
        ("sunion", Some(matches)) => {
            let key1 = matches.value_of("key1").unwrap();
            let mut keys: Vec<_> = matches.values_of("keys").unwrap().collect();
            keys.insert(0, key1);
            let empty = String::from("");
            set::cmpstore(&client, &keys, &empty, OpType::SET_UNION)
        }
        ("sunionstore", Some(matches)) => {
            let store_key = matches.value_of("destination").unwrap();
            let key1 = matches.value_of("key1").unwrap();
            let mut keys: Vec<_> = matches.values_of("keys").unwrap().collect();
            keys.insert(0, key1);
            set::cmpstore(&client, &keys, &store_key, OpType::SET_UNION)
        }
        ("time", Some(_matches)) => server::time(&client),
        ("type", Some(matches)) => {
            let key = matches.value_of("key").unwrap();
            keys::typ(&client, key)
        }
        ("", None) => {
            println!("No subcommand specified.  Run with --help for help.");
            Ok(())
        }
        _ => unreachable!(),
    }
}
