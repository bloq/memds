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
        .subcommand(keys::args::dump())
        .subcommand(keys::args::exists())
        .subcommand(keys::args::rename())
        .subcommand(keys::args::renamenx())
        .subcommand(keys::args::restore())
        .subcommand(keys::args::typ())
        .subcommand(list::args::lindex())
        .subcommand(list::args::llen())
        .subcommand(list::args::lpop())
        .subcommand(list::args::lpush())
        .subcommand(list::args::lpushx())
        .subcommand(list::args::rpop())
        .subcommand(list::args::rpush())
        .subcommand(list::args::rpushx())
        .subcommand(server::args::bgsave())
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
        .subcommand(set::args::smove())
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
        Some(("append", matches)) => {
            let key = matches.value_of("key").unwrap();
            let value = matches.value_of("value").unwrap();
            string::set(&client, key, value, false, true, false)
        }
        Some(("bgsave", _matches)) => server::bgsave(&client),
        Some(("dbsize", _matches)) => server::dbsize(&client),
        Some(("decr", matches)) => {
            let key = matches.value_of("key").unwrap();
            string::incrdecr(&client, OpType::STR_DECR, key, 1)
        }
        Some(("decrby", matches)) => {
            let key = matches.value_of("key").unwrap();
            let n = value_t!(matches, "n", i64).unwrap_or(1);
            string::incrdecr(&client, OpType::STR_DECRBY, key, n)
        }
        Some(("del", matches)) => {
            let keys: Vec<_> = matches.values_of("key").unwrap().collect();
            keys::del_exist(&client, &keys, true)
        }
        Some(("dump", matches)) => {
            let key = matches.value_of("key").unwrap();
            keys::dump(&client, key)
        }
        Some(("exists", matches)) => {
            let keys: Vec<_> = matches.values_of("key").unwrap().collect();
            keys::del_exist(&client, &keys, false)
        }
        Some(("flushall", _matches)) => server::flush(&client, true),
        Some(("flushdb", _matches)) => server::flush(&client, false),
        Some(("incr", matches)) => {
            let key = matches.value_of("key").unwrap();
            string::incrdecr(&client, OpType::STR_INCR, key, 1)
        }
        Some(("incrby", matches)) => {
            let key = matches.value_of("key").unwrap();
            let n = value_t!(matches, "n", i64).unwrap_or(1);
            string::incrdecr(&client, OpType::STR_INCRBY, key, n)
        }
        Some(("get", matches)) => {
            let key = matches.value_of("key").unwrap();
            string::get(&client, key)
        }
        Some(("getrange", matches)) => {
            let key = matches.value_of("key").unwrap();
            let start = value_t!(matches, "start", i32).unwrap_or(0);
            let end = value_t!(matches, "end", i32).unwrap_or(0);
            string::getrange(&client, key, start, end)
        }
        Some(("getset", matches)) => {
            let key = matches.value_of("key").unwrap();
            let value = matches.value_of("value").unwrap();
            string::set(&client, key, value, true, false, false)
        }
        Some(("lindex", matches)) => {
            let key = matches.value_of("key").unwrap();
            let n = value_t!(matches, "index", i32).unwrap();
            list::lindex(&client, key, n)
        }
        Some(("llen", matches)) => {
            let key = matches.value_of("key").unwrap();
            list::llen(&client, key)
        }
        Some(("lpop", matches)) => {
            let key = matches.value_of("key").unwrap();
            list::pop(&client, key, true)
        }
        Some(("lpush", matches)) => {
            let key = matches.value_of("key").unwrap();
            let elems: Vec<_> = matches.values_of("element").unwrap().collect();
            list::push(&client, key, &elems, true, false)
        }
        Some(("lpushx", matches)) => {
            let key = matches.value_of("key").unwrap();
            let elems: Vec<_> = matches.values_of("element").unwrap().collect();
            list::push(&client, key, &elems, true, true)
        }
        Some(("rename", matches)) => {
            let old_key = matches.value_of("old_key").unwrap();
            let new_key = matches.value_of("new_key").unwrap();
            keys::rename(&client, old_key, new_key, false)
        }
        Some(("renamenx", matches)) => {
            let old_key = matches.value_of("old_key").unwrap();
            let new_key = matches.value_of("new_key").unwrap();
            keys::rename(&client, old_key, new_key, true)
        }
        Some(("restore", matches)) => {
            let key = matches.value_of("key");
            let restore_fn = matches.value_of("file").unwrap();
            keys::restore(&client, key, restore_fn)
        }
        Some(("rpop", matches)) => {
            let key = matches.value_of("key").unwrap();
            list::pop(&client, key, false)
        }
        Some(("rpush", matches)) => {
            let key = matches.value_of("key").unwrap();
            let elems: Vec<_> = matches.values_of("element").unwrap().collect();
            list::push(&client, key, &elems, false, false)
        }
        Some(("rpushx", matches)) => {
            let key = matches.value_of("key").unwrap();
            let elems: Vec<_> = matches.values_of("element").unwrap().collect();
            list::push(&client, key, &elems, false, true)
        }
        Some(("sadd", matches)) => {
            let key = matches.value_of("key").unwrap();
            let elems: Vec<_> = matches.values_of("element").unwrap().collect();
            set::add_del(&client, key, &elems, false)
        }
        Some(("scard", matches)) => {
            let key = matches.value_of("key").unwrap();
            set::info(&client, key)
        }
        Some(("sdiff", matches)) => {
            let key1 = matches.value_of("key1").unwrap();
            let mut keys: Vec<_> = matches.values_of("keys").unwrap().collect();
            keys.insert(0, key1);
            let empty = String::from("");
            set::cmpstore(&client, &keys, &empty, OpType::SET_DIFF)
        }
        Some(("sdiffstore", matches)) => {
            let store_key = matches.value_of("destination").unwrap();
            let key1 = matches.value_of("key1").unwrap();
            let mut keys: Vec<_> = matches.values_of("keys").unwrap().collect();
            keys.insert(0, key1);
            set::cmpstore(&client, &keys, &store_key, OpType::SET_DIFF)
        }
        Some(("sinter", matches)) => {
            let key1 = matches.value_of("key1").unwrap();
            let mut keys: Vec<_> = matches.values_of("keys").unwrap().collect();
            keys.insert(0, key1);
            let empty = String::from("");
            set::cmpstore(&client, &keys, &empty, OpType::SET_INTERSECT)
        }
        Some(("sinterstore", matches)) => {
            let store_key = matches.value_of("destination").unwrap();
            let key1 = matches.value_of("key1").unwrap();
            let mut keys: Vec<_> = matches.values_of("keys").unwrap().collect();
            keys.insert(0, key1);
            set::cmpstore(&client, &keys, &store_key, OpType::SET_INTERSECT)
        }
        Some(("sismember", matches)) => {
            let key = matches.value_of("key").unwrap();
            let elems: Vec<_> = matches.values_of("element").unwrap().collect();
            set::is_member(&client, key, &elems)
        }
        Some(("smembers", matches)) => {
            let key = matches.value_of("key").unwrap();
            set::members(&client, key)
        }
        Some(("srem", matches)) => {
            let key = matches.value_of("key").unwrap();
            let elems: Vec<_> = matches.values_of("element").unwrap().collect();
            set::add_del(&client, key, &elems, true)
        }
        Some(("set", matches)) => {
            let key = matches.value_of("key").unwrap();
            let value = matches.value_of("value").unwrap();
            string::set(&client, key, value, false, false, false)
        }
        Some(("setnx", matches)) => {
            let key = matches.value_of("key").unwrap();
            let value = matches.value_of("value").unwrap();
            string::set(&client, key, value, false, false, true)
        }
        Some(("smove", matches)) => {
            let src_key = matches.value_of("src_key").unwrap();
            let dest_key = matches.value_of("dest_key").unwrap();
            let member = matches.value_of("member").unwrap();
            set::mov(&client, src_key, dest_key, member)
        }
        Some(("strlen", matches)) => {
            let key = matches.value_of("key").unwrap();
            string::strlen(&client, key)
        }
        Some(("sunion", matches)) => {
            let key1 = matches.value_of("key1").unwrap();
            let mut keys: Vec<_> = matches.values_of("keys").unwrap().collect();
            keys.insert(0, key1);
            let empty = String::from("");
            set::cmpstore(&client, &keys, &empty, OpType::SET_UNION)
        }
        Some(("sunionstore", matches)) => {
            let store_key = matches.value_of("destination").unwrap();
            let key1 = matches.value_of("key1").unwrap();
            let mut keys: Vec<_> = matches.values_of("keys").unwrap().collect();
            keys.insert(0, key1);
            set::cmpstore(&client, &keys, &store_key, OpType::SET_UNION)
        }
        Some(("time", _matches)) => server::time(&client),
        Some(("type", matches)) => {
            let key = matches.value_of("key").unwrap();
            keys::typ(&client, key)
        }
        Some((_, _)) | None => {
            println!("No subcommand specified.  Run with --help for help.");
            Ok(())
        }
    }
}
