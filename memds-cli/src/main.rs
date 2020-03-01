extern crate clap;

use clap::{Arg, SubCommand};

const APPNAME: &'static str = "memds-cli";
const VERSION: &'static str = env!("CARGO_PKG_VERSION");

fn main() {
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
            println!("ACTION: str.get {}", key);
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
}
