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
            SubCommand::with_name("str.get").about("String.Get").arg(
                Arg::with_name("key")
                    .help("Key of item to retrieve")
                    .required(true),
            ),
        )
        .subcommand(
            SubCommand::with_name("str.set")
                .about("String.Set")
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
        ("str.get", Some(matches)) => {
            let key = matches.value_of("key").unwrap();
            println!("ACTION: str.get {}", key);
        }
        ("str.set", Some(matches)) => {
            let key = matches.value_of("key").unwrap();
            let value = matches.value_of("value").unwrap();
            println!("ACTION: str.set {}={}", key, value);
        }
        ("", None) => println!("No subcommand specified.  Run with --help for help."),
        _ => unreachable!(),
    }
}
