extern crate clap;

use clap::value_t;
use serde_derive::Deserialize;

const APPNAME: &'static str = "memds-server";
const VERSION: &'static str = env!("CARGO_PKG_VERSION");

const DEF_BIND_ADDR: &'static str = "127.0.0.1";
const DEF_CONFIG_FN: &'static str = "memds.conf";

#[derive(Deserialize)]
struct TomlConfig {
    network: Option<TomlNetworkConfig>,
    fs: Option<TomlFsConfig>,
}

#[derive(Deserialize)]
struct TomlNetworkConfig {
    bind_addr: Option<String>,
    bind_port: Option<u16>,
}

#[derive(Deserialize)]
struct TomlFsConfig {
    import: Option<String>,
}

pub struct Config {
    pub network: NetworkConfig,
    pub fs: FsConfig,
}

pub struct NetworkConfig {
    pub bind_addr: String,
    pub bind_port: u16,
}

pub struct FsConfig {
    pub import: Option<String>,
}

pub fn get() -> Config {
    // parse command line
    let cli_matches = clap::App::new(APPNAME)
        .version(VERSION)
        .about("Memory Database Service")
        .arg(
            clap::Arg::with_name("bind-addr")
                .short("a")
                .long("bind-addr")
                .value_name("IP-ADDRESS")
                .help(&format!("socket bind address (default: {})", DEF_BIND_ADDR))
                .takes_value(true),
        )
        .arg(
            clap::Arg::with_name("bind-port")
                .short("p")
                .long("bind-port")
                .value_name("PORT")
                .help(&format!(
                    "socket bind port (default: {})",
                    memds_proto::DEF_PORT
                ))
                .takes_value(true),
        )
        .arg(
            clap::Arg::with_name("config")
                .short("c")
                .long("config")
                .value_name("TOML-FILE")
                .help(&format!(
                    "Read configuration file (default: {})",
                    DEF_CONFIG_FN
                ))
                .takes_value(true),
        )
        .arg(
            clap::Arg::with_name("import")
                .long("import")
                .value_name("MEMDS-FILE")
                .help("Import serialized database file")
                .takes_value(true),
        )
        .get_matches();

    let config_fn = cli_matches.value_of("config").unwrap_or(DEF_CONFIG_FN);

    // read config file
    let cfg_res = std::fs::read_to_string(config_fn);
    let f_cfg = {
        // read config, or create default
        let mut f_cfg: TomlConfig;
        if cfg_res.is_ok() {
            f_cfg = toml::from_str(&cfg_res.unwrap()).unwrap();
        } else {
            f_cfg = TomlConfig {
                network: None,
                fs: None,
            };
        }

        // if network section missing, create default one
        if f_cfg.network.is_none() {
            f_cfg.network = Some(TomlNetworkConfig {
                bind_addr: None,
                bind_port: None,
            });
        }

        // if fs section missing, create default one
        if f_cfg.fs.is_none() {
            f_cfg.fs = Some(TomlFsConfig { import: None });
        }

        let f_fs_cfg = f_cfg.fs.as_mut().unwrap();

        if cli_matches.is_present("import") {
            f_fs_cfg.import = Some(cli_matches.value_of("import").unwrap().to_string());
        }

        let f_net_cfg = f_cfg.network.as_mut().unwrap();

        // CLI arg overrides config file value; else if missing, provide def.
        if cli_matches.is_present("bind-addr") {
            f_net_cfg.bind_addr = Some(cli_matches.value_of("bind-addr").unwrap().to_string());
        } else if f_net_cfg.bind_addr.is_none() {
            f_net_cfg.bind_addr = Some(DEF_BIND_ADDR.to_string());
        }

        // CLI arg overrides config file value; else if missing, provide def.
        if cli_matches.is_present("bind-port") {
            f_net_cfg.bind_port = Some(value_t!(cli_matches, "bind-port", u16).unwrap());
        } else if f_net_cfg.bind_port.is_none() {
            f_net_cfg.bind_port = Some(memds_proto::DEF_PORT);
        }

        f_cfg
    };

    let f_net_cfg = f_cfg.network.unwrap();
    let f_fs_cfg = f_cfg.fs.unwrap();

    Config {
        network: NetworkConfig {
            bind_addr: f_net_cfg.bind_addr.unwrap(),
            bind_port: f_net_cfg.bind_port.unwrap(),
        },
        fs: FsConfig {
            import: f_fs_cfg.import,
        },
    }
}
