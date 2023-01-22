use kvs::{Client, Result};
use std::{env, process};
use structopt::StructOpt;
use std::io::{Read, Write};
use env_logger::{Env};

#[derive(StructOpt, Debug, PartialEq)]
#[structopt(name = env!("CARGO_PKG_NAME"), version = env!("CARGO_PKG_VERSION"), author = env!("CARGO_PKG_AUTHORS"))]
pub struct Opt {
    #[structopt(subcommand)]
    cmd: Option<Cmd>,
}

#[derive(StructOpt, Debug, PartialEq)]
pub enum Cmd {
    #[structopt(name="get", about="get <key> [--addr IP-PORT]")]
    Get { 
        key: String, 

        // 不加long参数 addr是一个args
        #[structopt(name="addr", long, default_value="127.0.0.1:4000")]
        addr: String,
    },

    #[structopt(name="set", about="set <key> <value> [--addr IP-PORT]")]
    Set { 
        key: String, 
        value: String,

        #[structopt(name="addr", long, default_value="127.0.0.1:4000")]
        addr: String,
    },

    #[structopt(name="rm", about="rm <key> [--addr IP-PORT]")]
    Rm { 
        key: String,

        #[structopt(name="addr", long, default_value="127.0.0.1:4000")]
        addr: String,
    },
}

fn main() -> Result<()> {
    env_logger::Builder::from_env(Env::default().default_filter_or("info")).init();

    let opt = Opt::from_args();
    // println!("args: {:?}", opt);

    if opt.cmd.is_none() {
        process::exit(1);
    }

    if let Some(command) = opt.cmd {
        match command {
            Cmd::Get { key, addr } => {
                // info!("key: {}, addr: {}", key, addr);
                let mut client = Client::connect(addr)?;
                if let Some(value) = client.get(key)? {
                    println!("{}", value);
                } else {
                    println!("Key not found");
                }
            },
            Cmd::Set { key, value, addr } => {
                // info!("key: {}, value: {}, addr: {}", key, value, addr);
                let mut client = Client::connect(addr)?;
                client.set(key, value)?;
            },
            Cmd::Rm { key , addr} => {
                // info!("key: {}, addr: {}", key, addr);
                let mut client = Client::connect(addr)?;
                client.remove(key)?;
            }
        }
    }

    Ok(())
}
