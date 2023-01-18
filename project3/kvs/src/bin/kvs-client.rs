use kvs::{Result, Request, Response, Protocol};
use std::{env, process, net::TcpStream, io::BufWriter};
use structopt::StructOpt;
use std::io::{Read, Write};
use serde::{Serialize, Deserialize};

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
    let opt = Opt::from_args();
    println!("args: {:?}", opt);

    if opt.cmd.is_none() {
        process::exit(1);
    }

    if let Some(command) = opt.cmd {
        match command {
            Cmd::Get { key, addr } => {
                println!("key: {}, addr: {}", key, addr);
                let mut stream = TcpStream::connect(addr)?;
                let mut protocol = Protocol::with_stream(stream)?;
                protocol.send_messsage(&Request::Get {
                    key,
                })?;
                let resp: Response = protocol.read_message::<Response>()?;
                println!("response: {}", resp);
            },
            Cmd::Set { key, value, addr } => {
                println!("key: {}, value: {}, addr: {}", key, value, addr);
            },
            Cmd::Rm { key , addr} => {
                println!("key: {}, addr: {}", key, addr);
            }
        }
    }

    Ok(())
}
