use kvs::{KvStore, KvsEngine, Result};
use std::{env, process, net::TcpListener};
use structopt::StructOpt;
use log::{debug, error, log_enabled, info, Level};
use env_logger::{Env};
use std::io::Read;

#[derive(StructOpt, Debug, PartialEq)]
#[structopt(name = env!("CARGO_PKG_NAME"), version = env!("CARGO_PKG_VERSION"), author = env!("CARGO_PKG_AUTHORS"))]
pub struct Opt {
    // 不加long参数 addr是一个args
    #[structopt(name="addr", long, default_value="127.0.0.1:4000", about="[--addr IP-PORT]")]
    addr: String,

    // 不加long参数 addr是一个args
    #[structopt(name="engine", long, default_value="kvs", about="[--engine ENGINE-NAME]")]
    engine: String,
}


fn main() -> Result<()> {
    // set the default log_level -> Level::Info
    env_logger::Builder::from_env(Env::default().default_filter_or("info")).init();

    // let mut builder = Builder::from_default_env();
    // builder.target(Target::Stdout);
    // builder.init();

    let opt = Opt::from_args();
    // println!("args: {:?}", opt);

    let addr = opt.addr;
    let engine = opt.engine;

    info!("{} vesrsion {}, addr: {} port: {}", 
            env!("CARGO_PKG_NAME"), 
            env!("CARGO_PKG_VERSION"), 
            addr, engine);

    let listener = TcpListener::bind(addr)?;

    for stream in listener.incoming() {
        match stream {
            Ok(mut stream) => {
                /* handle connection */
                let mut buf = vec![0; 128];
                stream.read(&mut buf)?;
                println!("buf: {:?}", buf);
            },
            Err(e) => {
                /* connection error */
            }
        }
    }
    
    Ok(())
}
