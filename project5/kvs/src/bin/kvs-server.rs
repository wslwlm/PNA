extern crate tokio;
use kvs::{KvStore, SledEngine, KvsEngine, Result, KvError, Server, thread_pool::RayonThreadPool};
use std::{fs, env};
use structopt::StructOpt;
use log::{info};
use env_logger::{Env};
use std::sync::{Arc, atomic::{AtomicBool}};

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

fn current_engine() -> Result<Option<String>> {
    let engine_file = env::current_dir()?.join("engine");
    if !engine_file.exists() {
        return Ok(None);
    }

    let engine = fs::read_to_string(engine_file)?;
    Ok(Some(engine))
}


async fn run_with_engine<E: KvsEngine>(engine: E, addr: String) -> Result<()> {
    let is_stop = Arc::new(AtomicBool::new(false));
    let mut server = Server::new(engine, is_stop)?;
    server.run(addr).await?;
    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
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

    if let Some(curr_engine) = current_engine()? {
        if curr_engine != engine {
            return Err(KvError::WrongEngine);
        }
    }

    let engine_file = env::current_dir()?.join("engine");
    fs::write(engine_file, format!("{}", engine))?;

    let cpu_num = num_cpus::get();

    if engine == "kvs" {
        run_with_engine(KvStore::<RayonThreadPool>::open(env::current_dir()?, cpu_num)?, addr).await?;
    } else if engine == "sled" {
        run_with_engine(SledEngine::<RayonThreadPool>::open(env::current_dir()?, cpu_num)?, addr).await?;
    } else {
        return Err(KvError::WrongEngine);
    }
    
    Ok(())
}
