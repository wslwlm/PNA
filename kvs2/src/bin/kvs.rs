use std::env;
use kvs2::{KvStore, Result};
use std::process;
use structopt::StructOpt;

#[derive(StructOpt, Debug, PartialEq)]
#[structopt(name = env!("CARGO_PKG_NAME"), version = env!("CARGO_PKG_VERSION"), author = env!("CARGO_PKG_AUTHORS"))]
pub struct Opt {
    #[structopt(subcommand)]
    cmd: Option<Cmd>,
}

#[derive(StructOpt, Debug, PartialEq)]
pub enum Cmd {
    Get { key: String },

    Set { key: String, value: String },

    Rm { key: String },
}

fn main() -> Result<()> {
    let opt = Opt::from_args();
    // println!("args: {:?}", opt);

    if opt.cmd.is_none() {
        process::exit(1);
    }

    let mut kvs = KvStore::open(env::current_dir().unwrap()).unwrap();

    if let Some(command) = opt.cmd {
        match command {
            Cmd::Get { key } => {
                let value = kvs.get(key).unwrap();
                match value {
                    Some(v) => {
                        println!("{}", v);
                    },
                    None => {
                        println!("Key not found");
                    }
                }
            }   
            Cmd::Set { key, value } => {
                if let Err(e) = kvs.set(key, value) {
                    println!("{}", e);
                    return Err(e);
                }
            }
            Cmd::Rm { key } => {
                if let Err(e) =  kvs.remove(key) {
                    println!("{}", e);
                    return Err(e);
                }
            }
        }
    }

    Ok(())
}
