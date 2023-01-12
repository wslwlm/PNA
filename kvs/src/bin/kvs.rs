// use clap::{arg, Arg, command, ArgAction, Command};
use kvs::KvStore;
use std::process;
use structopt::StructOpt;

#[derive(StructOpt, Debug, PartialEq)]
#[structopt(name = env!("CARGO_PKG_NAME"), version = env!("CARGO_PKG_VERSION"), author = env!("CARGO_PKG_AUTHORS"))]
pub struct Opt {
    #[structopt(subcommand)]
    cmd: Option<Cmd>
}

#[derive(StructOpt, Debug, PartialEq)]
pub enum Cmd {
    Get {
        key: String,
    },

    Set {
        key: String,
        value: String,
    },

    Rm {
        key: String,
    }
}

fn main() {
    let mut kvs = KvStore::new();

    let opt = Opt::from_args();
    println!("args: {:?}", opt);

    if opt.cmd.is_none() {
        process::exit(1);
    }
    
    if let Some(command) = opt.cmd {
        match command {
            Cmd::Get { key } => {
                let value = kvs.get(key);
                println!("value: {:?}", value);
            },
            Cmd::Set { key, value} => {
                kvs.set(key, value);
            },
            Cmd::Rm { key } => {
                kvs.remove(key);
            }
        }
    }

    // let matches = Command::new(env!("CARGO_PKG_NAME"))
    //     .version(env!("CARGO_PKG_VERSION"))
    //     .author(env!("CARGO_PKG_AUTHORS"))
    //     // .arg(Arg::new("version")
    //     //     .short('V')
    //     //     .long("version")
    //     //     .action(ArgAction::SetTrue)
    //     //     .required(false)
    //     // )
    //     .subcommand(
    //         Command::new("get")
    //             .about("Get value from kvs")
    //             .arg(Arg::new("key")),
    //     )
    //     .subcommand(
    //         Command::new("set")
    //             .about("Set (key, value) into kvs")
    //             .arg(Arg::new("key"))
    //             .arg(Arg::new("value")),
    //     )
    //     .subcommand(    
    //             Command::new("rm")
    //             .about("remove key from kvs")
    //             .arg(Arg::new("key")),
    //     ).get_matches();

    // // no argument
    // if !matches.args_present() && matches.subcommand() == None {
    //     process::exit(1);
    // }

    // // match matches.get_one::<bool>("version") {
    // //     Some(&has_version) => {
    // //         if has_version {
    // //             println!("version: {}", env!("CARGO_PKG_VERSION"))
    // //         }
    // //     },
    // //     None => ()
    // // };

    // match matches.subcommand() {
    //     Some(("get", sub_matches)) => {
    //         let key = sub_matches.get_one::<String>("key").expect("[Get] Key required");
    //         let value = kvs.get(key.clone());
    //         println!("[Get] value: {:?}", value);
    //     },
    //     Some(("set", sub_matches)) => {
    //         let key = sub_matches.get_one::<String>("key").expect("[Set] Key required");
    //         let value = sub_matches.get_one::<String>("value").expect("[Set] Value required");
    //         kvs.set(key.clone(), value.clone());
    //     },
    //     Some(("rm", sub_matches)) => {
    //         let key = sub_matches.get_one::<String>("key").expect("[Remove] Key Required");
    //         kvs.remove(key.clone());
    //     }
    //     _ => {
    //         process::exit(1);
    //     }
    // };
}