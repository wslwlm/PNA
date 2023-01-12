use clap::{arg, Arg, command, ArgAction, Command};
use kvs::KvStore;
use std::process;

fn main() {
    let mut kvs = KvStore::new();

    let matches = Command::new(env!("CARGO_PKG_NAME"))
        .version(env!("CARGO_PKG_VERSION"))
        .author(env!("CARGO_PKG_AUTHORS"))
        // .arg(Arg::new("version")
        //     .short('V')
        //     .long("version")
        //     .action(ArgAction::SetTrue)
        //     .required(false)
        // )
        .subcommand(
            Command::new("get")
                .about("Get value from kvs")
                .arg(Arg::new("key")),
        )
        .subcommand(
            Command::new("set")
                .about("Set (key, value) into kvs")
                .arg(Arg::new("key"))
                .arg(Arg::new("value")),
        )
        .subcommand(    
                Command::new("rm")
                .about("remove key from kvs")
                .arg(Arg::new("key")),
        ).get_matches();

    // no argument
    if !matches.args_present() && matches.subcommand() == None {
        process::exit(1);
    }

    // match matches.get_one::<bool>("version") {
    //     Some(&has_version) => {
    //         if has_version {
    //             println!("version: {}", env!("CARGO_PKG_VERSION"))
    //         }
    //     },
    //     None => ()
    // };

    match matches.subcommand() {
        Some(("get", sub_matches)) => {
            let key = sub_matches.get_one::<String>("key").expect("[Get] Key required");
            let value = kvs.get(key.clone());
            println!("[Get] value: {:?}", value);
        },
        Some(("set", sub_matches)) => {
            let key = sub_matches.get_one::<String>("key").expect("[Set] Key required");
            let value = sub_matches.get_one::<String>("value").expect("[Set] Value required");
            kvs.set(key.clone(), value.clone());
        },
        Some(("rm", sub_matches)) => {
            let key = sub_matches.get_one::<String>("key").expect("[Remove] Key Required");
            kvs.remove(key.clone());
        }
        _ => {
            process::exit(1);
        }
    };
}