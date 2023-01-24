use crate::{KvsEngine, Protocol, Request, GetResponse, SetResponse, RemoveResponse, Result};
use std::net::{ToSocketAddrs, TcpStream};
use std::sync::{Arc, atomic::{AtomicBool, Ordering}};
use crate::thread_pool::{ThreadPool};

pub struct Server<E: KvsEngine, P: ThreadPool> {
    engine: E,
    pool: P,
    is_stop: Arc<AtomicBool>,
}

impl<E: KvsEngine, P: ThreadPool> Server<E, P> {
    pub fn new(engine: E, pool: P, is_stop: Arc<AtomicBool>) -> Result<Self> {
        Ok(Server {
            engine,
            pool,
            is_stop,
        })
    }

    pub fn run<A: ToSocketAddrs>(&mut self, addr: A) -> Result<()> {
        let listener = Protocol::bind(addr)?;

        for stream in listener.incoming() {
            // stop if `is_stop` flag is true
            if self.is_stop.load(Ordering::SeqCst) {
                break;
            }

            let engine = self.engine.clone();

            match stream {
                Ok(stream) => {
                    /* handle connection */
                    self.pool.spawn(move|| {
                        handle_connection(engine, stream).unwrap();
                    });
                },
                Err(e) => {
                    /* connection error */
                    println!("err: {}", e.to_string());
                    return Err(e)?;
                }
            }
        }
        Ok(())
    }
}

fn handle_connection<E: KvsEngine>(engine: E, stream: TcpStream) -> Result<()> {
    let mut protocol = Protocol::with_stream(stream)?;
    let req:Request = protocol.read_message::<Request>()?;

    match req {
        Request::Get{key} => {
            let resp = match engine.get(key) {
                Ok(value) => GetResponse::Ok(value),
                Err(e) => GetResponse::Err(e.to_string()),
            };
            protocol.send_messsage(&resp)?;
        },
        Request::Set { key, value } => {
            let resp = match engine.set(key, value) {
                Ok(_) => SetResponse::Ok,
                Err(e) => SetResponse::Err(e.to_string()),
            };
            protocol.send_messsage(&resp)?;
        },
        Request::Remove { key } => {
            let resp = match engine.remove(key) {
                Ok(_) => RemoveResponse::Ok,
                Err(e) => RemoveResponse::Err(e.to_string()),
            };
            protocol.send_messsage(&resp)?;
        },
    }

    Ok(())
}