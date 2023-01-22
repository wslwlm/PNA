use crate::{KvsEngine, Protocol, Request, GetResponse, SetResponse, RemoveResponse, Result};
use std::net::{ToSocketAddrs, TcpStream};
use log::{info};

pub struct Server<E: KvsEngine> {
    engine: E,
}

impl<E: KvsEngine> Server<E> {
    pub fn new(engine: E) -> Result<Self> {
        Ok(Server {
            engine,
        })
    }

    pub fn handle_connection(&mut self, stream: TcpStream) -> Result<()> {
        let mut protocol = Protocol::with_stream(stream)?;
        let req:Request = protocol.read_message::<Request>()?;
        info!("request: {}", req);

        match req {
            Request::Get{key} => {
                let resp = match self.engine.get(key) {
                    Ok(value) => GetResponse::Ok(value),
                    Err(e) => GetResponse::Err(e.to_string()),
                };
                protocol.send_messsage(&resp)?;
            },
            Request::Set { key, value } => {
                let resp = match self.engine.set(key, value) {
                    Ok(_) => SetResponse::Ok,
                    Err(e) => SetResponse::Err(e.to_string()),
                };
                protocol.send_messsage(&resp)?;
            },
            Request::Remove { key } => {
                let resp = match self.engine.remove(key) {
                    Ok(_) => RemoveResponse::Ok,
                    Err(e) => RemoveResponse::Err(e.to_string()),
                };
                protocol.send_messsage(&resp)?;
            },
        }

        Ok(())
    }

    pub fn run<A: ToSocketAddrs>(&mut self, addr: A) -> Result<()> {
        let listener = Protocol::bind(addr)?;

        for stream in listener.incoming() {
            match stream {
                Ok(stream) => {
                    /* handle connection */
                    self.handle_connection(stream)?;
                },
                Err(e) => {
                    /* connection error */
                    return Err(e)?;
                }
            }
        }
        Ok(())
    }
}