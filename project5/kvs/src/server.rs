extern crate tokio;
extern crate futures;

use futures::{TryStreamExt, SinkExt};
use tokio::{spawn};
use tokio::net::{TcpListener, TcpStream, ToSocketAddrs};
use tokio_serde::{SymmetricallyFramed};
use tokio_serde::formats::SymmetricalBincode;
use tokio_util::codec::{FramedRead, FramedWrite, LengthDelimitedCodec};

use crate::{KvsEngine, Result, Request, Response, SymmetricalReader, SymmetricalWriter};
use std::sync::{Arc, atomic::{AtomicBool, Ordering}};

pub struct Server<E: KvsEngine> {
    engine: E,
    is_stop: Arc<AtomicBool>,
}

impl<E: KvsEngine> Server<E> {
    pub fn new(engine: E, is_stop: Arc<AtomicBool>) -> Result<Self> {
        Ok(Server {
            engine,
            is_stop,
        })
    }

    pub async fn run<A: ToSocketAddrs>(&mut self, addr: A) -> Result<()> {
        let listener = TcpListener::bind(addr).await?;

        loop {
            if self.is_stop.load(Ordering::SeqCst) {
                break;
            }

            let (socket, addr) = listener.accept().await?;
            println!("accept connectiong: {addr}");
            let engine = self.engine.clone();
            tokio::spawn(async move {
                handle_connection(engine, socket).await.unwrap();
            });
        }
        Ok(())
    }
}

async fn handle_connection<E: KvsEngine>(engine: E, stream: TcpStream) -> Result<()> {
    let (read_half, write_half) = stream.into_split();
    let mut reader: SymmetricalReader<Request> = SymmetricallyFramed::new(FramedRead::new(read_half, LengthDelimitedCodec::new()), SymmetricalBincode::default());
    let mut writer: SymmetricalWriter<Response> = SymmetricallyFramed::new(FramedWrite::new(write_half, LengthDelimitedCodec::new()), SymmetricalBincode::default());

    while let Some(req) = reader.try_next().await? {
        match req {
            Request::Get{key} => {
                let resp = match engine.get(key) {
                    Ok(value) => Response::Get(value),
                    Err(e) => Response::Err(e.to_string()),
                };
                writer.send(resp).await?;
            },
            Request::Set { key, value } => {
                // println!("[SetRequest] {key}: {value}");
                let resp = match engine.set(key, value) {
                    Ok(_) => Response::Set,
                    Err(e) => Response::Err(e.to_string()),
                };
                writer.send(resp).await?;
            },
            Request::Remove { key } => {
                let resp = match engine.remove(key) {
                    Ok(_) => Response::Remove,
                    Err(e) => Response::Err(e.to_string()),
                };
                writer.send(resp).await?;
            },
        }
    }
    Ok(())
}
