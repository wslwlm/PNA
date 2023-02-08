extern crate tokio;
extern crate futures;

use tokio::sync::oneshot::Receiver;
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

    pub async fn run<A: ToSocketAddrs>(&mut self, addr: A, rx: Receiver<()>) -> Result<()> {
        let listener = TcpListener::bind(addr).await?;

        tokio::select! {
            _ = async move {
                loop {
                    let (socket, addr) = listener.accept().await.unwrap();
                
                    let engine = self.engine.clone();
                    tokio::spawn(async move {
                        handle_connection(engine, socket).await.unwrap();
                    });
                }
            } => {}
            _ = rx => {
                // println!("terminating accept loop");
            }
        }

        Ok(())
    }
}

async fn handle_connection<E: KvsEngine>(engine: E, stream: TcpStream) -> Result<()> {
    let (read_half, write_half) = stream.into_split();
    let mut reader: SymmetricalReader<Request> = SymmetricallyFramed::new(FramedRead::new(read_half, LengthDelimitedCodec::new()), SymmetricalBincode::default());
    let mut writer: SymmetricalWriter<Response> = SymmetricallyFramed::new(FramedWrite::new(write_half, LengthDelimitedCodec::new()), SymmetricalBincode::default());

    if let Some(req) = reader.try_next().await? {
        match req {
            Request::Get{key} => {
                let resp = match engine.get(key).await {
                    Ok(value) => Response::Get(value),
                    Err(e) => Response::Err(e.to_string()),
                };
                writer.send(resp).await?;
            },
            Request::Set { key, value } => {
                // println!("[SetRequest] {key}: {value}");
                let resp = match engine.set(key, value).await {
                    Ok(_) => Response::Set,
                    Err(e) => Response::Err(e.to_string()),
                };
                writer.send(resp).await?;
            },
            Request::Remove { key } => {
                let resp = match engine.remove(key).await {
                    Ok(_) => Response::Remove,
                    Err(e) => Response::Err(e.to_string()),
                };
                writer.send(resp).await?;
            },
        }
    }
    Ok(())
}
