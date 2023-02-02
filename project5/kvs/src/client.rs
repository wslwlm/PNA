use crate::{KvError, Result};
use crate::common::{Request, Response};
use tokio::net::ToSocketAddrs;
use tokio_util::codec::{FramedRead, FramedWrite, LengthDelimitedCodec};
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::net::TcpStream;
use tokio_serde::formats::*;
use tokio_serde::SymmetricallyFramed;
use futures::prelude::*;

pub type SymmetricalReader<T> = SymmetricallyFramed<
    FramedRead<OwnedReadHalf, LengthDelimitedCodec>,
    T,
    SymmetricalBincode<T>>;

pub type SymmetricalWriter<T> = SymmetricallyFramed<
    FramedWrite<OwnedWriteHalf, LengthDelimitedCodec>,
    T,
    SymmetricalBincode<T>>;

pub struct Client {
    reader: SymmetricalReader<Response>,
    writer: SymmetricalWriter<Request>,
}

impl Client {
    pub async fn connect<A: ToSocketAddrs>(addr: A) -> Result<Self> {
        let stream = TcpStream::connect(addr).await?;
        let (read_half, write_half) = stream.into_split();
        let reader = SymmetricallyFramed::new(FramedRead::new(read_half, LengthDelimitedCodec::new()), SymmetricalBincode::default());
        let writer = SymmetricallyFramed::new(FramedWrite::new(write_half, LengthDelimitedCodec::new()), SymmetricalBincode::default());
        Ok(Client {
            reader,
            writer,
        })
    }

    pub async fn get(&mut self, key: String) -> Result<Option<String>> {
        let resp = self.send_request(Request::Get { key }).await?;
        match resp {
            Some(Response::Get(value)) => Ok(value),
            Some(Response::Err(msg)) => Err(KvError::StringError(msg)),
            Some(_) => Err(KvError::StringError("Invalid response".to_owned())),
            None => Err(KvError::StringError("No response received".to_owned())),
        }
    }

    pub async fn set(&mut self, key: String, value: String) -> Result<()> {
        let resp = self.send_request(Request::Set { key, value }).await?;
        match resp {
            Some(Response::Set) => Ok(()),
            Some(Response::Err(msg)) => Err(KvError::StringError(msg)),
            Some(_) => Err(KvError::StringError("Invalid response".to_owned())),
            None => Err(KvError::StringError("No response received".to_owned())),
        }
    }

    pub async fn remove(&mut self, key: String) -> Result<()> {
        let resp = self.send_request(Request::Remove { key }).await?;
        match resp {
            Some(Response::Remove) => Ok(()),
            Some(Response::Err(msg)) => Err(KvError::StringError(msg)),
            Some(_) => Err(KvError::StringError("Invalid response".to_owned())),
            None => Err(KvError::StringError("No response received".to_owned())),
        }
    }

    pub async fn send_request(&mut self, req: Request) -> Result<Option<Response>> {
        self.writer.send(req).await?;
        self.reader.try_next().await.map_err(|e| e.into())
    }
}
