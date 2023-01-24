use crate::{Request, GetResponse, SetResponse, RemoveResponse, Protocol, KvError, Result};
use std::net::ToSocketAddrs;

pub struct Client {
    protocol: Protocol,
}

impl Client {
    pub fn connect<A: ToSocketAddrs>(addr: A) -> Result<Self> {
        Ok(Client {
            protocol: Protocol::connect(addr)?
        })
    }

    pub fn get(&mut self, key: String) -> Result<Option<String>> {
        self.protocol.send_messsage(&Request::Get {key})?;
        let resp = self.protocol.read_message::<GetResponse>()?;
        match resp {
            GetResponse::Ok(val) => Ok(val),
            GetResponse::Err(e) => Err(KvError::StringError(e)), 
        }
    }

    pub fn set(&mut self, key: String, value: String) -> Result<()> {
        self.protocol.send_messsage(&Request::Set {key: key.clone(), value: value.clone()})?;
        let resp = self.protocol.read_message::<SetResponse>()?;
        match resp {
            SetResponse::Ok => Ok(()),
            SetResponse::Err(e) => Err(KvError::StringError(e)),
        }
    }

    pub fn remove(&mut self, key: String) -> Result<()> {
        self.protocol.send_messsage(&Request::Remove {key})?;
        let resp = self.protocol.read_message::<RemoveResponse>()?;
        match resp {
            RemoveResponse::Ok => Ok(()),
            RemoveResponse::Err(e) => Err(KvError::StringError(e))
        }
    }
}