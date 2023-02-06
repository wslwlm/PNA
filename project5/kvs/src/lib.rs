#![feature(type_alias_impl_trait)]

pub use engines::{KvStore, SledEngine, KvsEngine};
// pub use network::{Request, GetResponse, SetResponse, RemoveResponse, Protocol};
pub use error::{KvError, Result};
pub use client::{Client, SymmetricalReader, SymmetricalWriter};
pub use server::{Server};
pub use common::{Request, Response};

mod common;
mod engines;
mod network;
mod error;
mod client;
mod server;
pub mod thread_pool;