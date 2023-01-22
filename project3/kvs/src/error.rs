use std::convert::From;
use failure::Fail;
use sled;
use std::io;
use std::string::FromUtf8Error;

#[derive(Fail, Debug)]
pub enum KvError {
    #[fail(display = "Io Error: {}", _0)]
    Io(#[cause] io::Error),
    
    #[fail(display = "serialize/deserialize error: {}", _0)]
    Serialize(#[cause] serde_json::Error),

    #[fail(display = "sled engine error: {}", _0)]
    Sled(#[cause] sled::Error),

    #[fail(display = "Key not found")]
    KeyNotFound,

    #[fail(display = "Reader not found")]
    ReaderNotFound,

    #[fail(display = "utf8 error")]
    Utf8(#[cause] FromUtf8Error),

    #[fail(display = "response error message: {}", _0)]
    StringError(String),

    #[fail(display = "wrone engine")]
    WrongEngine,
}

/// io::Error -> KvStoreError
impl From<io::Error> for KvError {
    fn from(error: io::Error) -> Self {
        KvError::Io(error)
    }
}

/// serde_jsonL::Error -> KvStoreError
impl From<serde_json::Error> for KvError {
    fn from(error: serde_json::Error) -> Self {
        KvError::Serialize(error)
    }
}

impl From<sled::Error> for KvError {
    fn from(error: sled::Error) -> Self {
        KvError::Sled(error)
    }
}

impl From<FromUtf8Error> for KvError {
    fn from(error: FromUtf8Error) -> Self {
        KvError::Utf8(error)
    }
}

pub type Result<T> = std::result::Result<T, KvError>;