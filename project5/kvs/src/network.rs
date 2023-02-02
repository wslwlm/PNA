use std::convert::From;
use std::io::{self, Read, Write, BufReader, BufWriter};
use std::fmt::{self};
use std::net::{TcpStream, TcpListener, SocketAddr, ToSocketAddrs};
use failure::Fail;
use byteorder::{NetworkEndian, ReadBytesExt, WriteBytesExt};
use serde;
use serde_json::de::{Deserializer, IoRead}; 
use crate::{KvError, Result};

// /// traits for something that can convert to `&[u8]`
// pub trait Serialize {
//     /// Serialize to a `write`able buffer
//     fn serialize(&self, buf: &mut impl Write) -> Result<usize>;
// }

// /// traits for something that can be converted from `[u8]`
// pub trait Deserialize {
//     /// the type that `&[u8]` deserialize to
//     type Output;

//     /// Deserialize from the `Read`able buffer 
//     fn deserialize(buf: &mut impl Read) -> Result<Self::Output>;
// }

/// Request object (client -> server)
#[derive(serde::Serialize, serde::Deserialize, Debug)]
pub enum Request {
    Get {key: String},
    Set {key: String, value: String},
    Remove {key: String},
}

// impl Serialize for Request {
//     fn serialize(&self, buf: &mut impl Write) -> Result<usize> {
//         serde_json::to_writer(buf, self)?;
//         buf.flush()?;
//         Ok(0)
//     }
// }

// impl Deserialize for Request {
//     type Output = Request;

//     fn deserialize(buf: &mut impl Read) -> Result<Self::Output> {
//         Ok(serde_json::from_reader(buf)?)
//     }
// }

// impl From<&Request> for u8 {
//     fn from(value: &Request) -> Self {
//         match value {
//             Request::Get { .. } => 1,
//             Request::Set { .. } => 2,
//             Request::Remove { .. } => 3,
//         }
//     }
// }

// fn encode_string(buf: &mut impl Write, message: &String) -> Result<usize> {
//     let message_bytes = message.as_bytes();
//     buf.write_u32::<NetworkEndian>((message_bytes.len()) as u32)?;
//     buf.write_all(message_bytes)?;
//     Ok(2 + message_bytes.len())
// }

// impl Serialize for Request {
//     /// Serialize Request to bytes array
//     fn serialize(&self, buf: &mut impl Write) -> Result<usize> {
//         buf.write_u8(self.into())?;
//         let mut bytes_written: usize = 1;
//         match self {
//             Request::Get { key } => {
//                 bytes_written += encode_string(buf, key)?;
//             }, 
//             Request::Set { key, value} => {
//                 bytes_written += encode_string(buf, key)?;
//                 bytes_written += encode_string(buf, value)?;
//             },  
//             Request::Remove { key } => {
//                 bytes_written += encode_string(buf, key)?;
//             }
//         }
//         Ok(bytes_written)
//     }
// }

// fn decode_string(buf: &mut impl Read) -> Result<String> {
//     let key_size = buf.read_u32::<NetworkEndian>()?;
//     let mut key_buf = vec![0; key_size as usize];
//     buf.read_exact(&mut key_buf)?;
//     String::from_utf8(key_buf).map_err(|_| KvError::Io(io::Error::new(io::ErrorKind::InvalidData, "Invalid utf8")))
// }

// impl Deserialize for Request {
//     type Output = Request;

//     fn deserialize(buf: &mut impl Read) -> Result<Self::Output> {
//         match buf.read_u8()? {
//             1 => {
//                 // Deserialize to Get Request
//                 let key = decode_string(buf)?;
//                 Ok(Request::Get { key, })
//             },
//             2 => {
//                 // Deserialize to Set Request
//                 let key = decode_string(buf)?;
//                 let value = decode_string(buf)?;
//                 Ok(Request::Set { key, value })
//             },
//             3 => {
//                 let key = decode_string(buf)?;
//                 Ok(Request::Remove { key })
//             },
//             _ => { Err(KvError::Io(io::Error::new(io::ErrorKind::InvalidData, "invalid data")))}
//         }
//     }
// }

impl fmt::Display for Request {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Request::Get {key} => write!(f, "[Get] key: {}", key),
            Request::Set {key, value} => write!(f, "[Set] key: {}, value: {}", key, value),
            Request::Remove { key } => write!(f, "[Remove] key: {}", key),
        }
    }
}

// TODO(wsl): how to represent the error
#[derive(serde::Serialize, serde::Deserialize)]
pub enum GetResponse {
    Ok(Option<String>),
    Err(String),
}

// impl Serialize for GetResponse {
//     fn serialize(&self, buf: &mut impl Write) -> Result<usize> {
//         serde_json::to_writer(buf, self)?;
//         Ok(0)
//     }
// }

// impl Deserialize for GetResponse {
//     type Output = GetResponse;

//     fn deserialize(buf: &mut impl Read) -> Result<Self::Output> {
//         Ok(serde_json::from_reader(buf)?)
//     }
// }

#[derive(serde::Serialize, serde::Deserialize, Debug)]
pub enum SetResponse {
    Ok,
    Err(String),
}

// impl Serialize for SetResponse {
//     fn serialize(&self, buf: &mut impl Write) -> Result<usize> {
//         serde_json::to_writer(buf, self)?;
//         Ok(0)
//     }
// }

// impl Deserialize for SetResponse {
//     type Output = SetResponse;

//     fn deserialize(buf: &mut impl Read) -> Result<Self::Output> {
//         Ok(serde_json::from_reader(buf)?)
//     }
// }

#[derive(serde::Serialize, serde::Deserialize)]
pub enum RemoveResponse {
    Ok, 
    Err(String)
}

// impl Serialize for RemoveResponse {
//     fn serialize(&self, buf: &mut impl Write) -> Result<usize> {
//         serde_json::to_writer(buf, self)?;
//         Ok(0)
//     }
// }

// impl Deserialize for RemoveResponse {
//     type Output = RemoveResponse;

//     fn deserialize(buf: &mut impl Read) -> Result<Self::Output> {
//         Ok(serde_json::from_reader(buf)?)
//     }
// } 

/// Abstracted protocol wraps a TcpStream
pub struct Protocol {
    reader: Deserializer<IoRead<BufReader<TcpStream>>>,
    writer: BufWriter<TcpStream>,
}

impl Protocol {
    pub fn with_stream(stream: TcpStream) -> Result<Self> {
        Ok (Protocol{ 
            reader: Deserializer::from_reader(BufReader::new(stream.try_clone()?)),
            // Why exists EOF Serialize?
            writer: BufWriter::new(stream),
        })
    }

    pub fn connect<A: ToSocketAddrs>(socket_addr: A) -> Result<Self> {
        let stream = TcpStream::connect(socket_addr)?;
        Self::with_stream(stream)
    }

    pub fn bind<A: ToSocketAddrs>(socket_addr: A) -> Result<TcpListener> {
        Ok(TcpListener::bind(socket_addr)?)
    }

    pub fn send_messsage(&mut self, message: &impl serde::Serialize) -> Result<()> {
        // let length = message.serialize(&mut self.writer)?;
        // self.writer.flush()?;
        // Ok(length)
        serde_json::to_writer(&mut self.writer, message)?;
        self.writer.flush()?;
        Ok(())
    } 

    pub fn read_message<'a, T: serde::Deserialize<'a>>(&mut self) -> Result<T> {
        // T::deserialize(&mut self.reader)
        Ok(T::deserialize(&mut self.reader)?)
    }
}