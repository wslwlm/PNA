use std::convert::From;
use std::io::{self, Read, Write, BufReader};
use std::fmt::{self};
use std::iter::Successors;
use std::net::TcpStream;
use byteorder::{NetworkEndian, ReadBytesExt, WriteBytesExt};

/// traits for something that can convert to `&[u8]`
pub trait Serialize {
    /// Serialize to a `write`able buffer
    fn serialize(&self, buf: &mut impl Write) -> io::Result<usize>;
}

/// traits for something that can be converted from `[u8]`
pub trait Deserialize {
    /// the type that `&[u8]` deserialize to
    type Output;

    /// Deserialize from the `Read`able buffer 
    fn deserialize(buf: &mut impl Read) -> io::Result<Self::Output>;
}

/// Request object (client -> server)
pub enum Request {
    Get {key: String},
    Set {key: String, value: String},
    Remove {key: String},
}

impl From<&Request> for u8 {
    fn from(value: &Request) -> Self {
        match value {
            Request::Get { .. } => 1,
            Request::Set { .. } => 2,
            Request::Remove { .. } => 3,
        }
    }
}

fn encode_string(buf: &mut impl Write, message: &String) -> io::Result<usize> {
    let message_bytes = message.as_bytes();
    buf.write_u32::<NetworkEndian>((message_bytes.len()) as u32)?;
    buf.write_all(message_bytes)?;
    Ok(2 + message_bytes.len())
}

impl Serialize for Request {
    /// Serialize Request to bytes array
    fn serialize(&self, buf: &mut impl Write) -> io::Result<usize> {
        buf.write_u8(self.into())?;
        let mut bytes_written: usize = 1;
        match self {
            Request::Get { key } => {
                bytes_written += encode_string(buf, key)?;
            }, 
            Request::Set { key, value} => {
                bytes_written += encode_string(buf, key)?;
                bytes_written += encode_string(buf, value)?;
            },  
            Request::Remove { key } => {
                bytes_written += encode_string(buf, key)?;
            }
        }
        Ok(bytes_written)
    }
}

fn decode_string(buf: &mut impl Read) -> io::Result<String> {
    let key_size = buf.read_u32::<NetworkEndian>()?;
    let mut key_buf = vec![0; key_size as usize];
    buf.read_exact(&mut key_buf)?;
    String::from_utf8(key_buf).map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "Invalid utf8"))
}

impl Deserialize for Request {
    type Output = Request;

    fn deserialize(buf: &mut impl Read) -> io::Result<Self::Output> {
        match buf.read_u8()? {
            1 => {
                // Deserialize to Get Request
                let key = decode_string(buf)?;
                Ok(Request::Get { key, })
            },
            2 => {
                // Deserialize to Set Request
                let key = decode_string(buf)?;
                let value = decode_string(buf)?;
                Ok(Request::Set { key, value })
            },
            3 => {
                let key = decode_string(buf)?;
                Ok(Request::Remove { key })
            },
            _ => { Err(io::Error::new(io::ErrorKind::InvalidData, "invalid data"))}
        }
    }
}

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
pub enum Response {
    Get {
        value: String,
        success: u8, 
        err_msg: String,
    }, 

    Set {
        success: u8,
        err_msg: String,
    },

    Remove {
        success: u8,
        err_msg: String,
    }
}

impl From<&Response> for u8 {
    fn from(value: &Response) -> Self {
        match value {
            Response::Get { .. } => 1,
            Response::Set { .. } => 2,
            Response::Remove { .. } => 3,
        }
    }
}

impl Serialize for Response {
    fn serialize(&self, buf: &mut impl Write) -> io::Result<usize> {
        buf.write_u8(self.into())?;
        let mut bytes_written: usize = 1;
        match self {
            Response::Get { value, success, err_msg } => {
                bytes_written += encode_string(buf, value)?;
                buf.write_u8(*success)?;
                bytes_written += encode_string(buf, err_msg)?;
            },
            Response::Set { success, err_msg } => {
                buf.write_u8(*success)?;
                bytes_written += encode_string(buf, err_msg)?;
            },
            Response::Remove { success, err_msg } => {
                buf.write_u8(*success)?;
                bytes_written += encode_string(buf, err_msg)?;
            }
        }
        Ok(bytes_written)
    }
}

impl Deserialize for Response {
    type Output = Response;

    fn deserialize(buf: &mut impl Read) -> io::Result<Self::Output> {
        match buf.read_u8()? {
            1 => {
                let value = decode_string(buf)?;
                let success = buf.read_u8()?;
                let err_msg = decode_string(buf)?;
                Ok(Response::Get { value, success, err_msg, })
            },
            2 => {
                let success = buf.read_u8()?;
                let err_msg = decode_string(buf)?;
                Ok(Response::Set { success, err_msg, })
            },
            3 => {
                let success = buf.read_u8()?;
                let err_msg = decode_string(buf)?;
                Ok(Response::Remove { success, err_msg, })
            }, 
            _ => { Err(io::Error::new(io::ErrorKind::InvalidData, "invalid data"))}
        }
    }
}

impl fmt::Display for Response {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Response::Get { value, success, err_msg } => write!(f, "Response value: {:?} success: {:?} message: {}", value, success, err_msg),
            Response::Set { success, err_msg } => write!(f, "Response success: {:?} message: {}", success, err_msg),
            Response::Remove { success, err_msg } => write!(f, "Response success: {:?} message: {}", success, err_msg),
        }
    }
}

/// Abstracted protocol wraps a TcpStream
pub struct Protocol {
    reader: BufReader<TcpStream>,
    stream: TcpStream,
}

impl Protocol {
    pub fn with_stream(stream: TcpStream) -> io::Result<Self> {
        Ok (
        Protocol{ 
            reader: BufReader::new(stream.try_clone()?),
            stream,
        })
    }

    pub fn send_messsage(&mut self, message: &impl Serialize) -> io::Result<usize> {
        let length = message.serialize(&mut self.stream)?;
        self.stream.flush();
        Ok(length)
    } 

    pub fn read_message<T: Deserialize>(&mut self) -> io::Result<T::Output> {
        T::deserialize(&mut self.reader)
    }
}