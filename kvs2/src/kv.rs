extern crate failure;

use std::io::{Write, BufWriter, SeekFrom, BufReader, Read, ErrorKind};
use std::path::PathBuf;
use failure::Error;
use failure::format_err;
use std::fs;
use std::collections::HashMap;
use std::io::Seek;

pub struct KvStore {
    path_buf: PathBuf,
    index_map: HashMap<String, ValueOffset>, 
}

pub struct ValueOffset {
    value_size: u64,
    value_offset: u64,
}

// #[derive(Fail, Copy, Eq, Debug)]
// pub enum MyError {
//     #[fail(display = "open path is not dir")]
//     PathError,
// }

pub type Result<T> = std::result::Result<T, Error>;

impl KvStore {
    pub fn new(path_buf: PathBuf) -> Self {
        KvStore {
            path_buf,
            index_map: HashMap::new(),
        }
    }

    pub fn open(path: impl Into<PathBuf>) -> Result<KvStore> {
        let dir_path_buf = path.into();
        let dir_path = dir_path_buf.as_path();
        if !dir_path.is_dir() {
            return  Err(format_err!("open path is not a directory"));
        }

        let mut kvs = KvStore::new(dir_path_buf.clone());
        let log_path = dir_path.join(".log");

        if log_path.exists() {
            let f = fs::File::open(log_path)?;
            let mut reader = BufReader::new(f);

            loop {
                let record_offset = reader.seek(SeekFrom::Current(0)).expect ("Could not get current position!");

                let mut key_size_bytes: [u8; 4] = [0; 4];
                let mut value_size_bytes: [u8; 4] = [0; 4];
                if let Err(e) = reader.read_exact(&mut key_size_bytes) {
                    if e.kind() == ErrorKind::UnexpectedEof {
                        break;
                    }
                }
                reader.read_exact(&mut value_size_bytes)?;
                let key_size = u32::from_be_bytes(key_size_bytes);
                let value_size = u32::from_be_bytes(value_size_bytes);
                // println!("key size: {}, value size: {}", key_size, value_size);

                let mut key_bytes: Vec<u8> = vec![0; key_size as usize];
                reader.read_exact(&mut key_bytes)?;
                let key: String = serde_json::from_slice(&key_bytes)?;
                // tombstone flag
                if value_size == 0 {
                    kvs.index_map.remove(&key);
                    continue;
                }

                let mut value_bytes: Vec<u8> = vec![0; value_size as usize];
                reader.read_exact(&mut value_bytes)?;
                // let value: String = serde_json::from_slice(&value_bytes)?;
                // println!("key: {}, value: {}", key, value);

                kvs.index_map.insert(key, ValueOffset {
                    value_size: value_size as u64,
                    value_offset: record_offset,
                });
            }
        }

        Ok(kvs)
    }

    pub fn get(&mut self, key: String) -> Result<Option<String>> {
        match self.index_map.get(&key) {
            Some(value_offset) => {
                let value_size = value_offset.value_size;
                let record_offset = value_offset.value_offset;
                // println!("value size: {}, record offset: {}", value_size, record_offset);
                
                let log_path = self.path_buf.join(".log");
                let f = fs::File::open(log_path)?;
                let mut reader = BufReader::new(f);
                reader.seek(SeekFrom::Start(record_offset))?;

                let mut key_bytes: [u8; 4] = [0; 4];
                reader.read_exact(&mut key_bytes)?;
                let key_size = u32::from_be_bytes(key_bytes);
                // println!("key size: {}", key_size);

                reader.seek(SeekFrom::Current(key_size as i64 + 4))?;
                let mut serialized_value = vec![0; value_size as usize];
                reader.read_exact(&mut serialized_value)?;
                let value = serde_json::from_slice(&serialized_value)?;
                
                Ok(Some(value))
            },
            None => {
                Ok(None)
            }
        }

    }

    pub fn set(&mut self, key: String, value: String) -> Result<()> {
        let log_path = self.path_buf.join(".log");
        let f = fs::OpenOptions::new().create(true).append(true).open(log_path)?;
        let mut writer = BufWriter::new(f);
        let record_offset = writer.seek(SeekFrom::End(0)).expect ("Could not get current position!");

        // layout: key_sz | key | value_sz | value
        let serialized_key = serde_json::to_vec(&key)?;
        let key_size: [u8; 4] = (serialized_key.len() as u32).to_be_bytes();

        let serialized_value = serde_json::to_vec(&value)?;
        let value_size: [u8; 4] = (serialized_value.len() as u32).to_be_bytes();
        
        // TODO(wsl): how to combine the multiple writes into one write
        writer.write_all(&key_size)?;
        writer.write_all(&value_size)?;
        writer.write_all(&serialized_key)?;
        writer.write_all(&serialized_value)?;
        writer.flush()?;

        self.index_map.insert(key, ValueOffset { 
            value_size: serialized_value.len() as u64, 
            value_offset: record_offset });
        Ok(())
    }

    pub fn remove(&mut self, key: String) -> Result<()> {
        if self.index_map.get(&key).is_none() {
            return Err(format_err!("Key not found"));
        }

        let log_path = self.path_buf.join(".log");
        let f = fs::OpenOptions::new().create(true).append(true).open(log_path)?;
        let mut writer = BufWriter::new(f);

        // layout: key_sz | key | value_sz | value
        let serialized_key = serde_json::to_vec(&key)?;
        let key_size: [u8; 4] = (serialized_key.len() as u32).to_be_bytes();

        let value_size: [u8; 4] = [0; 4];
        
        // TODO(wsl): how to combine the multiple writes into one write
        writer.write_all(&key_size)?;
        writer.write_all(&value_size)?;
        writer.write_all(&serialized_key)?;
        writer.flush()?;

        self.index_map.remove(&key);
        Ok(())
    }
}