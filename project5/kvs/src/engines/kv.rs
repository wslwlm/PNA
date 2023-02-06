use std::borrow::{BorrowMut, Borrow};
use std::sync::RwLock;
use std::{fmt};
use std::fs::{self, OpenOptions};
use std::fs::File;
use std::collections::HashMap;
use serde::{Serialize, Deserialize};
use serde_json::Deserializer;
use std::io::{self, Seek, SeekFrom, Write, Read, BufReader, BufWriter};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, atomic::{AtomicU64}};
use dashmap::DashMap;
use std::cell::{RefCell};
use std::sync::atomic::Ordering;
use log::{info, error};
use tokio::sync::oneshot;
use futures::Future;
use std::pin::Pin;

use crate::error;
use crate::{KvsEngine, KvError, Result, thread_pool::ThreadPool};

const COMPACTION_LIMIT: u64 = 1024 * 1024;

// #[derive(Clone)]
// pub struct KvStore(Arc<RwLock<ShardKvStore>>);

// pub struct ShardKvStore {
//     path: PathBuf,
//     // current log number
//     curr_gen: u64,
//     // map string key -> command position
//     index_map: HashMap<String, CommandPos>,
//     // map log number -> log file reader
//     reader_map: Arc<Mutex<HashMap<u64, BufReaderWithPos<File>>>>,
//     // current log file writer
//     writer: BufWriterWithPos<File>,
//     // redundant bytes number
//     uncompacted: u64,
// }

#[derive(Clone)]
pub struct KvStore<P: ThreadPool> {
    // concurrent map string key -> command pos
    index_map: Arc<DashMap<String, CommandPos>>,
    // kv writer
    kv_writer: Arc<Mutex<KvWriter>>,
    // kv reader
    kv_reader: KvReader,
    // writer thread pool
    pool: P,
}

impl<P: ThreadPool> KvStore<P> {
    /// open a kv-store with a given directory
    pub fn open(dir: impl Into<PathBuf>, concurrency: usize) -> Result<Self> {
        let dir_buf = Arc::new(dir.into());
        let path = dir_buf.as_path();
        let gen_list: Vec<u64> = sorted_gen_list(path)?;

        let mut reader_map: HashMap<u64, BufReaderWithPos<File>> = HashMap::new();
        let mut index_map = Arc::new(DashMap::new());
        
        let mut curr_gen = 0;
        let mut uncompacted = 0;
        for &gen in gen_list.iter() {
            let f = File::open(log_path(path, gen))?;
            let mut reader = BufReaderWithPos::new(f);
            uncompacted += load(gen, &mut index_map, &mut reader)?;
            reader_map.insert(gen, reader);
            curr_gen = gen;
        }
        let first_gen = *gen_list.first().unwrap_or(&0);

        // every open create a new log file
        // TODO(wsl): BufReader & BufWriter -> the same file (cursor not share)
        curr_gen += 1;
        let writer = new_log_file(path, curr_gen)?;
        let f = File::open(log_path(path, curr_gen))?;
        let reader = BufReaderWithPos::new(f);
        reader_map.insert(curr_gen, reader);

        let safe_point = Arc::new(AtomicU64::new(first_gen));

        Ok(
            KvStore { 
                index_map: index_map.clone(),
                kv_writer: Arc::new(Mutex::new(KvWriter {
                    path: dir_buf.clone(),
                    curr_gen,
                    safe_point: safe_point.clone(),
                    writer,
                    index_map: index_map.clone(),
                    reader_map,
                    uncompacted,
                })),
                kv_reader: KvReader { 
                    path: dir_buf,
                    safe_point, 
                    index_map,
                    reader_map: Mutex::new(HashMap::new())
                },
                pool: P::new(concurrency)?,
            }
        )
    }
    
}

impl<P: ThreadPool> KvsEngine for KvStore<P> {
    fn set(&self, key: String, value: String) -> Pin<Box<dyn Future<Output = Result<()>> + Send>> {
        let writer = self.kv_writer.clone();
        let (tx, rx) = oneshot::channel();
        self.pool.spawn(move || {
            let res = writer.lock().unwrap().set(key, value);
            if tx.send(res).is_err() {
                error!("Receiving end is dropped");
            }
        });
        
        Box::pin(
            async move {
                rx.await.unwrap()
            }
        )
    }

    fn remove(&self, key: String) -> Pin<Box<dyn Future<Output = Result<()>> + Send>> {
        let writer = self.kv_writer.clone();
        let (tx, rx) = oneshot::channel();
        self.pool.spawn(move || {
            let res = writer.lock().unwrap().remove(key);
            if tx.send(res).is_err() {
                error!("Receiving end is dropped");
            }
        });
        
        Box::pin(
            async move {
                rx.await.unwrap()
            }
        )
    }

    fn get(&self, key: String) -> Result<Option<String>> {
        self.kv_reader.get(key)
    }
}

pub struct KvWriter {
    path: Arc<PathBuf>,
    // current log number
    curr_gen: u64,
    // safe point to sync the compaction gen number
    safe_point: Arc<AtomicU64>,
    // current log file writer
    writer: BufWriterWithPos<File>,
    // concurrent map string key -> command pos
    index_map: Arc<DashMap<String, CommandPos>>,
    // map u64 -> buf reader
    reader_map: HashMap<u64, BufReaderWithPos<File>>,
    // redundant bytes number
    uncompacted: u64,
}

impl KvWriter {
    /// Set the value of a string key to string
    pub fn set(&mut self, key: String, value: String) -> Result<()> {
        let cmd = serde_json::to_vec(&Command::Set { key: key.clone(), value })?;
        let pos = self.writer.pos;
        // TODO(wsl): How to guarantee the atomicity of writing?
        let len = self.writer.write(&cmd)?;
        self.writer.flush()?;

        // old command log redundant
        if let Some(old_cmd) = self.index_map.insert(key, CommandPos { gen: self.curr_gen, pos, len: len as u64 }) {
            self.uncompacted += old_cmd.len;
        }

        if self.uncompacted > COMPACTION_LIMIT {
            self.compaction()?;
        }
        Ok(())
    }

    /// remove the value of the string key
    pub fn remove(&mut self, key: String) -> Result<()> {
        // only existent key need to remove
        if self.index_map.contains_key(&key) {
            let cmd = serde_json::to_vec(&Command::Remove { key: key.clone() })?;
            let len = self.writer.write(&cmd)?;
            self.writer.flush()?;

            self.uncompacted += self.index_map.get(&key).map(|e| e.value().len).unwrap_or(0) + (len as u64);
            self.index_map.remove(&key);

            if self.uncompacted > COMPACTION_LIMIT {
                self.compaction()?;
            }
            Ok(())
        } else {
            Err(KvError::KeyNotFound)
        }
    }

    /// compact out-of-date log
    fn compaction(&mut self) -> Result<()> {
        let dir = self.path.as_path();
        let temp_gen = self.curr_gen + 1;
        let mut writer = new_log_file(dir, temp_gen)?;

        // index_map -> currently valid (key, value)
        for mut cmd_pos in self.index_map.iter_mut() {
            if let Some(reader) = self.reader_map.get_mut(&cmd_pos.gen) {
                // move reader to log pointer and read command
                reader.seek(SeekFrom::Start(cmd_pos.pos))?;
                let mut buf = vec![0; cmd_pos.len as usize];
                reader.read_exact(&mut buf)?;

                let pos = writer.pos;
                writer.write_all(&buf)?;

                cmd_pos.gen = temp_gen;
                cmd_pos.pos = pos;
            } else {
                // println!("tempgen: {} gen: {}", temp_gen, cmd_pos.gen);
                // println!("index map: {:?} reader_map: {:?}", self.index_map, self.reader_map.keys());
                return Err(KvError::ReaderNotFound);
            }
        }
        // flush written log after compaction finish
        writer.flush()?;

        // create compaction log file reader
        let reader = BufReaderWithPos::new(File::open(log_path(dir, temp_gen))?);
        self.reader_map.insert(temp_gen,  reader);

        // store the current gen number
        self.safe_point.store(temp_gen, Ordering::SeqCst);

        // create new log file writer & reader
        self.curr_gen += 2;
        self.writer = new_log_file(dir, self.curr_gen)?;
        let reader = BufReaderWithPos::new(File::open(log_path(dir, self.curr_gen))?);
        self.reader_map.insert(self.curr_gen,  reader);

        // remove stale files
        // The file cannot be removed immediately because the `KvReader` still keep the file handle.
        // When `KvReader` used next, it will clear the file handle 
        let stale_gens = sorted_gen_list(&self.path)?.into_iter().filter(|gen| *gen < temp_gen);
        for gen in stale_gens {
            self.reader_map.remove(&gen);
            fs::remove_file(log_path(dir, gen))?;
        }

        self.uncompacted = 0;
        Ok(())
    }
}

pub struct KvReader {
    path: Arc<PathBuf>,
    // safe point to sync the compaction gen number
    safe_point: Arc<AtomicU64>,
    // concurrent map string key -> command pos
    index_map: Arc<DashMap<String, CommandPos>>,
    // map gen number -> bufReader
    reader_map: Mutex<HashMap<u64, BufReaderWithPos<File>>>,
}

impl KvReader {
    /// Get the value of the string key
    pub fn get(&self, key: String) -> Result<Option<String>> {
        let safe_point = self.safe_point.load(Ordering::SeqCst);
        
        // println!("reader_map: {:?} safe point: {}", self.reader_map.borrow().keys(), safe_point);
        // remove the stale file handle
        self.reader_map.lock().unwrap().retain(|&k, _| k >= safe_point);
        
        if let Some(cmd_pos) = self.index_map.get(&key) {
            let cmd_pos = cmd_pos.value();
            // if the reader hashmap not contains the key, open corresponding file ans create the buf reader
            let mut readers = self.reader_map.lock().unwrap();
            if !readers.contains_key(&cmd_pos.gen) { 
                let file = File::open(log_path(self.path.as_path(), cmd_pos.gen))?;
                let reader = BufReaderWithPos::new(file);
                readers.insert(cmd_pos.gen, reader);
            }
            
            let reader = readers.get_mut(&cmd_pos.gen).unwrap();
            // move reader to log pointer and read command
            reader.seek(SeekFrom::Start(cmd_pos.pos))?;
            let mut buf = vec![0; cmd_pos.len as usize];
            reader.read_exact(&mut buf)?;
            let cmd: Command = serde_json::from_slice(&buf)?;
            // println!("cmd: {}", cmd);
            match cmd {
                Command::Set {key: _, value} => {
                    Ok(Some(value))
                },
                Command::Remove { key: _ } => {
                    Ok(None)
                }
            }
        } else {
            Ok(None)
        }
        // Ok(None)
    }
}

// create a new reader map
impl Clone for KvReader {
    fn clone(&self) -> Self {
        KvReader { 
            path: self.path.clone(),
            safe_point: self.safe_point.clone(),
            index_map: self.index_map.clone(),
            reader_map: Mutex::new(HashMap::new()),
        }
    }
}

fn log_path(dir: &Path, gen: u64) -> PathBuf {
    dir.join(format!("{}", gen))
}

fn new_log_file(dir: &Path, gen: u64) -> Result<BufWriterWithPos<File>> {
    let f = OpenOptions::new().create(true).append(true).open(log_path(dir, gen))?;
    Ok(BufWriterWithPos::new(f))
}

/// sort the log file number in ascending order
fn sorted_gen_list(path: &Path) -> Result<Vec<u64>> {
    let mut list = fs::read_dir(path)?
        .filter_map(|res| res.map(|e| e.path()).ok())
        .filter(|res| res.extension().is_none() && res.is_file())
        .filter_map(|res| res.file_name().unwrap().to_str().unwrap().parse::<u64>().ok())
        .collect::<Vec<_>>();

    list.sort();
    Ok(list)
}

/// load log file and fill the index map
fn load(gen: u64, index_map: &mut Arc<DashMap<String, CommandPos>>, reader: &mut BufReaderWithPos<File>) -> Result<u64> {
    // Creates a JSON deserializer from an io::Read
    let mut stream = Deserializer::from_reader(reader).into_iter::<Command>();
    // Get current position 
    let mut offset = stream.byte_offset();
    let mut uncompacted = 0;

    while let Some(cmd) = stream.next() {
        let c = cmd?;
        // println!("command: {}", c);
        let curr_offset = stream.byte_offset();
        match c {
            Command::Set{key, value: _} => {
                if let Some(old_cmd) = index_map.insert(key, CommandPos {
                    gen,
                    pos: offset as u64,
                    len: (curr_offset - offset) as u64,
                }) {
                    uncompacted += old_cmd.len;
                }
            },
            Command::Remove { key } => {
                if let Some(old_cmd) = index_map.remove(&key) {
                    uncompacted += old_cmd.1.len;
                }
                uncompacted += (curr_offset - offset) as u64;
            }
        }
        offset = curr_offset;
    }
    Ok(uncompacted)
}

#[derive(Serialize, Deserialize)]
pub enum Command {
    Set { key: String, value: String },
    Remove { key: String },
}

impl fmt::Display for Command {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        // match reference 不需要在enum前面加&，key，value both reference
        // 具体见https://rust-lang.github.io/rfcs/2005-match-ergonomics.html
        match self {
            Command::Set {key, value} => write!(f, "Set ({}, {})", key, value),
            Command::Remove {key} => write!(f, "Remove {}", key)
        }
    }
}

/// gen -> file number, pos: file position, len: command length
#[derive(Debug)]
pub struct CommandPos {
    gen: u64,
    pos: u64,
    len: u64
}

impl fmt::Display for CommandPos {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "gen: {} pos: {}, len: {}", self.gen, self.pos, self.len)
    }
}

pub struct BufReaderWithPos<R: Read + Seek> {
    reader: BufReader<R>,
    pos: u64,
}

impl<R: Read + Seek> BufReaderWithPos<R> {
    pub fn new(mut inner: R) -> Self {
        let pos = inner.seek(SeekFrom::Current(0)).unwrap();
        BufReaderWithPos {
            reader: BufReader::new(inner),
            pos,
        }
    }
}

impl<R: Read + Seek> Read for BufReaderWithPos<R> {  
    fn read(&mut self, buf: &mut [u8]) -> std::result::Result<usize, io::Error> {
        let res = self.reader.read(buf)?;
        self.pos += buf.len() as u64;
        Ok(res)
    }
}

impl<R: Read + Seek> Seek for BufReaderWithPos<R> {
    fn seek(&mut self, pos: SeekFrom) -> std::result::Result<u64, io::Error> {
        self.pos = self.reader.seek(pos)?;
        Ok(self.pos)
    }
}

pub struct BufWriterWithPos<R: Write + Seek> {
    writer: BufWriter<R>,
    pos: u64,
}

impl<W: Write + Seek> BufWriterWithPos<W> {
    pub fn new(mut inner: W) -> Self {
        let pos = inner.seek(SeekFrom::Current(0)).unwrap();
        BufWriterWithPos {
            writer: BufWriter::new(inner),
            pos,
        }
    }
}

impl<W: Write + Seek> Write for BufWriterWithPos<W> {
    fn write(&mut self, buf: &[u8]) -> std::result::Result<usize, io::Error> {
        let res = self.writer.write(buf)?;
        self.pos += buf.len() as u64;
        Ok(res)
    }

    fn flush(&mut self) -> std::result::Result<(), io::Error> {
        self.writer.flush()
    }
}

impl<W: Write + Seek> Seek for BufWriterWithPos<W> {
    fn seek(&mut self, pos: SeekFrom) -> std::result::Result<u64, io::Error> {
        self.pos = self.writer.seek(pos)?;
        Ok(self.pos)
    }
}