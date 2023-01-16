use std::fmt;
use std::fs::{self, OpenOptions};
use std::fs::File;
use std::collections::HashMap;
use serde::{Serialize, Deserialize};
use serde_json::Deserializer;
use failure::{Fail};
use std::io::{self, Seek, SeekFrom, Write, Read, BufReader, BufWriter};
use std::path::{Path, PathBuf};

const COMPACTION_LIMIT: u64 = 1024 * 1024;

#[derive(Fail, Debug)]
pub enum MyError {
    #[fail(display = "{}", _0)]
    Io(#[cause] io::Error),
    
    #[fail(display = "{}", _0)]
    Se(#[cause] serde_json::Error),

    #[fail(display = "Key not found")]
    KeyNotFound,

    #[fail(display = "Reader not found")]
    ReaderNotFound,
}

/// io::Error -> MyError
impl From<io::Error> for MyError {
    fn from(error: io::Error) -> Self {
        MyError::Io(error)
    }
}

/// serde_jsonL::Error -> MyError
impl From<serde_json::Error> for MyError {
    fn from(error: serde_json::Error) -> Self {
        MyError::Se(error)
    }
}

pub type Result<T> = std::result::Result<T, MyError>;

pub struct KvStore {
    path: PathBuf,
    // current log number
    curr_gen: u64,
    // map string key -> command position
    index_map: HashMap<String, CommandPos>,
    // map log number -> log file reader
    reader_map: HashMap<u64, BufReaderWithPos<File>>,
    // current log file writer
    writer: BufWriterWithPos<File>,
    // redundant bytes number
    uncompacted: u64,
}

impl KvStore {
    /// open a kv-store with a given directory
    pub fn open(dir: impl Into<PathBuf>) -> Result<KvStore> {
        let dir_buf = dir.into();
        let path = dir_buf.as_path();
        let gen_list: Vec<u64> = sorted_gen_list(path)?;

        let mut reader_map: HashMap<u64, BufReaderWithPos<File>> = HashMap::new();
        let mut index_map: HashMap<String, CommandPos> = HashMap::new();
        
        let mut curr_gen = 0;
        let mut uncompacted = 0;
        for &gen in gen_list.iter() {
            let f = File::open(log_path(path, gen))?;
            let mut reader = BufReaderWithPos::new(f);
            uncompacted += load(gen, &mut index_map, &mut reader)?;
            reader_map.insert(gen, reader);
            curr_gen = gen;
        }

        // every open create a new log file
        // TODO(wsl): BufReader & BufWriter -> the same file (cursor not share)
        curr_gen += 1;
        let writer = new_log_file(path, curr_gen)?;
        let f = File::open(log_path(path, curr_gen))?;
        let reader = BufReaderWithPos::new(f);
        reader_map.insert(curr_gen, reader);
    
        Ok(
            KvStore { 
                path: dir_buf, 
                curr_gen,
                index_map,
                reader_map,
                writer,
                uncompacted,
            }
        )
    }

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
        if let Some(old_cmd) = self.index_map.get(&key) {
            let cmd = serde_json::to_vec(&Command::Remove { key: key.clone() })?;
            let len = self.writer.write(&cmd)?;
            self.writer.flush()?;

            self.uncompacted += old_cmd.len + (len as u64);
            self.index_map.remove(&key);

            if self.uncompacted > COMPACTION_LIMIT {
                self.compaction()?;
            }
            Ok(())
        } else {
            Err(MyError::KeyNotFound)
        }
    }

    /// Get the value of the string key
    pub fn get(&mut self, key: String) -> Result<Option<String>> {
        if let Some(cmd_pos) = self.index_map.get(&key) {
            if let Some(reader) = self.reader_map.get_mut(&cmd_pos.gen) {
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
                Err(MyError::ReaderNotFound)
            }
        } else {
            Ok(None)
        }
        // Ok(None)
    }

    fn compaction(&mut self) -> Result<()> {
        let dir = self.path.as_path();
        let temp_gen = self.curr_gen + 1;
        let mut writer = new_log_file(dir, temp_gen)?;

        // index_map -> currently valid (key, value)
        for cmd_pos in self.index_map.values_mut() {
            if let Some(reader) = self.reader_map.get_mut(&cmd_pos.gen) {
                // move reader to log pointer and read command
                reader.seek(SeekFrom::Start(cmd_pos.pos))?;
                let mut buf = vec![0; cmd_pos.len as usize];
                reader.read_exact(&mut buf)?;

                let pos = writer.pos;
                writer.write_all(&buf)?;
                writer.flush()?;

                cmd_pos.gen = temp_gen;
                cmd_pos.pos = pos;
            } else {
                // println!("tempgen: {} gen: {}", temp_gen, cmd_pos.gen);
                // println!("index map: {:?} reader_map: {:?}", self.index_map, self.reader_map.keys());
                return Err(MyError::ReaderNotFound);
            }
        }

        let mut new_reader_map = HashMap::new();
        // clear the reader_map
        // self.reader_map.clear();

        // create new log file reader
        let reader = BufReaderWithPos::new(File::open(log_path(dir, temp_gen))?);
        new_reader_map.insert(temp_gen,  reader);

        // create new log file writer
        self.curr_gen += 2;
        self.writer = new_log_file(dir, self.curr_gen)?;
        let reader = BufReaderWithPos::new(File::open(log_path(dir, self.curr_gen))?);
        new_reader_map.insert(self.curr_gen,  reader);

        // delete old file 
        for &gen in self.reader_map.keys() {
            fs::remove_file(log_path(dir, gen))?;
        }
        // replace old reader_map with new reader_map
        self.reader_map.clear();
        self.reader_map = new_reader_map;

        self.uncompacted = 0;
        Ok(())
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
fn load(gen: u64, index_map: &mut HashMap<String, CommandPos>, reader: &mut BufReaderWithPos<File>) -> Result<u64> {
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
                    uncompacted += old_cmd.len;
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