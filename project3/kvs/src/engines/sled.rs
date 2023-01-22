use crate::{KvsEngine, KvError, Result};
use sled::{self, Db, Tree};
use std::path::PathBuf;
use std::io;

pub struct SledEngine {
    db: Db,
}

impl SledEngine {
    pub fn open(dir: impl Into<PathBuf>) -> Result<impl KvsEngine> {
        let db = sled::open(dir.into())?;
        Ok(SledEngine {
            db,
        })
    }
}

impl KvsEngine for SledEngine {
    fn set(&mut self, key: String, value: String) -> Result<()> {
        let tree: &Tree = &self.db;
        tree.insert(key, value.into_bytes()).map(|_| ())?;
        tree.flush()?;
        Ok(())
    }

    fn get(&mut self, key: String) -> Result<Option<String>> {
        let tree: &Tree = &self.db;
        // if let Some(vec) = tree.get(key)? {
        //     let v = String::from_utf8((*vec).try_into().unwrap())
        //         .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "invalid data"))?;
        //     Ok(Some(v))
        // } else {
        //     Ok(None)
        // }

        Ok(tree
            .get(key)?
            .map(|i_vec| AsRef::<[u8]>::as_ref(&i_vec).to_vec())
            .map(String::from_utf8)
            .transpose()?)
    }

    fn remove(&mut self, key: String) -> Result<()> {
        let tree: &Tree = &self.db;
        tree.remove(key)?.ok_or(KvError::KeyNotFound)?;
        tree.flush()?;
        Ok(())
    }
}