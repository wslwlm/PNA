use crate::thread_pool::ThreadPool;
use crate::{KvsEngine, KvError, Result};
use tokio::sync::oneshot;
use sled::{self, Db, Tree};
use std::path::PathBuf;
use std::pin::Pin;
use futures::Future;
use std::sync::Arc;
use log::{error};

#[derive(Clone)]
pub struct SledEngine<P: ThreadPool> {
    db: Arc<Db>,
    pool: P,
}

impl<P: ThreadPool> SledEngine<P> {
    pub fn open(dir: impl Into<PathBuf>, concurrency: usize) -> Result<impl KvsEngine> {
        let db = sled::open(dir.into())?;
        Ok(SledEngine {
            db: Arc::new(db),
            pool: P::new(concurrency)?,
        })
    }
}

impl<P: ThreadPool> KvsEngine for SledEngine<P> {
    fn set(&self, key: String, value: String) -> Pin<Box<dyn Future<Output = Result<()>> + Send>> {
        let tree = self.db.clone();
        let (tx, rx) = oneshot::channel();
        self.pool.spawn(move || {
            let res = (|| {
                tree.insert(key, value.into_bytes()).map(|_| ())?;
                tree.flush()?;
                Ok(())
            })();

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

    fn remove(&self, key: String) -> Pin<Box<dyn Future<Output = Result<()>> + Send>> {
        let tree = self.db.clone();
        let (tx, rx) = oneshot::channel();
        self.pool.spawn(move || {
            let res = (|| {
                tree.remove(key)?.ok_or(KvError::KeyNotFound)?;
                tree.flush()?;
                Ok(())
            })();

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
}