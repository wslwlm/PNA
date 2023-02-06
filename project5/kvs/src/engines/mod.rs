use futures::{Future};
use std::pin::Pin;
pub use crate::{KvError, Result};

/// Clone + Send + 'static supertraits
pub trait KvsEngine: Clone + Send + Sync + 'static {
    fn set(&self, key: String, value: String) -> Pin<Box<dyn Future<Output = Result<()>> + Send>>;
    fn get(&self, key: String) -> Result<Option<String>>;
    fn remove(&self, key: String) -> Pin<Box<dyn Future<Output = Result<()>> + Send>>;
}


mod kv;
mod sled;

pub use self::kv::{KvStore};
pub use self::sled::{SledEngine};