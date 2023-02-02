use crate::Result;

pub trait ThreadPool {
    fn new(threads: usize) -> Result<Self> where Self: Sized;

    fn spawn<F>(&self, job: F) where F: FnOnce() + Send + 'static;
}

mod shared_queue;
mod rayon;

pub use self::shared_queue::SharedQueueThreadPool;
pub use self::rayon::RayonThreadPool;