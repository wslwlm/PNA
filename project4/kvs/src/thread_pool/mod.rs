use crate::Result;

pub trait ThreadPool {
    fn new(threads: usize) -> Result<Self> where Self: Sized;

    fn spawn<F>(&self, job: F) where F: FnOnce() + Send + 'static;
}

mod naivethreadpool;
mod rayon;

pub use self::naivethreadpool::NaiveThreadPool;
pub use self::rayon::RayonThreadPool;