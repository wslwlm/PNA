use rayon;
use super::ThreadPool;
use crate::Result;

pub struct RayonThreadPool(rayon::ThreadPool);

impl ThreadPool for RayonThreadPool {
    fn new(threads: usize) -> Result<Self> {
        Ok(RayonThreadPool(rayon::ThreadPoolBuilder::new().num_threads(threads).build()?))
    }

    fn spawn<F>(&self, job: F) where F: FnOnce() + Send + 'static {
        self.0.spawn(job);
    }
}