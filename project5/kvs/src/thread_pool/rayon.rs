use rayon;
use super::ThreadPool;
use crate::Result;
use std::sync::Arc;

#[derive(Clone)]
pub struct RayonThreadPool(Arc<rayon::ThreadPool>);

impl ThreadPool for RayonThreadPool {
    fn new(threads: usize) -> Result<Self> {
        Ok(RayonThreadPool(Arc::new(rayon::ThreadPoolBuilder::new().num_threads(threads).build()?)))
    }

    fn spawn<F>(&self, job: F) where F: FnOnce() + Send + 'static {
        self.0.spawn(job);
    }
}