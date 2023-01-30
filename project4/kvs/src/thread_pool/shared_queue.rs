use log::{info, warn};
use sled::Shared;

use crate::{Result};
use super::ThreadPool;
use std::{
    sync::{mpsc, Arc, Mutex},
    thread, panic::{self},
};

pub struct Worker {
    id: usize,
    thread: Option<thread::JoinHandle<()>>,
}

impl Worker {
    pub fn new(id: usize, receiver: Arc<Mutex<mpsc::Receiver<Job>>>) -> Self {
        let thread = thread::spawn(move || loop {
            let message = receiver.lock().unwrap().recv();

            match message {
                Ok(job) => {
                    info!("Worker {id} get an executing job");

                    // What is the AssertUnwindSafe use?
                    // Is it safe to use here?
                    let wrapper = panic::AssertUnwindSafe(job);
                    if let Err(e) = panic::catch_unwind(wrapper) {
                        warn!("job execute panic error: {:?}", e);
                    }
                    // job();
                },
                Err(e) => {
                    info!("Worker {id} disconnected, shutdown");
                    break;
                }
            }
        });

        Worker { id, thread: Some(thread), }
    }
}

pub struct SharedQueueThreadPool {
    workers: Vec<Worker>,
    sender: Option<mpsc::Sender<Job>>,
}

type Job = Box<dyn FnOnce() + Send + 'static>;

impl ThreadPool for SharedQueueThreadPool {
    fn new(threads: usize) -> Result<Self> {
        let (sender, receiver) = mpsc::channel();
        let receiver = Arc::new(Mutex::new(receiver));
        let mut workers = Vec::with_capacity(threads);

        for id in 0..threads {
            workers.push(Worker::new(id, Arc::clone(&receiver)));
        }

        Ok(SharedQueueThreadPool {
            workers,
            sender: Some(sender),
        })
    }

    fn spawn<F>(&self, job: F) where F: FnOnce() + Send + 'static {
        let f = Box::new(job);

        self.sender.as_ref().unwrap().send(f).unwrap();
    }
}

impl Drop for SharedQueueThreadPool {
    fn drop(&mut self) {
        drop(self.sender.take());

        for worker in self.workers.iter_mut() {
            info!("shutting down worker {}", worker.id);

            if let Some(thread) = worker.thread.take() {
                thread.join().unwrap();
            }
        }
    }
}