use std::fmt::format;
use std::{thread};
use log::{info, warn};
use env_logger::{Env};
use criterion::{criterion_group, criterion_main, Criterion};
use crossbeam_utils::sync::WaitGroup;
use kvs::thread_pool::{ThreadPool, SharedQueueThreadPool, RayonThreadPool};
use kvs::{Client, Server, KvStore, KvsEngine, SledEngine};
use sled::Shared;
use tempfile::TempDir;
use std::time::Duration;
use std::sync::{Arc, Once, atomic::{AtomicBool, Ordering}};

static START: Once = Once::new();

fn write_queued_kvstore(c: &mut Criterion) {
    START.call_once(|| {
        env_logger::Builder::from_env(Env::default().default_filter_or("warn")).init();
    });

    let mut group = c.benchmark_group("write_queued_kvstore");
    for i in &vec![1, 2, 4, 8, 16, 32, 64] {
        group.bench_with_input(format!("kvs_{}", i), i, |b, &i| {
            let temp_dir = TempDir::new().unwrap();
            let addr = "127.0.0.1:4000";
            let entries:usize = 1000;
            
            let is_stop = Arc::new(AtomicBool::new(false));
            let pool = SharedQueueThreadPool::new(i).unwrap();
            let store = KvStore::open(temp_dir.path()).unwrap();
            let mut server = Server::new(store, pool, is_stop.clone()).unwrap();
            let handle = thread::spawn(move || {
                server.run(addr.to_owned()).unwrap();
            });
            
            thread::sleep(Duration::from_secs(1));

            b.iter(|| {
                let mut handles = Vec::new();
                for j in 0..entries_len {
                    let handle = thread::spawn(move|| {
                        match Client::connect(addr.to_owned()) {
                            Ok(mut client) => {
                                match client.get(format!("key{}", j)) {
                                    Ok(value) => {
                                        assert_eq!(value, Some(format!("value{}", j)));
                                    },
                                    Err(e) => warn!("client set error: {:?}", e)
                                }
                            }, 
                            Err(e) => {
                                warn!("client connect error: {:?}", e);
                            }  
                        }
                    });
                    handles.push(handle);
                }
                for handle in handles {
                    handle.join().unwrap();
                }
            });

            is_stop.store(true, Ordering::SeqCst);

            let _ = Client::connect(addr).unwrap();
            if let Err(e) = handle.join() {
                warn!("server run error: {:?}", e);
            }
        });
        group.bench_with_input(format!("sled_{}", i), i, |b, &i| {
            let temp_dir = TempDir::new().unwrap();
            let addr = "127.0.0.1:4000";
            let entries:usize = 1000;
            
            let is_stop = Arc::new(AtomicBool::new(false));
            let pool = SharedQueuePool::new(i).unwrap();
            let store = SledEngine::open(temp_dir.path()).unwrap();
            let mut server = Server::new(store, pool, is_stop.clone()).unwrap();
            // create a new thread to run server
            let handle = thread::spawn(move || {
                server.run(addr.to_owned()).unwrap();
            });
            
            thread::sleep(Duration::from_secs(1));

            b.iter(|| {
                let mut handles = Vec::new();
                for j in 0..entries_len {
                    let handle = thread::spawn(move|| {
                        match Client::connect(addr.to_owned()) {
                            Ok(mut client) => {
                                match client.get(format!("key{}", j)) {
                                    Ok(value) => {
                                        assert_eq!(value, Some(format!("value{}", j)));
                                    },
                                    Err(e) => warn!("client set error: {:?}", e)
                                }
                            }, 
                            Err(e) => {
                                warn!("client connect error: {:?}", e);
                            }  
                        }
                    });
                    handles.push(handle);
                }
                for handle in handles {
                    handle.join().unwrap();
                }

            // stop server
            is_stop.store(true, Ordering::SeqCst);

            let _ = Client::connect(addr).unwrap();
            
            if let Err(e) = handle.join() {
                warn!("server run error: {:?}", e);
            }
        });
    }
}

fn write_rayon_kvstore(c: &mut Criterion) {
    START.call_once(|| {
        env_logger::Builder::from_env(Env::default().default_filter_or("warn")).init();
    });

    let mut group = c.benchmark_group("write_rayon_kvstore");
    for i in &vec![1, 2, 4, 8, 16, 32, 64] {
        group.bench_with_input(format!("kvs_{}", i), i, |b, &i| {
            let temp_dir = TempDir::new().unwrap();
            let addr = "127.0.0.1:4000";
            let entries:usize = 1000;
            
            let is_stop = Arc::new(AtomicBool::new(false));
            let pool = RayonThreadPool::new(i).unwrap();
            let store = KvStore::open(temp_dir.path()).unwrap();
            let mut server = Server::new(store, pool, is_stop.clone()).unwrap();
            let handle = thread::spawn(move || {
                server.run(addr.to_owned()).unwrap();
            });
            
            thread::sleep(Duration::from_secs(1));

            b.iter(|| {
                let mut handles = Vec::new();
                for j in 0..entries_len {
                    let handle = thread::spawn(move|| {
                        match Client::connect(addr.to_owned()) {
                            Ok(mut client) => {
                                match client.get(format!("key{}", j)) {
                                    Ok(value) => {
                                        assert_eq!(value, Some(format!("value{}", j)));
                                    },
                                    Err(e) => warn!("client set error: {:?}", e)
                                }
                            }, 
                            Err(e) => {
                                warn!("client connect error: {:?}", e);
                            }  
                        }
                    });
                    handles.push(handle);
                }
                for handle in handles {
                    handle.join().unwrap();
                }
            });

            is_stop.store(true, Ordering::SeqCst);

            let _ = Client::connect(addr).unwrap();
            if let Err(e) = handle.join() {
                warn!("server run error: {:?}", e);
            }
        });
        group.bench_with_input(format!("sled_{}", i), i, |b, &i| {
            let temp_dir = TempDir::new().unwrap();
            let addr = "127.0.0.1:4000";
            let entries:usize = 1000;
            
            let is_stop = Arc::new(AtomicBool::new(false));
            let pool = RayonThreadPool::new(i).unwrap();
            let store = SledEngine::open(temp_dir.path()).unwrap();
            let mut server = Server::new(store, pool, is_stop.clone()).unwrap();
            // create a new thread to run server
            let handle = thread::spawn(move || {
                server.run(addr.to_owned()).unwrap();
            });
            
            thread::sleep(Duration::from_secs(1));

            b.iter(|| {
                let mut handles = Vec::new();
                for j in 0..entries_len {
                    let handle = thread::spawn(move|| {
                        match Client::connect(addr.to_owned()) {
                            Ok(mut client) => {
                                match client.get(format!("key{}", j)) {
                                    Ok(value) => {
                                        assert_eq!(value, Some(format!("value{}", j)));
                                    },
                                    Err(e) => warn!("client set error: {:?}", e)
                                }
                            }, 
                            Err(e) => {
                                warn!("client connect error: {:?}", e);
                            }  
                        }
                    });
                    handles.push(handle);
                }
                for handle in handles {
                    handle.join().unwrap();
                }
            });
            // stop server
            is_stop.store(true, Ordering::SeqCst);

            let _ = Client::connect(addr).unwrap();
            
            if let Err(e) = handle.join() {
                warn!("server run error: {:?}", e);
            }
        });
    }
}

fn read_queued_kvstore(c: &mut Criterion) {
    START.call_once(|| {
        env_logger::Builder::from_env(Env::default().default_filter_or("warn")).init();
    });

    let mut group = c.benchmark_group("read_queued_kvstore");
    for i in &vec![1, 2, 4, 8, 16, 32, 64] {
        group.bench_with_input(format!("kvs_{}", i), i, |b, &i| {
            let temp_dir = TempDir::new().unwrap();
            let addr = "127.0.0.1:4000";
            let entries_len:usize = 1000;
            
            let is_stop = Arc::new(AtomicBool::new(false));
            let pool = SharePool::new(i).unwrap();
            // previous set the key/value pairs 
            let store = KvStore::open(temp_dir.path()).unwrap();
            for j in 0..entries_len {
                if let Err(e) = store.set(format!("key{}", j), format!("value{}", j)) {
                    warn!("store previous set value error: {:?}", e);
                }
            }

            let mut server = Server::new(store, pool, is_stop.clone()).unwrap();
            let handle = thread::spawn(move || {
                server.run(addr.to_owned()).unwrap();
            });
            
            thread::sleep(Duration::from_secs(1));

            b.iter(|| {
                let mut handles = Vec::new();
                for j in 0..entries_len {
                    let handle = thread::spawn(move|| {
                        match Client::connect(addr.to_owned()) {
                            Ok(mut client) => {
                                match client.get(format!("key{}", j)) {
                                    Ok(value) => {
                                        assert_eq!(value, Some(format!("value{}", j)));
                                    },
                                    Err(e) => warn!("client set error: {:?}", e)
                                }
                            }, 
                            Err(e) => {
                                warn!("client connect error: {:?}", e);
                            }  
                        }
                    });
                    handles.push(handle);
                }
                for handle in handles {
                    handle.join().unwrap();
                }
            });

            is_stop.store(true, Ordering::SeqCst);

            let _ = Client::connect(addr).unwrap();
            if let Err(e) = handle.join() {
                warn!("server run error: {:?}", e);
            }
        });
        group.bench_with_input(format!("sled_{}", i), i, |b, &i| {
            let temp_dir = TempDir::new().unwrap();
            let addr = "127.0.0.1:4000";
            let entries_len:usize = 1000;
            
            let is_stop = Arc::new(AtomicBool::new(false));
            let pool = SharedQueueThreadPool::new(i).unwrap();
            // previous set the key/value pairs 
            let store = SledEngine::open(temp_dir.path()).unwrap();
            for j in 0..entries_len {
                if let Err(e) = store.set(format!("key{}", j), format!("value{}", j)) {
                    warn!("store previous set value error: {:?}", e);
                }
            }

            let mut server = Server::new(store, pool, is_stop.clone()).unwrap();
            let handle = thread::spawn(move || {
                server.run(addr.to_owned()).unwrap();
            });
            
            thread::sleep(Duration::from_secs(1));

            b.iter(|| {
                let mut handles = Vec::new();
                for j in 0..entries_len {
                    let handle = thread::spawn(move|| {
                        match Client::connect(addr.to_owned()) {
                            Ok(mut client) => {
                                match client.get(format!("key{}", j)) {
                                    Ok(value) => {
                                        assert_eq!(value, Some(format!("value{}", j)));
                                    },
                                    Err(e) => warn!("client set error: {:?}", e)
                                }
                            }, 
                            Err(e) => {
                                warn!("client connect error: {:?}", e);
                            }  
                        }
                    });
                    handles.push(handle);
                }
                for handle in handles {
                    handle.join().unwrap();
                }
            });

            is_stop.store(true, Ordering::SeqCst);

            let _ = Client::connect(addr).unwrap();
            if let Err(e) = handle.join() {
                warn!("server run error: {:?}", e);
            }
        });
    }
}

fn read_rayon_kvstore(c: &mut Criterion) {
    START.call_once(|| {
        env_logger::Builder::from_env(Env::default().default_filter_or("warn")).init();
    });

    let mut group = c.benchmark_group("read_rayon_kvstore");
    for i in &vec![1, 2, 4, 8, 16, 32, 64] {
        group.bench_with_input(format!("kvs_{}", i), i, |b, &i| {
            let temp_dir = TempDir::new().unwrap();
            let addr = "127.0.0.1:4000";
            let entries_len:usize = 1000;
            
            let is_stop = Arc::new(AtomicBool::new(false));
            let pool = SharedQueueThreadPool::new(i).unwrap();
            // previous set the key/value pairs 
            let store = KvStore::open(temp_dir.path()).unwrap();
            for j in 0..entries_len {
                if let Err(e) = store.set(format!("key{}", j), format!("value{}", j)) {
                    warn!("store previous set value error: {:?}", e);
                }
            }

            let mut server = Server::new(store, pool, is_stop.clone()).unwrap();
            let handle = thread::spawn(move || {
                server.run(addr.to_owned()).unwrap();
            });
            
            thread::sleep(Duration::from_secs(1));

            b.iter(|| {
                let mut handles = Vec::new();
                for j in 0..entries_len {
                    let handle = thread::spawn(move|| {
                        match Client::connect(addr.to_owned()) {
                            Ok(mut client) => {
                                match client.get(format!("key{}", j)) {
                                    Ok(value) => {
                                        assert_eq!(value, Some(format!("value{}", j)));
                                    },
                                    Err(e) => warn!("client set error: {:?}", e)
                                }
                            }, 
                            Err(e) => {
                                warn!("client connect error: {:?}", e);
                            }  
                        }
                    });
                    handles.push(handle);
                }
                for handle in handles {
                    handle.join().unwrap();
                }
            });

            is_stop.store(true, Ordering::SeqCst);

            let _ = Client::connect(addr).unwrap();
            if let Err(e) = handle.join() {
                warn!("server run error: {:?}", e);
            }
        });
        group.bench_with_input(format!("sled_{}", i), i, |b, &i| {
            let temp_dir = TempDir::new().unwrap();
            let addr = "127.0.0.1:4000";
            let entries_len:usize = 1000;
            
            let is_stop = Arc::new(AtomicBool::new(false));
            let pool = SharedQueueThreadPool::new(i).unwrap();
            // previous set the key/value pairs 
            let store = SledEngine::open(temp_dir.path()).unwrap();
            for j in 0..entries_len {
                if let Err(e) = store.set(format!("key{}", j), format!("value{}", j)) {
                    warn!("store previous set value error: {:?}", e);
                }
            }

            let mut server = Server::new(store, pool, is_stop.clone()).unwrap();
            let handle = thread::spawn(move || {
                server.run(addr.to_owned()).unwrap();
            });
            
            thread::sleep(Duration::from_secs(1));

            b.iter(|| {
                let mut handles = Vec::new();
                for j in 0..entries_len {
                    let handle = thread::spawn(move|| {
                        match Client::connect(addr.to_owned()) {
                            Ok(mut client) => {
                                match client.get(format!("key{}", j)) {
                                    Ok(value) => {
                                        assert_eq!(value, Some(format!("value{}", j)));
                                    },
                                    Err(e) => warn!("client set error: {:?}", e)
                                }
                            }, 
                            Err(e) => {
                                warn!("client connect error: {:?}", e);
                            }  
                        }
                    });
                    handles.push(handle);
                }
                for handle in handles {
                    handle.join().unwrap();
                }
            });

            is_stop.store(true, Ordering::SeqCst);

            let _ = Client::connect(addr).unwrap();
            if let Err(e) = handle.join() {
                warn!("server run error: {:?}", e);
            }
        });
    }
}

fn concurrent_get(c: &mut Criterion) {
    START.call_once(|| {
        env_logger::Builder::from_env(Env::default().default_filter_or("warn")).init();
    });

    let mut group = c.benchmark_group("concurrent_get");
    for i in &vec![1, 2, 4, 8, 16, 32, 64, 128] {
        group.bench_with_input(format!("kvs_{}", i), i, |b, &i| {
            let temp_dir = TempDir::new().unwrap();
            let entries_len:usize = 1000;
            
            // previous set the key/value pairs 
            let store = KvStore::open(temp_dir.path()).unwrap();
            for j in 0..entries_len {
                if let Err(e) = store.set(format!("key{}", j), format!("value{}", j)) {
                    warn!("store previous set value error: {:?}", e);
                }
            }

            let pool = RayonThreadPool::new(i).unwrap();
            thread::sleep(Duration::from_secs(1));

            b.iter(|| {
                let wg = WaitGroup::new();
                for thread_id in 0..1000 {
                    let wg = wg.clone();
                    let store = store.clone();
                    pool.spawn(move || {
                        for i in 0..100 {
                            let key_id = (i + thread_id) % 1000;
                            assert_eq!(
                                store.get(format!("key{}", key_id)).unwrap(),
                                Some(format!("value{}", key_id))
                            );
                        }
                        drop(wg);
                    });
                }
                wg.wait();
            });

            drop(store);
        });
        group.bench_with_input(format!("sled_{}", i), i, |b, &i| {
            let temp_dir = TempDir::new().unwrap();
            let entries_len:usize = 1000;
            
            // previous set the key/value pairs 
            let store = SledEngine::open(temp_dir.path()).unwrap();
            for j in 0..entries_len {
                if let Err(e) = store.set(format!("key{}", j), format!("value{}", j)) {
                    warn!("store previous set value error: {:?}", e);
                }
            }

            let pool = RayonThreadPool::new(i).unwrap();
            thread::sleep(Duration::from_secs(1));

            b.iter(|| {
                let wg = WaitGroup::new();
                for thread_id in 0..1000 {
                    let wg = wg.clone();
                    let store = store.clone();
                    pool.spawn(move || {
                        for i in 0..100 {
                            let key_id = (i + thread_id) % 1000;
                            assert_eq!(
                                store.get(format!("key{}", key_id)).unwrap(),
                                Some(format!("value{}", key_id))
                            );
                        }
                        drop(wg);
                    });
                }
                wg.wait();
            });

            drop(store);
        });
    }

    group.finish();
}

criterion_group!(
    name = benches;
    config = Criterion::default().sample_size(10);
    // targets = write_queued_kvstore, write_rayon_kvstore, read_queued_kvstore, read_rayon_kvstore);
    targets = read_rayon_kvstore，concurrent_get);
criterion_main!(benches);
