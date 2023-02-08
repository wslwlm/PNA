use std::{thread};
use log::{warn};
use env_logger::{Env};
use criterion::{criterion_group, criterion_main, Criterion};
use kvs::thread_pool::{RayonThreadPool};
use kvs::{Client, Server, KvStore, KvsEngine};
use tempfile::TempDir;
use tokio::sync::{Barrier, oneshot};
use std::time::Duration;
use std::sync::{Arc, Once, atomic::{AtomicBool}};

static START: Once = Once::new();

fn write_rayon_kvstore(c: &mut Criterion) {
    START.call_once(|| {
        env_logger::Builder::from_env(Env::default().default_filter_or("warn")).init();
    });

    let mut group = c.benchmark_group("write_rayon_kvstore");
    for i in &vec![1, 2, 4, 8, 16, 32, 64] {
        group.bench_with_input(format!("kvs_{}", i), i, |b, &i| {
            let runtime = tokio::runtime::Builder::new_multi_thread()
                                .enable_all()
                                .build()
                                .unwrap();
            let temp_dir = TempDir::new().unwrap();
            let addr = "127.0.0.1:4000";
            let entries_len:usize = 1000;
            
            let cpu_num = num_cpus::get();
            let store = KvStore::<RayonThreadPool>::open(temp_dir.path(), cpu_num).unwrap();
            let is_stop = Arc::new(AtomicBool::new(false));
            let mut server = Server::new(store, is_stop).unwrap();
            
            let (tx, rx) = oneshot::channel();
            runtime.spawn(async move {
                server.run(addr.to_owned(), rx).await.unwrap();
            });
            
            thread::sleep(Duration::from_secs(1));

            b.to_async(&runtime).iter(|| async {
                let barrier = Arc::new(Barrier::new(entries_len + 1));
                for j in 0..entries_len {
                    let barrier = barrier.clone();
                    runtime.spawn(async move {
                        match Client::connect(addr.to_owned()).await {
                            Ok(mut client) => {
                                match client.set(format!("key{}", j), format!("value{}", j)).await {
                                    Ok(_) => {},
                                    Err(e) => warn!("client set error: {:?}", e)
                                }
                            }, 
                            Err(e) => {
                                warn!("client connect error: {:?}", e);
                            }  
                        }
                        barrier.wait().await;
                    });
                }
                barrier.wait().await;
            });

            runtime.spawn(async move {
                tx.send(()).unwrap();
            });
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
            let runtime = tokio::runtime::Builder::new_multi_thread()
                                .enable_all()
                                .build()
                                .unwrap();
            let temp_dir = TempDir::new().unwrap();
            let addr = "127.0.0.1:4000";
            let entries_len:usize = 1000;
            
            // let is_stop = Arc::new(AtomicBool::new(false));
            // let cpu_num = num_cpus::get();
            // previous set the key/value pairs 
            let store = KvStore::<RayonThreadPool>::open(temp_dir.path(), i).unwrap();
            runtime.block_on(async {
                for j in 0..entries_len {
                    if let Err(e) = store.set(format!("key{}", j), format!("value{}", j)).await {
                        warn!("store previous set value error: {:?}", e);
                    }
                }
            });
            
            let is_stop = Arc::new(AtomicBool::new(false));
            let mut server = Server::<KvStore::<RayonThreadPool>>::new(store, is_stop).unwrap();
            let (tx, rx) = oneshot::channel();
            runtime.spawn(async move {
                server.run(addr.to_owned(), rx).await.unwrap();
            });
            
            thread::sleep(Duration::from_secs(1));

            b.to_async(&runtime).iter(|| async {
                let barrier = Arc::new(Barrier::new(entries_len + 1));
                for j in 0..entries_len {
                    let barrier = barrier.clone();
                    runtime.spawn(async move {
                        match Client::connect(addr.to_owned()).await {
                            Ok(mut client) => {
                                match client.get(format!("key{}", j)).await {
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
                        barrier.wait().await;
                    });
                }
                barrier.wait().await;
            });

            runtime.spawn(async move {
                let res = tx.send(());
                if res.is_err() {
                    println!("{:?}", res);
                }
            });
        });
    }
}

fn concurrent_set(c: &mut Criterion) {
    START.call_once(|| {
        env_logger::Builder::from_env(Env::default().default_filter_or("warn")).init();
    });

    let mut group = c.benchmark_group("concurrent_get");
    for i in &vec![1, 2, 4, 8, 16, 32, 64] {
        group.bench_with_input(format!("kvs_{}", i), i, |b, &i| {
            let runtime = tokio::runtime::Builder::new_multi_thread()
                                .enable_all()
                                .build()
                                .unwrap();
            let temp_dir = TempDir::new().unwrap();
            let entries_len:usize = 10000;
            
            // previous set the key/value pairs 
            let store = KvStore::<RayonThreadPool>::open(temp_dir.path(), i).unwrap();
            b.to_async(&runtime).iter(|| async {
                let barrier = Arc::new(Barrier::new(entries_len + 1));
                for j in 0..entries_len {
                    let store = store.clone();
                    let barrier = barrier.clone();
                    runtime.spawn(async move {
                        store
                            .set(format!("key{}", j), format!("value{}", j)).await
                            .unwrap();
                        barrier.wait().await;
                    });
                }
                barrier.wait().await;
            });

            // We only check concurrent set in this test, so we check sequentially here
            let store = KvStore::<RayonThreadPool>::open(temp_dir.path(), 1).unwrap();
            runtime.block_on(async move {
                for i in 0..10000 {
                    store.get(format!("key{}", i)).await
                    .map(move |res| {
                        assert_eq!(res, Some(format!("value{}", i)));
                    }).unwrap();
                }
            });
        });
    }
}

fn concurrent_get(c: &mut Criterion) {
    START.call_once(|| {
        env_logger::Builder::from_env(Env::default().default_filter_or("warn")).init();
    });

    let mut group = c.benchmark_group("concurrent_get");
    for i in &vec![1, 2, 4, 8, 16, 32, 64] {
        group.bench_with_input(format!("kvs_{}", i), i, |b, &i| {
            let runtime = tokio::runtime::Builder::new_multi_thread()
                                .enable_all()
                                .build()
                                .unwrap();
            let temp_dir = TempDir::new().unwrap();
            let entries_len:usize = 10000;
            
            // previous set the key/value pairs 
            let store = KvStore::<RayonThreadPool>::open(temp_dir.path(), i).unwrap();
            runtime.block_on(async {
                for j in 0..entries_len {
                    if let Err(e) = store.set(format!("key{}", j), format!("value{}", j)).await {
                        warn!("store previous set value error: {:?}", e);
                    }
                }
            });

            b.to_async(&runtime).iter(|| async {
                let barrier = Arc::new(Barrier::new(entries_len + 1));
                for j in 0..entries_len {
                    let store = store.clone();
                    let barrier = barrier.clone();
                    runtime.spawn(async move {
                        store
                            .get(format!("key{}", j)).await
                            .map(move |res| {
                                assert_eq!(res, Some(format!("value{}", j)));
                            }).unwrap();
                        barrier.wait().await;
                    });
                }
                barrier.wait().await;
            });
        });
    }
}

criterion_group!(
    name = benches;
    config = Criterion::default().sample_size(10);
    // targets = write_queued_kvstore, write_rayon_kvstore, read_queued_kvstore, read_rayon_kvstore);
    targets = concurrent_set);
criterion_main!(benches);