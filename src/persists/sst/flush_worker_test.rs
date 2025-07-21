use std::{collections::HashMap, sync::Arc};

use tokio::sync::RwLock;

use crate::persists::{
    memtable::{btree_map::BTreeMemTable, memtable_trait::MemTable},
    sst::flush_worker::{FlushCommand, FlushWorker},
};

#[tokio::test]
async fn flush_worker() {
    let flushable_tables = Arc::new(RwLock::new(HashMap::new()));

    for j in 0..2 {
        let mut table = BTreeMemTable::<640>::new();

        for i in 0..10 {
            table.insert(
                format!("key{i}").as_bytes(),
                format!("value{}", j * i).as_bytes(),
                j * i,
            );
        }
        flushable_tables.write().await.insert(j, Arc::new(table));
    }

    let (flush_tx, flush_rx) = tokio::sync::mpsc::channel(16);

    let (flush_result_tx, mut flush_result_rx) = tokio::sync::mpsc::channel(16);

    let worker = Arc::new(FlushWorker::<640>::new(flushable_tables));
    let worker_clone = Arc::clone(&worker); // explizit vor tokio::spawn

    tokio::spawn(async move {
        worker_clone.flush(flush_rx, flush_result_tx).await;
    });

    let _ = flush_tx.send(FlushCommand::FlushAll).await;

    let mut flushed_paths = Vec::new();

    for _ in 0..2 {
        match flush_result_rx.recv().await.expect("missing flush result") {
            Ok((_id, path)) => {
                assert!(
                    path.exists(),
                    "Expected flushed SSTable at {}, but it does not exist",
                    path.display()
                );
                flushed_paths.push(path);
            }
            Err(e) => panic!("Flush failed: {e:?}"),
        }
    }

    drop(flush_tx);

    for path in flushed_paths {
        tokio::fs::remove_file(&path)
            .await
            .unwrap_or_else(|e| panic!("Failed to remove file {}: {e}", path.display()));
    }
}
