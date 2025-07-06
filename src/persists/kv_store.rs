use std::{
    collections::HashMap,
    sync::{Arc, atomic::AtomicU64},
};

use tokio::sync::{Mutex, RwLock, mpsc};

use crate::persists::{
    memtable::{
        btree_map::BTreeMemTable,
        memtable_trait::{LookupResult, MemTable},
    },
    sst::flush_worker::{FlushCommand, FlushResult, FlushWorker},
};

use super::wal::{LogCommand, Wal};

pub struct KvStore<const MAX_SIZE: usize> {
    pub(crate) store: Arc<RwLock<BTreeMemTable<{ MAX_SIZE }>>>,
    pub(crate) flushable_tables: Arc<RwLock<HashMap<u64, Arc<BTreeMemTable<{ MAX_SIZE }>>>>>,
    read_from_wal: bool,
    wal: Arc<Mutex<Wal>>,
    sequence_number_counter: AtomicU64,
    flush_worker: Arc<FlushWorker<{ MAX_SIZE }>>,
    sender: mpsc::Sender<FlushCommand>,
}

impl<const MAX_SIZE: usize> KvStore<MAX_SIZE> {
    pub async fn new() -> Arc<Self> {
        let (flush_result_tx, flush_result_rx) = tokio::sync::mpsc::channel(16);
        Self::new_with_channels(flush_result_tx, flush_result_rx).await
    }

    pub async fn new_with_channels(
        flush_result_tx: tokio::sync::mpsc::Sender<FlushResult>,
        flush_result_rx: tokio::sync::mpsc::Receiver<FlushResult>,
    ) -> Arc<Self> {
        let flushable_tables = Arc::new(RwLock::new(HashMap::new()));
        let (flush_tx, flush_rx) = tokio::sync::mpsc::channel(16);

        let store = Arc::new(KvStore {
            store: Arc::new(RwLock::new(BTreeMemTable::new())),
            flushable_tables: flushable_tables.clone(),
            read_from_wal: false,
            wal: Arc::new(Mutex::new(
                Wal::new().await.expect("failed to open the wal file"),
            )),
            sequence_number_counter: AtomicU64::new(0),
            flush_worker: Arc::new(FlushWorker::new(flushable_tables)),
            sender: flush_tx,
        });

        if store.read_from_wal {
            let wal_entries = Wal::read_wal().await;
            for entry in wal_entries.expect("Wal read failed") {
                match entry {
                    LogCommand::Put { key, value, .. } => {
                        store.put_value(&key, &value).await.expect("put failed");
                    }
                    LogCommand::Delete { key, .. } => {
                        store.delete_value(&key).await.0.expect("delete failed");
                    }
                }
            }
        }

        tokio::spawn({
            let flush_worker = store.flush_worker.clone();
            async move {
                flush_worker.flush(flush_rx, flush_result_tx).await;
            }
        });

        let store_clone = Arc::clone(&store);
        tokio::spawn(async move {
            store_clone.event_loop(flush_result_rx).await;
        });

        store
    }

    async fn event_loop(&self, mut receiver: mpsc::Receiver<FlushResult>) {
        while let Some(res) = receiver.recv().await {
            match res {
                Ok((id, _path)) => {
                    //TODO add path to lsm tree
                    let mut guard = self.flushable_tables.write().await;
                    guard.remove(&id);
                }
                Err(_) => {
                    //check if error can be handled
                    continue;
                }
            }
        }
    }

    pub async fn get_value(&self, key: &str) -> Option<String> {
        let store = self.store.read().await;
        if let LookupResult::Found((bytes, _seq_number)) = store.get(key.as_bytes()) {
            return Some(
                String::from_utf8(bytes.to_vec()).expect("Value is not a valid Utf8 String"),
            );
        }

        // the value might be in a flushable table
        let flushable_tables = self.flushable_tables.read().await;

        let values_from_flushable: Vec<LookupResult> = flushable_tables
            .iter()
            .map(|(id, table)| table.get(key.as_bytes()))
            .filter(|res| !matches!(res, LookupResult::NotFound))
            .collect();

        // we must ensure that we return the value with the highest valid sequence number
        let highest = values_from_flushable.into_iter().max_by(|a, b| {
            let a_seq = match a {
                LookupResult::Deleted(seq) => *seq,
                LookupResult::Found((_, seq)) => *seq,
                LookupResult::NotFound => 0,
            };
            let b_seq = match b {
                LookupResult::Deleted(seq) => *seq,
                LookupResult::Found((_, seq)) => *seq,
                LookupResult::NotFound => 0,
            };
            a_seq.cmp(&b_seq)
        });

        match highest {
            Some(LookupResult::Found((value_bytes, _))) => {
                Some(String::from_utf8_lossy(value_bytes).to_string())
            }
            _ => None,
        }
    }

    pub async fn put_value(&self, key: &str, value: &str) -> Result<u64, std::io::Error> {
        let seq_number = self.get_next_sequence_number();

        let mut wal = self.wal.lock().await;

        wal.append(&LogCommand::Put {
            key: key.into(),
            value: value.into(),
            seq_number,
        })
        .await?;

        let encoded_len = BTreeMemTable::<MAX_SIZE>::encoded_len(key.as_bytes(), value.as_bytes());

        {
            let mut store_guard = self.store.write().await;

            if !store_guard.has_capacity(encoded_len) {
                // Take ownership of current table and replace with new
                let old_table = std::mem::take(&mut *store_guard);
                let mut flushables = self.flushable_tables.write().await;
                flushables.insert(seq_number, Arc::new(old_table));
                println!("new table was created");
                //TODO handle result
                let _ = self.sender.send(FlushCommand::FlushAll).await;
            }

            store_guard.insert(key.as_bytes(), value.as_bytes(), seq_number);
        }

        Ok(seq_number)
    }

    pub async fn delete_value(&self, key: &str) -> (Option<(String, String)>, u64) {
        let seq_number = self.get_next_sequence_number();
        //TODO return result

        let mut wal = self.wal.lock().await;
        let _result = wal
            .append(&LogCommand::Delete {
                key: key.into(),
                seq_number,
            })
            .await;
        let val = self.store.write().await.delete(key.as_bytes(), seq_number);

        match val {
            Some((Some(value), _)) => (
                Some((
                    key.into(),
                    String::from_utf8(value.to_vec()).expect("Value is not a valid Utf8 String"),
                )),
                seq_number,
            ),
            _ => (None, seq_number),
        }
    }

    pub async fn get_all(&self) -> Vec<(String, String)> {
        self.store
            .read()
            .await
            .get_all()
            .into_iter()
            .map(|value| {
                (
                    String::from_utf8(value.0).expect("value not valid utf8"),
                    String::from_utf8(value.1).expect("value not valid utf8"),
                )
            })
            .collect()
    }

    fn get_next_sequence_number(&self) -> u64 {
        self.sequence_number_counter
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed)
    }
}
