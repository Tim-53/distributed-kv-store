use std::sync::{Arc, atomic::AtomicU64};

use tokio::sync::{Mutex, RwLock};

use crate::persists::memtable::{
    btree_map::BTreeMemTable,
    memtable_trait::{LookupResult, MemTable},
};

use super::wal::{LogCommand, Wal};

pub struct KvStore<const MAX_SIZE: usize> {
    pub(crate) store: Arc<RwLock<BTreeMemTable<{ MAX_SIZE }>>>,
    pub(crate) flushable_tables: Arc<RwLock<Vec<BTreeMemTable<{ MAX_SIZE }>>>>,
    read_from_wal: bool,
    wal: Arc<Mutex<Wal>>,
    sequence_number_counter: AtomicU64,
}

impl<const MAX_SIZE: usize> KvStore<MAX_SIZE> {
    pub async fn new() -> Self {
        let mut store = KvStore {
            store: Arc::new(RwLock::new(BTreeMemTable::new())),
            flushable_tables: Arc::new(RwLock::new(Vec::new())),
            read_from_wal: false,
            wal: Arc::new(Mutex::new(
                Wal::new().await.expect("failed to open the wal file"),
            )),
            sequence_number_counter: AtomicU64::new(0),
        };
        if store.read_from_wal {
            let wal_entries = self::Wal::read_wal().await;

            for entry in wal_entries.expect("Wal could not be read") {
                //TODO use original sequence numbers here
                match entry {
                    LogCommand::Put {
                        key,
                        value,
                        seq_number: _,
                    } => {
                        store
                            .put_value(&key, &value)
                            .await
                            .unwrap_or_else(|_| panic!("put failed for value: {value}"));
                    }

                    LogCommand::Delete { key, seq_number: _ } => {
                        store
                            .delete_value(&key)
                            .await
                            .0
                            .unwrap_or_else(|| panic!("delete failed for key: {key}"));
                    }
                }
            }
        }

        store
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
            .map(|table| table.get(key.as_bytes()))
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
                flushables.push(old_table);
                println!("new table was created");
            }

            store_guard.insert(key.as_bytes(), value.as_bytes(), seq_number);
        }

        Ok(seq_number)
    }

    pub async fn delete_value(&mut self, key: &str) -> (Option<(String, String)>, u64) {
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
