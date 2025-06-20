use crate::persists::memtable::{
    btree_map::BTreeMemTable,
    memtable_trait::{LookupResult, MemTable},
};

use super::wal::{LogCommand, Wal};

pub struct KvStore<const MAX_SIZE: usize> {
    pub(crate) store: BTreeMemTable<{ MAX_SIZE }>,
    pub(crate) flushable_tables: Vec<BTreeMemTable<{ MAX_SIZE }>>,
    read_from_wal: bool,
    wal: Wal,
}

impl<const MAX_SIZE: usize> KvStore<MAX_SIZE> {
    pub async fn new() -> Self {
        let mut store = KvStore {
            store: BTreeMemTable::new(),
            flushable_tables: Vec::new(),
            read_from_wal: false,
            wal: Wal::new().await.expect("failed to open the wal file"),
        };
        if store.read_from_wal {
            let wal_entries = self::Wal::read_wal().await;

            for entry in wal_entries.expect("Wal could not be read") {
                match entry {
                    LogCommand::Put { key, value } => store
                        .put_value(&key, &value)
                        .await
                        .unwrap_or_else(|_| panic!("... {value}")),
                    LogCommand::Delete { key } => {
                        store.delete_value(&key).await;
                    }
                }
            }
        }

        store
    }
    pub fn get_value(&self, key: &str) -> Option<String> {
        match self.store.get(key.as_bytes()) {
            LookupResult::Found(bytes) => {
                Some(String::from_utf8(bytes.to_vec()).expect("Value is not a valid Utf8 String"))
            }
            _ => None,
        }
    }

    pub async fn put_value(&mut self, key: &str, value: &str) -> Result<(), std::io::Error> {
        let result = self
            .wal
            .append(&LogCommand::Put {
                key: (key.into()),
                value: (value.into()),
            })
            .await;

        if !self
            .store
            .has_capacity(BTreeMemTable::<MAX_SIZE>::encoded_len(
                key.as_bytes(),
                value.as_bytes(),
            ))
        {
            // memtable is out of capacity so we need to flush it and create a new one
            let new_mem_table = BTreeMemTable::new();
            let old_mem_table = std::mem::replace(&mut self.store, new_mem_table);
            self.flushable_tables.push(old_mem_table);
            println!(" neue tabele erstellt");
        }

        self.store.insert(key.as_bytes(), value.as_bytes());

        result
    }

    pub async fn delete_value(&mut self, key: &str) -> Option<(String, String)> {
        //TODO return result
        let _result = self
            .wal
            .append(&LogCommand::Delete { key: key.into() })
            .await;
        let val = self.store.delete(key.as_bytes());

        match val {
            Some(Some(value)) => Some((
                key.into(),
                String::from_utf8(value.to_vec()).expect("Value is not a valid Utf8 String"),
            )),
            _ => None,
        }
    }

    pub async fn get_all(&self) -> Vec<(String, String)> {
        self.store
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
}
