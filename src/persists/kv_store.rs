use crate::persists::memtable::{
    btree_map::BTreeMemTable,
    memtable_trait::{LookupResult, MemTable},
};

use super::wal::{LogCommand, Wal};

pub struct KvStore {
    store: BTreeMemTable,
    wal: Wal,
}

impl KvStore {
    pub async fn new() -> Self {
        let mut store = KvStore {
            store: BTreeMemTable::new(),
            wal: Wal::new().await.expect("failed to open the wal file"),
        };
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
