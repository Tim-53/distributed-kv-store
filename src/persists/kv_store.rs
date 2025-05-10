use std::collections::HashMap;

use super::wal::{LogCommand, Wal};

pub struct KvStore {
    store: HashMap<String, String>,
    wal: Wal,
}

impl KvStore {
    pub async fn new() -> Self {
        let mut store = KvStore {
            store: HashMap::new(),
            wal: Wal::new().await.expect("failed to open the wal file"),
        };
        let wal_entries = self::Wal::read_wal().await;

        for entry in wal_entries.expect("Wal could not be read") {
            match entry {
                LogCommand::Put { key, value } => store.put_value(&key, &value).await,
                LogCommand::Delete { key } => {
                    store.delete_value(&key).await;
                }
            }
        }

        store
    }
    pub fn get_value(&self, key: &str) -> Option<String> {
        self.store.get(key).cloned()
    }

    pub async fn put_value(&mut self, key: &str, value: &str) {
        //TODO return result
        let _result = self
            .wal
            .append(&LogCommand::Put {
                key: (key.into()),
                value: (value.into()),
            })
            .await;
        self.store.insert(key.into(), value.into());
    }

    pub async fn delete_value(&mut self, key: &str) -> Option<(String, String)> {
        //TODO return result
        let _result = self
            .wal
            .append(&LogCommand::Delete { key: key.into() })
            .await;
        self.store
            .remove(key)
            .map(|deleted_value| (key.into(), deleted_value))
    }

    pub async fn get_all(&self) -> HashMap<String, String> {
        self.store.clone()
    }
}
