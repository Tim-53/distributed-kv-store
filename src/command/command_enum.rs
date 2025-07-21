use std::sync::Arc;

use axum::Json;
use serde::{Deserialize, Serialize};

use crate::persists::KvStore;

const DEFAULT_MEM_SIZE: usize = 64 * 1024;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum CommandInput {
    Put { key: String, value: String },
    Get { key: String },
    Delete { key: String },
}

pub struct CommandExecutor {
    store: Arc<KvStore<DEFAULT_MEM_SIZE>>,
}

impl CommandExecutor {
    pub fn new(store: Arc<KvStore<DEFAULT_MEM_SIZE>>) -> Self {
        Self { store }
    }

    pub async fn execute_put(&self, key: &str, value: &str) {
        self.store.put_value(key, value).await.unwrap();
    }

    pub async fn execute_get(&self, key: &str) -> Option<String> {
        self.store.get_value(key).await
    }

    pub async fn execute_delete(&self, key: &str) -> Option<(String, String)> {
        self.store.delete_value(key).await.0
    }

    pub async fn handle_get_all(&self) -> Json<Vec<(String, String)>> {
        Json(self.store.get_all().await)
    }
}
