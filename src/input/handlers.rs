use crate::command::command_enum::CommandExecutor;
use axum::debug_handler;
use axum::{
    Json,
    extract::{Path, State},
};
use serde::Deserialize;
use std::sync::Arc;

use axum::http::StatusCode;

#[derive(Deserialize)]
pub struct PutRequest {
    pub key: String,
    pub value: String,
}

#[derive(Deserialize)]
pub struct DeleteRequest {
    pub key: String,
}

#[derive(Clone)]
pub struct Handler {
    pub executor: Arc<CommandExecutor>,
}

impl Handler {
    pub fn new(executor: CommandExecutor) -> Self {
        Self {
            executor: Arc::new(executor),
        }
    }

    pub async fn handle_put(&self, payload: PutRequest) {
        self.executor
            .execute_put(&payload.key, &payload.value)
            .await;
    }

    pub async fn handle_get(&self, key: &str) -> Option<String> {
        self.executor.execute_get(key).await
    }

    pub async fn handle_get_all(&self) -> Json<Vec<(String, String)>> {
        self.executor.handle_get_all().await
    }

    pub async fn handle_delete(&self, key: &str) -> Option<(String, String)> {
        self.executor.execute_delete(key).await
    }
}

#[debug_handler]
pub async fn put_handler(
    State(handler): State<Arc<Handler>>,
    Json(payload): Json<PutRequest>,
) -> Result<Json<&'static str>, StatusCode> {
    handler.handle_put(payload).await;
    Ok(Json("OK"))
}

#[debug_handler]
pub async fn get_handler(
    State(handler): State<Arc<Handler>>,
    Path(key): Path<String>,
) -> Json<Option<String>> {
    Json(handler.handle_get(&key).await)
}

#[debug_handler]
pub async fn delete_handler(
    State(handler): State<Arc<Handler>>,
    Json(payload): Json<DeleteRequest>,
) -> Json<Option<(String, String)>> {
    Json(handler.handle_delete(&payload.key).await)
}

#[debug_handler]
pub async fn get_all_handler(State(handler): State<Arc<Handler>>) -> Json<Vec<(String, String)>> {
    handler.handle_get_all().await
}
