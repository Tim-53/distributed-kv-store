pub mod command;
pub mod input;
pub mod persists;

use std::sync::Arc;

use axum::{
    Router,
    routing::{delete, get, put},
};

use command::command_enum::CommandExecutor;
use input::handlers::{Handler, delete_handler, get_handler, put_handler};
use persists::KvStore;
use tokio::sync::RwLock;

pub async fn run() {
    let store = Arc::new(RwLock::new(KvStore::new().await));
    let executor = CommandExecutor::new(store.clone());
    let handler = Arc::new(Handler::new(executor));

    let app = Router::new()
        .route("/", put(put_handler))
        .route("/", delete(delete_handler))
        .route("/put", put(put_handler))
        .route("/get/{key}", get(get_handler))
        .with_state(handler.clone());

    // run our app with hyper, listening globally on port 3000
    let listener = tokio::net::TcpListener::bind("0.0.0.0:3000").await.unwrap();
    axum::serve(listener, app).await.unwrap();
}
