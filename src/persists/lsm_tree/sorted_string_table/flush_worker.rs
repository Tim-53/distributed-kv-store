use std::{collections::HashMap, error::Error, sync::Arc};

use tokio::sync::{RwLock, mpsc};

use crate::persists::{
    lsm_tree::sorted_string_table::sst_writer::SSTableWriter,
    memtable::{btree_map::BTreeMemTable, memtable_trait::MemTable},
};

pub enum FlushCommand {
    FlushAll,
}

pub type FlushResult = Result<(u64, std::path::PathBuf), Box<dyn std::error::Error + Send + Sync>>;

pub struct FlushWorker<const MAX_SIZE: usize> {
    flushable_tables: Arc<RwLock<HashMap<u64, Arc<BTreeMemTable<{ MAX_SIZE }>>>>>,
    table_writer: SSTableWriter,
}

impl<const MAX_SIZE: usize> FlushWorker<MAX_SIZE> {
    pub fn new(
        flushable_tables: Arc<RwLock<HashMap<u64, Arc<BTreeMemTable<{ MAX_SIZE }>>>>>,
    ) -> Self {
        Self {
            flushable_tables,
            table_writer: SSTableWriter::new(std::path::PathBuf::from(format!(
                "L0_{}.sst",
                uuid::Uuid::new_v4()
            )))
            .expect("failed to open new file"),
        }
    }

    pub async fn flush(
        &self,
        mut rx: mpsc::Receiver<FlushCommand>,
        mut tx: mpsc::Sender<FlushResult>,
    ) {
        while let Some(cmd) = rx.recv().await {
            match cmd {
                FlushCommand::FlushAll => self.flush_all(&mut tx).await,
            }
        }
    }

    pub async fn flush_all(&self, tx: &mut mpsc::Sender<FlushResult>) {
        let to_flush = {
            let guard = self.flushable_tables.read().await;
            guard.clone() // shallow clone: nur Arcs
        };

        for (id, table) in &to_flush {
            let buffer = table.flush();
            let path = std::path::PathBuf::from(format!("L0_{}.sst", uuid::Uuid::new_v4()));

            //TODO use new file writer here
            let result: FlushResult =
                SSTableWriter::write_to_file(&path, buffer, table.bytes_used() as u32)
                    .map(|_| (*id, path.clone()))
                    .map_err(|e| Box::new(e) as Box<dyn Error + Send + Sync>);

            let _ = tx.send(result).await;
        }
    }
}
