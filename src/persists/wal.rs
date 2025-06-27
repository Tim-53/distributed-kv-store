use serde::{Deserialize, Serialize};
use serde_json::from_str;
use tokio::{
    fs::{File, OpenOptions},
    io::{AsyncBufReadExt, AsyncWriteExt, BufReader},
};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum LogCommand {
    Put { key: String, value: String },
    Delete { key: String },
}

pub struct Wal {
    file: File,
}

impl Wal {
    pub async fn new() -> std::io::Result<Self> {
        let write_file = OpenOptions::new()
            .create(true)
            .append(true)
            .open("wal.log")
            .await?;

        Ok(Self { file: write_file })
    }

    pub async fn append(&mut self, command: &LogCommand) -> std::io::Result<()> {
        let line = serde_json::to_string(command)?;
        self.file.write_all(line.as_bytes()).await?;
        self.file.write_all(b"\n").await?;
        Ok(())
    }

    pub async fn read_wal() -> std::io::Result<Vec<LogCommand>> {
        let file = File::open("wal.log").await?;
        let reader = BufReader::new(file);
        let mut lines = reader.lines();

        let mut entries = Vec::new();

        while let Some(line) = lines.next_line().await? {
            match from_str::<LogCommand>(&line) {
                Ok(cmd) => entries.push(cmd),
                Err(e) => {
                    eprintln!("Skipping invalid WAL line: {line} — {e:?}");
                }
            }
        }

        Ok(entries)
    }
}
