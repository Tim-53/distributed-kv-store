use std::{error::Error, fs::File, io::Write, path::Path};

use std::fmt;

use byteorder::{LittleEndian, WriteBytesExt};
const BLOCK_SIZE: usize = 4096; // 4 KB
const INDEX_INTERVAL: usize = 128; // index every 128th key

const HEADER_SIZE: usize = std::mem::size_of::<u32>();

struct SSTableBlock {
    size: usize,
    entry_buf: Vec<u8>,
    start_value: (Vec<u8>, Vec<u8>),
}
#[derive(Debug)]
pub struct BlockSizeExceededError;

impl fmt::Display for BlockSizeExceededError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Block size exceeded")
    }
}

impl Error for BlockSizeExceededError {}

impl SSTableBlock {
    pub fn new() -> Self {
        Self {
            entry_buf: Vec::new(),
            size: 0,
            start_value: (Vec::new(), Vec::new()),
        }
    }

    pub fn add_entry_to_block(
        &mut self,
        entry: &(Vec<u8>, Option<Vec<u8>>),
    ) -> Result<(), BlockSizeExceededError> {
        let key = &entry.0;
        let value = entry.1.as_ref().map_or(Vec::new(), |v| v.clone());
        let entry_size = 2 * HEADER_SIZE + key.len() + value.len();

        if self.size + entry_size <= BLOCK_SIZE {
            self.insert_entry(entry_size, key, &value)?;
            return Ok(());
        }

        return Err(BlockSizeExceededError);
    }

    pub fn can_entry_be_added(entry: &(Vec<u8>, Option<Vec<u8>>)) -> bool {}

    fn insert_entry(
        &mut self,
        entry_size: usize,
        key: &Vec<u8>,
        value: &Vec<u8>,
    ) -> std::io::Result<()> {
        let mut buf = Vec::with_capacity(entry_size);

        buf.write_u32::<LittleEndian>(key.len() as u32)?;
        buf.extend_from_slice(&key);
        buf.write_u32::<LittleEndian>(value.len() as u32)?;
        buf.extend_from_slice(&value);

        self.entry_buf.extend_from_slice(&buf);

        Ok(())
    }
}

struct SSTableWriter {}

impl SSTableWriter {
    pub fn write_to_file(
        path: &Path,
        entries: Vec<(Vec<u8>, Option<Vec<u8>>)>,
    ) -> Result<usize, std::io::Error> {
        let mut file = File::create_new(path)?;

        let file_content_buf: Vec<u8> = Vec::new();

        let _current_block = SSTableBlock::new();

        entries
            .iter()
            .for_each(|entry: &(Vec<u8>, Option<Vec<u8>>)| {});

        file.write(&file_content_buf)
    }
}
