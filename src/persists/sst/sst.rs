use std::{error::Error, fs::File, io::Write, path::Path};

use std::fmt;

use byteorder::{ByteOrder, LittleEndian, WriteBytesExt};

pub const BLOCK_SIZE: usize = 4096; // 4 KB
const INDEX_INTERVAL: usize = 128; // index every 128th key

const HEADER_SIZE: usize = std::mem::size_of::<u32>();

struct SSTableBlock {
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
            start_value: (Vec::new(), Vec::new()),
        }
    }
    fn append_block(&mut self, mut block_entry: BlockEntry) {
        self.entry_buf.append(&mut block_entry.buffer);
    }

    fn capacity(&self) -> usize {
        BLOCK_SIZE - self.entry_buf.len()
    }

    pub fn finalize(mut self) -> Vec<u8> {
        if self.entry_buf.len() < BLOCK_SIZE {
            self.entry_buf.resize(BLOCK_SIZE, 0);
        }
        self.entry_buf
    }
}

struct BlockEntry {
    buffer: Vec<u8>,
}

impl BlockEntry {
    pub fn from_parts(key: &[u8], value: &[u8]) -> Self {
        let mut buffer = Vec::with_capacity(4 + key.len() + 4 + value.len());

        buffer.write_u32::<LittleEndian>(key.len() as u32).unwrap();
        buffer.extend_from_slice(key);
        buffer
            .write_u32::<LittleEndian>(value.len() as u32)
            .unwrap();
        buffer.extend_from_slice(value);

        BlockEntry { buffer }
    }

    pub fn key(&self) -> &[u8] {
        let key_len = LittleEndian::read_u32(&self.buffer[0..4]) as usize;
        &self.buffer[4..4 + key_len]
    }

    pub fn value(&self) -> &[u8] {
        let key_len = LittleEndian::read_u32(&self.buffer[0..4]) as usize;
        let value_len_offset = 4 + key_len;
        let value_len =
            LittleEndian::read_u32(&self.buffer[value_len_offset..value_len_offset + 4]) as usize;
        &self.buffer[value_len_offset + 4..value_len_offset + 4 + value_len]
    }

    fn can_fit(&self, current_block: &SSTableBlock) -> bool {
        return self.buffer.len() <= current_block.capacity();
    }
}

pub struct SSTableWriter {}

impl SSTableWriter {
    pub fn write_to_file(
        path: &Path,
        entries: Vec<(Vec<u8>, Option<Vec<u8>>)>,
        size: u32,
    ) -> Result<usize, std::io::Error> {
        let mut file = File::create_new(path)?;

        let mut blocks: Vec<SSTableBlock> = Vec::new();

        let mut data_buffer: Vec<u8> = Vec::with_capacity(size as usize);

        let mut current_block = SSTableBlock::new();

        for (key, value_opt) in &entries {
            let block_entry = BlockEntry::from_parts(key, &value_opt.clone().unwrap_or_default());

            if block_entry.can_fit(&current_block) {
                current_block.append_block(block_entry);
            } else {
                blocks.push(current_block);
                current_block = SSTableBlock::new();
                current_block.append_block(block_entry);
            }
        }

        if !current_block.entry_buf.is_empty() {
            blocks.push(current_block);
        }

        for block in blocks {
            let padded_block = block.finalize();
            data_buffer.extend_from_slice(&padded_block);
        }

        file.write_all(&data_buffer)?;
        Ok(data_buffer.len())
    }
}
