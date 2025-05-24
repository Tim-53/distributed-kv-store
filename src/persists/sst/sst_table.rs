use super::block_entry::BlockEntry;

pub const BLOCK_SIZE: usize = 4096; // 4 KB
const INDEX_INTERVAL: usize = 128; // index every 128th key

const HEADER_SIZE: usize = std::mem::size_of::<u32>();

pub struct SSTableBlock {
    entry_buf: Vec<u8>,
    start_value: (Vec<u8>, Vec<u8>),
}

impl Default for SSTableBlock {
    fn default() -> Self {
        Self::new()
    }
}

impl SSTableBlock {
    pub fn new() -> Self {
        Self {
            entry_buf: Vec::new(),
            start_value: (Vec::new(), Vec::new()),
        }
    }
    pub fn append_block(&mut self, mut block_entry: BlockEntry) {
        self.entry_buf.append(block_entry.get_entry_buffer());
    }

    pub fn capacity(&self) -> usize {
        BLOCK_SIZE - self.entry_buf.len()
    }

    pub fn finalize(mut self) -> Vec<u8> {
        if self.entry_buf.len() < BLOCK_SIZE {
            self.entry_buf.resize(BLOCK_SIZE, 0);
        }
        self.entry_buf
    }

    pub fn is_empty(&self) -> bool {
        self.entry_buf.is_empty()
    }
}
