pub struct TableResult<'a> {
    //the arc only exists for the lifetime
    _mmap: Arc<Mmap>,
    pub key: &'a [u8],
    pub value: &'a [u8],
    pub sequence_number: u64,
}

use std::{error::Error, fs::File, ops::Range, path::Path, sync::Arc};

use byteorder::{ByteOrder, LittleEndian};
use memmap2::Mmap;

use crate::persists::sst::sst_table_block::{BLOCK_SIZE, HEADER_SIZE};

struct DataEntryBlock {
    data_buffer: Arc<Mmap>,
    key_range: Range<usize>,
    value_range: Range<usize>,
    _offset: usize,
    //TODO use ref here
    seq_number: u64,
}

impl DataEntryBlock {
    fn parse_from_offset(buffer: Arc<Mmap>, mut offset: usize) -> Option<(Self, usize)> {
        if offset + HEADER_SIZE > buffer.len() {
            return None;
        }

        let key_length = LittleEndian::read_u32(&buffer[offset..offset + HEADER_SIZE]) as usize;
        offset += HEADER_SIZE;

        if key_length == 0 || offset + key_length > buffer.len() {
            return None;
        }

        let key_range = offset..offset + key_length;
        offset += key_length;

        if offset + HEADER_SIZE > buffer.len() {
            return None;
        }

        let value_length = LittleEndian::read_u32(&buffer[offset..offset + HEADER_SIZE]) as usize;
        offset += HEADER_SIZE;

        let value_range = offset..offset + value_length;
        offset += value_length;

        if offset + 8 > buffer.len() {
            return None;
        }

        let seq = LittleEndian::read_u64(&buffer[offset..offset + 8]);
        offset += 8;

        Some((
            DataEntryBlock {
                data_buffer: buffer,
                key_range,
                value_range,
                _offset: offset,
                seq_number: seq,
            },
            offset,
        ))
    }

    fn key(&self) -> &[u8] {
        &self.data_buffer[self.key_range.clone()]
    }

    fn value(&self) -> &[u8] {
        &self.data_buffer[self.value_range.clone()]
    }

    fn get(&self) -> TableResult {
        TableResult {
            _mmap: Arc::clone(&self.data_buffer),
            key: self.key(),
            value: self.value(),
            sequence_number: self.seq_number,
        }
    }
}

struct DataBlock {
    _data_buffer: Arc<Mmap>,
    blocks: Vec<DataEntryBlock>,
}

impl DataBlock {
    fn from_buffer(buffer: &Arc<Mmap>, start: usize, end: usize) -> Self {
        let mut parsed_blocks = Vec::new();
        let mut offset = 0;

        while start + offset < end {
            let abs_offset = start + offset;

            if let Some((entry, next_offset)) =
                DataEntryBlock::parse_from_offset(Arc::clone(buffer), abs_offset)
            {
                offset = next_offset - start;
                parsed_blocks.push(entry);
            } else {
                break;
            }
        }

        DataBlock {
            _data_buffer: Arc::clone(buffer),
            blocks: parsed_blocks,
        }
    }

    fn get(&self, key: &[u8]) -> Option<TableResult> {
        self.blocks
            .iter()
            .map(|block| block.get())
            .find(|res| res.key == key)
    }
}

pub struct SortedStringTable {
    first_key: String,
    last_key: String,
    data_blocks: Vec<DataBlock>,
    _mmap: Arc<Mmap>,
    _meta_data: MetaData,
}

impl SortedStringTable {
    pub fn new(path: &Path) -> Result<Self, Box<dyn Error>> {
        let file = File::open(path)?;
        let mmap = unsafe { Mmap::map(&file)? };
        let mmap_arc = Arc::new(mmap);

        let meta_data = read_metadata(&mmap_arc);

        let mut blocks = Vec::new();
        for i in (0..meta_data.metadata_offset).step_by(BLOCK_SIZE) {
            let start = i;
            let end = (i + BLOCK_SIZE).min(meta_data.metadata_offset);
            let block = DataBlock::from_buffer(&mmap_arc, start, end);
            blocks.push(block);
        }

        let first_key = blocks
            .first()
            .and_then(|b| b.blocks.first())
            .map(|entry| String::from_utf8_lossy(entry.key()).to_string())
            .unwrap_or_default();

        let last_key = blocks
            .last()
            .and_then(|b| b.blocks.last())
            .map(|entry| String::from_utf8_lossy(entry.key()).to_string())
            .unwrap_or_default();

        Ok(SortedStringTable {
            first_key,
            last_key,
            data_blocks: blocks,
            _mmap: mmap_arc,
            _meta_data: meta_data,
        })
    }

    pub fn get(&self, key: &[u8]) -> Option<TableResult> {
        if key < self.first_key.as_bytes() || key > self.last_key.as_bytes() {
            return None;
        }

        for (i, block) in self.data_blocks.iter().enumerate() {
            if let Some(result) = block.get(key) {
                println!("Found in block {i}");
                return Some(result);
            }
        }
        None
    }
}

struct MetaData {
    metadata_offset: usize,
    _version: usize,
}

fn read_metadata(mmap: &Mmap) -> MetaData {
    let meta_data_binary = &mmap[mmap.len() - 8..];

    let metadata_offset = LittleEndian::read_u32(&meta_data_binary[0..4]) as usize;
    let version = LittleEndian::read_u32(&meta_data_binary[4..8]) as usize;

    MetaData {
        metadata_offset,
        _version: version,
    }
}
