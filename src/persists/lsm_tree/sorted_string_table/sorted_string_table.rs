use std::{error::Error, fs::File, ops::Range, path::Path, sync::Arc};

use byteorder::{ByteOrder, LittleEndian};
use memmap2::Mmap;

use crate::persists::lsm_tree::sorted_string_table::{
    sst_table_block::{BLOCK_SIZE, HEADER_SIZE},
    table_result::TableResult,
};

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

    pub fn iter(&self) -> DataBlockIterator {
        DataBlockIterator {
            inner: self.blocks.iter(),
        }
    }
}

pub struct DataBlockIterator<'a> {
    inner: std::slice::Iter<'a, DataEntryBlock>,
}

impl<'a> Iterator for DataBlockIterator<'a> {
    type Item = TableResult<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        self.inner.next().map(|entry_block| entry_block.get())
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

        println!("metadata offset{}", meta_data.metadata_offset);

        let mut blocks = Vec::new();
        for i in (0..meta_data.metadata_offset).step_by(BLOCK_SIZE) {
            let start = i;
            let end = (i + BLOCK_SIZE).min(meta_data.metadata_offset);
            let block = DataBlock::from_buffer(&mmap_arc, start, end);
            blocks.push(block);
        }

        println!("found {}", blocks.len());

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

    pub fn iter(&self) -> SSTableIterator<'_> {
        SSTableIterator {
            remaining_blocks: self.data_blocks.iter(),
            current_block_iter: None,
        }
    }
}

pub struct SSTableIterator<'a> {
    remaining_blocks: std::slice::Iter<'a, DataBlock>,
    current_block_iter: Option<DataBlockIterator<'a>>,
}

impl<'a> Iterator for SSTableIterator<'a> {
    type Item = TableResult<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            //entry from current block
            if let Some(iter) = &mut self.current_block_iter {
                if let Some(item) = iter.next() {
                    return Some(item);
                }
            }
            //enter next block
            match self.remaining_blocks.next() {
                Some(next_block) => {
                    self.current_block_iter = Some(next_block.iter());
                }
                None => return None,
            }
        }
    }
}
impl<'a> IntoIterator for &'a SortedStringTable {
    type Item = TableResult<'a>;
    type IntoIter = SSTableIterator<'a>;

    fn into_iter(self) -> Self::IntoIter {
        self.iter()
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
