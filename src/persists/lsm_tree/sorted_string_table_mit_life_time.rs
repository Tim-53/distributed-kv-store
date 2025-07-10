use byteorder::{ByteOrder, LittleEndian};
use memmap2::Mmap;
use std::{error::Error, fs::File, os::unix::raw::off_t, path::Path, sync::Arc};

use crate::persists::sst::sst_table_block::{BLOCK_SIZE, HEADER_SIZE};

pub struct TableResult<'a> {
    pub mmap: Arc<Mmap>,
    pub key: &'a [u8],
    pub value: &'a [u8],
}

struct DataBlock<'a> {
    data_buffer: Arc<Mmap>,
    blocks: Vec<DataEntryBlock<'a>>,
}

impl<'a> DataBlock<'a> {
    fn from_buffer(buffer: &'a Arc<Mmap>, start: usize, end: usize) -> Self {
        let raw_entries = &buffer[start..end];
        let mut parsed_blocks = Vec::new();
        let mut offset = 0;

        while offset < raw_entries.len() {
            let abs_offset = start + offset;

            if let Some((entry, next_offset)) =
                DataEntryBlock::parse_from_offset(&buffer, abs_offset)
            {
                parsed_blocks.push(entry);
                offset = next_offset - start;
            } else {
                break;
            }
        }

        DataBlock {
            data_buffer: Arc::clone(&buffer),
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

struct MetaData {
    metadata_offset: usize,
    version: usize,
}

fn read_metadata(mmap: &Mmap) -> MetaData {
    let meta_data_binary = &mmap[mmap.len() - 8..];

    let metadata_offset = LittleEndian::read_u32(&meta_data_binary[0..4]);
    let version = LittleEndian::read_u32(&meta_data_binary[4..8]);

    MetaData {
        metadata_offset: metadata_offset as usize,
        version: version as usize,
    }
}

struct DataEntryBlock<'a> {
    data_buffer: Arc<Mmap>,
    offset: usize,
    key: &'a [u8],
    value: &'a [u8],
}

impl<'a> DataEntryBlock<'a> {
    fn parse_from_offset<'b>(
        buffer: &'b Arc<Mmap>,
        mut offset: usize,
    ) -> Option<(DataEntryBlock<'b>, usize)> {
        if offset + HEADER_SIZE > buffer.len() {
            return None;
        }

        let key_length = LittleEndian::read_u32(&buffer[offset..offset + HEADER_SIZE]) as usize;
        offset += HEADER_SIZE;

        if key_length == 0 || offset + key_length > buffer.len() {
            return None;
        }

        let key = &buffer[offset..offset + key_length];
        offset += key_length;

        if offset + HEADER_SIZE > buffer.len() {
            return None;
        }

        let value_length = LittleEndian::read_u32(&buffer[offset..offset + HEADER_SIZE]) as usize;
        offset += HEADER_SIZE;

        if offset + value_length > buffer.len() {
            return None;
        }

        let value = &buffer[offset..offset + value_length];
        offset += value_length;

        if offset + 8 > buffer.len() {
            return None;
        }

        let _seq = LittleEndian::read_u64(&buffer[offset..offset + 8]);
        offset += 8;

        let entry = DataEntryBlock {
            data_buffer: Arc::clone(&buffer), // ✅ korrekt
            offset,
            key,
            value,
        };

        Some((entry, offset))
    }

    fn get(&self) -> TableResult {
        TableResult {
            mmap: self.data_buffer.clone(),
            key: &self.key,
            value: &self.value,
        }
    }

    pub fn key(&self) -> &'a [u8] {
        self.key
    }
}

struct SortedStringTable<'a> {
    // file_path: Path,
    first_key: String,
    last_key: String,
    //block cache manager
    //bloom filter
    //block index
    //file eventuell als mmap
    data_blocks: Vec<DataBlock<'a>>,
    mmap: Arc<Mmap>,
    meta_data: MetaData,
}

impl<'a> SortedStringTable<'a> {
    pub fn new(path: String) -> Result<Self, Box<dyn std::error::Error>> {
        let file = File::open(path)?;
        let mmap = unsafe { Mmap::map(&file)? };

        let meta_data = read_metadata(&mmap);

        let mmap_arc = Arc::new(mmap);

        let mut blocks = Vec::new();
        for i in (0..meta_data.metadata_offset).step_by(BLOCK_SIZE) {
            let start_of_block = i;
            let end_of_block = (i + BLOCK_SIZE).min(meta_data.metadata_offset);

            let block = DataBlock::from_buffer(&mmap_arc, start_of_block, end_of_block);

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
            mmap: mmap_arc.clone(),
            meta_data,
        })
    }

    pub fn get_value() {
        //check index and get block
        //load block
        //check cache
        //if not cached get from file
        //read value
    }

    pub fn file_iterator() {
        //iterator for compacting
    }
}
