use std::{
    fs::File,
    io::{Seek, Write},
    path::{Path, PathBuf},
};

use byteorder::{LittleEndian, WriteBytesExt};

use crate::persists::lsm_tree::sorted_string_table::{
    sst_table_block::BLOCK_SIZE, table_result::TableResult,
};

use super::{block_entry::BlockEntry, sst_table_block::SSTableBlock};

type EntryType = Vec<(Vec<u8>, (Option<Vec<u8>>, u64))>;
pub struct SSTableWriter {
    file: File,
    written_blocks_count: u32,
    current_block: SSTableBlock,
    path: PathBuf,
}

impl SSTableWriter {
    pub fn new<P: Into<PathBuf>>(path: P) -> std::io::Result<Self> {
        let path_buf: PathBuf = path.into();
        let file = File::create(&path_buf)?;

        Ok(Self {
            file,
            current_block: SSTableBlock::new(),
            path: path_buf,
            written_blocks_count: 0,
        })
    }

    pub fn write_to_file(
        path: &Path,
        entries: EntryType,
        size: u32,
    ) -> Result<usize, std::io::Error> {
        let mut file = File::create_new(path)?;

        let mut blocks: Vec<SSTableBlock> = Vec::new();

        let mut data_buffer: Vec<u8> = Vec::with_capacity(size as usize);

        let mut current_block = SSTableBlock::new();

        for (key, (value_opt, seq_number)) in &entries {
            let block_entry =
                BlockEntry::from_parts(key, &value_opt.clone().unwrap_or_default(), seq_number);

            if block_entry.can_fit(&current_block) {
                current_block.append_block(block_entry);
            } else {
                blocks.push(current_block);
                current_block = SSTableBlock::new();
                current_block.append_block(block_entry);
            }
        }

        if !current_block.is_empty() {
            blocks.push(current_block);
        }

        for block in blocks {
            let padded_block = block.finalize();
            data_buffer.extend_from_slice(&padded_block);
        }

        file.write_all(&data_buffer)?;

        let metadata_offset = data_buffer.len() as u32;
        let version: u32 = 1;

        file.write_u32::<LittleEndian>(metadata_offset).unwrap();
        file.write_u32::<LittleEndian>(version).unwrap();

        Ok(data_buffer.len())
    }

    pub fn append_entry(&mut self, entry: &TableResult) {
        //TODO use actully size of seq number
        println!("appending {:?}", entry);

        let TableResult {
            key,
            value,
            sequence_number,
            ..
        } = entry;

        let block_entry = BlockEntry::from_parts(key, value, sequence_number);

        if block_entry.can_fit(&self.current_block) {
            self.current_block.append_block(block_entry);
        } else {
            let block = std::mem::take(&mut self.current_block);
            let padded_block = block.finalize();
            self.written_blocks_count += 1;
            println!(
                "increased offset to metadata offset: {}",
                self.written_blocks_count
            );
            self.file
                .write_all(&padded_block)
                .expect("failed to write block");
            self.current_block.append_block(block_entry);
        }
    }

    pub fn finalize(mut self) -> Result<PathBuf, std::io::Error> {
        let block = std::mem::take(&mut self.current_block);
        let padded_block = block.finalize();
        self.written_blocks_count += 1;
        println!(
            "increased offset to metadata offset: {}",
            self.written_blocks_count
        );
        self.file
            .write_all(&padded_block)
            .expect("failed to write block");

        let version: u32 = 1;

        println!(
            "metadata offset: {}",
            self.written_blocks_count * BLOCK_SIZE as u32
        );

        // let metadata_offset = self.written_blocks_count * BLOCK_SIZE as u32;

        let metadata_offset = self.file.stream_position()? as u32;
        self.file
            .write_u32::<LittleEndian>(metadata_offset)
            .unwrap();
        self.file.write_u32::<LittleEndian>(version).unwrap();

        self.file.flush()?;
        self.file.sync_all()?;
        Ok(self.path)
    }
}
