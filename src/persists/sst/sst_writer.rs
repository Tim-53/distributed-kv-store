use std::{fs::File, io::Write, path::Path};

use super::{block_entry::BlockEntry, sst_table_block::SSTableBlock};

pub struct SSTableWriter {}

impl SSTableWriter {
    pub fn write_to_file(
        path: &Path,
        entries: Vec<(Vec<u8>, Option<Vec<u8>>, u64)>,
        size: u32,
    ) -> Result<usize, std::io::Error> {
        let mut file = File::create_new(path)?;

        let mut blocks: Vec<SSTableBlock> = Vec::new();

        let mut data_buffer: Vec<u8> = Vec::with_capacity(size as usize);

        let mut current_block = SSTableBlock::new();

        for (key, value_opt, seq_number) in &entries {
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
        Ok(data_buffer.len())
    }
}
