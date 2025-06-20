use std::path::Path;

use byteorder::{ByteOrder, LittleEndian};

use crate::persists::sst::sst_table_block::HEADER_SIZE;

use super::sst_table_block::BLOCK_SIZE;

pub struct SSTableReader {}

impl SSTableReader {
    pub fn parse_file(path: &Path) {
        let buffer = std::fs::read(path).expect("could not read SSTable file");

        //For now we don't validate the bonds in the block and just assume the are correct
        for block in buffer.chunks_exact(BLOCK_SIZE) {
            let mut offset = 0;
            while (offset + 2 * HEADER_SIZE + 1) < BLOCK_SIZE {
                let key_length =
                    LittleEndian::read_u32(&block[offset..offset + HEADER_SIZE]) as usize;
                offset += HEADER_SIZE;

                let key = &block[offset..(offset + key_length)];

                offset += key_length;

                let value_length =
                    LittleEndian::read_u32(&block[offset..offset + HEADER_SIZE]) as usize;

                offset += HEADER_SIZE;

                let value = &block[offset..(offset + value_length)];

                offset += value_length;

                println!("{}", String::from_utf8(key.to_vec()).unwrap());
                println!("{}", String::from_utf8(value.to_vec()).unwrap());
            }
        }
    }

    pub fn file_into_iterator(path: &Path) -> impl Iterator<Item = (Vec<u8>, Vec<u8>)> {
        let buffer = std::fs::read(path).expect("could not read SSTable file");

        let mut cursor = 0usize;

        std::iter::from_fn(move || {
            loop {
                if cursor >= buffer.len() {
                    return None;
                }

                // If we are too close to a block boundary to hold
                //  key_len + value_len + their headers, skip to next block.
                let pos_in_block = cursor % BLOCK_SIZE;
                let remaining_in_block = BLOCK_SIZE - pos_in_block;
                if remaining_in_block < 2 * HEADER_SIZE + 1 {
                    cursor += remaining_in_block;
                    continue;
                }

                if cursor + HEADER_SIZE > buffer.len() {
                    return None;
                }
                let key_len =
                    LittleEndian::read_u32(&buffer[cursor..cursor + HEADER_SIZE]) as usize;
                cursor += HEADER_SIZE;

                if cursor + key_len + HEADER_SIZE > buffer.len() {
                    return None;
                }
                let key = buffer[cursor..cursor + key_len].to_vec();
                cursor += key_len;

                let value_len =
                    LittleEndian::read_u32(&buffer[cursor..cursor + HEADER_SIZE]) as usize;
                cursor += HEADER_SIZE;

                if cursor + value_len > buffer.len() {
                    return None;
                }
                let value = buffer[cursor..cursor + value_len].to_vec();
                cursor += value_len;

                return Some((key, value));
            }
        })
    }
}
