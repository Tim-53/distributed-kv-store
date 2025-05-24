use byteorder::{ByteOrder, LittleEndian, WriteBytesExt};

use super::sst_table::SSTableBlock;

pub struct BlockEntry {
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

    pub fn can_fit(&self, current_block: &SSTableBlock) -> bool {
        self.buffer.len() <= current_block.capacity()
    }

    pub fn get_entry_buffer(&mut self) -> &mut Vec<u8> {
        &mut self.buffer
    }
}
