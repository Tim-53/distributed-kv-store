use std::{cmp::Ordering, sync::Arc};

use memmap2::Mmap;

#[derive(Debug)]
pub struct TableResult<'a> {
    pub(crate) _mmap: Arc<Mmap>,
    pub key: &'a [u8],
    pub value: &'a [u8],
    pub sequence_number: u64,
}

impl<'a> TableResult<'a> {
    pub fn new(mmap: Arc<Mmap>, key: &'a [u8], value: &'a [u8], sequence_number: u64) -> Self {
        Self {
            _mmap: mmap,
            key,
            value,
            sequence_number,
        }
    }
}

impl<'a> PartialEq for TableResult<'a> {
    fn eq(&self, other: &Self) -> bool {
        self.key == other.key && self.sequence_number == other.sequence_number
    }
}

impl<'a> Eq for TableResult<'a> {}

impl<'a> PartialOrd for TableResult<'a> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl<'a> Ord for TableResult<'a> {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        other
            .key
            .cmp(self.key)
            .then(self.sequence_number.cmp(&other.sequence_number))
    }
}
