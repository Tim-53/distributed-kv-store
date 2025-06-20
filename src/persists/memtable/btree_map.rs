use std::collections::BTreeMap;

use crate::persists::sst::sst_table_block::HEADER_SIZE;

use super::memtable_trait::{LookupResult, MemTable};

#[derive(Debug, Default)]
pub struct BTreeMemTable<const MAX_SIZE: usize> {
    data: BTreeMap<Vec<u8>, Option<Vec<u8>>>,
    used_bytes: usize,
}

impl<const MAX_SIZE: usize> BTreeMemTable<MAX_SIZE> {
    pub fn new() -> Self {
        Self {
            data: BTreeMap::new(),
            used_bytes: 0,
        }
    }

    pub fn get_all(&self) -> Vec<(Vec<u8>, Vec<u8>)> {
        self.data
            .iter()
            .filter_map(|(k, v)| v.as_ref().map(|v| (k.clone(), v.clone())))
            .collect()
    }

    #[inline]
    pub fn encoded_len(key: &[u8], value: &[u8]) -> usize {
        2 * HEADER_SIZE + key.len() + value.len()
    }

    pub fn iter_all(&self) -> impl Iterator<Item = (&[u8], Option<&[u8]>)> {
        self.data
            .iter()
            .map(|(k, v_opt)| (k.as_slice(), v_opt.as_deref()))
    }
}

impl<const MAX_SIZE: usize> MemTable for BTreeMemTable<MAX_SIZE> {
    fn insert(&mut self, key: &[u8], value: &[u8]) {
        // adjust counter if we overwrite an existing value
        if let Some(Some(old)) = self.data.get(key) {
            self.used_bytes -= old.len();
        }
        self.used_bytes += Self::encoded_len(key, value);
        self.data.insert(key.to_vec(), Some(value.to_vec()));
    }

    fn delete(&mut self, key: &[u8]) -> Option<Option<Vec<u8>>> {
        if let Some(Some(old_val)) = self.data.get(key) {
            self.used_bytes -= old_val.len();
        }
        // Tombstone always adds header + key bytes (value len = 0)
        self.used_bytes += 2 * HEADER_SIZE + key.len();
        self.data.insert(key.to_vec(), None)
    }

    fn get(&self, key: &[u8]) -> LookupResult {
        match self.data.get(key) {
            Some(Some(val)) => LookupResult::Found(val),
            Some(None) => LookupResult::Deleted,
            None => LookupResult::NotFound,
        }
    }

    fn flush(&mut self) -> Vec<(Vec<u8>, Option<Vec<u8>>)> {
        let flushed: Vec<_> = self
            .data
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();
        self.data.clear();
        self.used_bytes = 0;
        flushed
    }

    fn has_capacity(&self, additional: usize) -> bool {
        self.used_bytes + additional <= MAX_SIZE
    }

    fn bytes_used(&self) -> usize {
        self.used_bytes
    }

    fn inc_bytes_used(&mut self, delta: usize) {
        self.used_bytes += delta;
    }
}
