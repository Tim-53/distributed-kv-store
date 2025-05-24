use std::collections::BTreeMap;

use super::memtable_trait::{LookupResult, MemTable};

pub struct BTreeMemTable {
    data: BTreeMap<Vec<u8>, Option<Vec<u8>>>,
}

impl BTreeMemTable {
    pub fn new() -> Self {
        Self {
            data: BTreeMap::new(),
        }
    }

    pub fn get_all(&self) -> Vec<(Vec<u8>, Vec<u8>)> {
        self.data
            .iter()
            .filter_map(|(k, v)| v.as_ref().map(|v| (k.clone(), v.clone())))
            .collect()
    }
}

impl MemTable for BTreeMemTable {
    fn insert(&mut self, key: &[u8], value: &[u8]) {
        self.data.insert(key.to_vec(), Some(value.to_vec()));
    }

    fn get(&self, key: &[u8]) -> LookupResult {
        match self.data.get(key) {
            Some(Some(val)) => LookupResult::Found(val),
            Some(None) => LookupResult::Deleted,
            None => LookupResult::NotFound,
        }
    }

    // fn range<'a>(
    //     &'a self,
    //     range: impl std::ops::RangeBounds<&'a [u8]>,
    // ) -> Box<dyn Iterator<Item = (&'a [u8], super::memtable_trait::LookupResult<'a>)> + 'a> {
    //     todo!()
    // }

    fn delete(&mut self, key: &[u8]) -> Option<Option<Vec<u8>>> {
        self.data.remove(key)
    }

    fn flush(&mut self) -> Vec<(Vec<u8>, Option<Vec<u8>>)> {
        let flushed = self
            .data
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();

        self.data.clear();

        flushed
    }
}
