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

//TODO put into test file later

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_insert_and_get() {
        let mut table = BTreeMemTable::new();
        table.insert(b"foo", b"bar");

        match table.get(b"foo") {
            LookupResult::Found(val) => assert_eq!(val, b"bar"),
            _ => panic!("Expected Found"),
        }
    }

    #[test]
    fn test_delete() {
        let mut table = BTreeMemTable::new();
        table.insert(b"foo", b"bar");
        table.delete(b"foo");

        match table.get(b"foo") {
            LookupResult::Deleted => {} // okay
            _ => panic!("Expected Deleted"),
        }
    }

    #[test]
    fn test_not_found() {
        let table = BTreeMemTable::new();
        assert!(matches!(table.get(b"missing"), LookupResult::NotFound));
    }

    #[test]
    fn test_flush() {
        let mut table = BTreeMemTable::new();
        table.insert(b"a", b"1");
        table.insert(b"b", b"2");
        table.delete(b"c");

        let flushed = table.flush();

        assert!(flushed.contains(&(b"a".to_vec(), Some(b"1".to_vec()))));
        assert!(flushed.contains(&(b"b".to_vec(), Some(b"2".to_vec()))));
        assert!(flushed.contains(&(b"c".to_vec(), None)));

        assert!(matches!(table.get(b"a"), LookupResult::NotFound)); // clear after
    }
}
