#[cfg(test)]
mod tests {
    use crate::memtable::{
        btree_map::BTreeMemTable,
        memtable_trait::{LookupResult, MemTable},
    };

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

        assert!(matches!(table.get(b"a"), LookupResult::NotFound));
    }
}
