#[cfg(test)]
mod tests {
    use crate::persists::memtable::{
        btree_map::BTreeMemTable,
        memtable_trait::{LookupResult, MemTable},
    };

    type ActiveTestMemTable = BTreeMemTable<{ 2 * 64 }>;

    #[test]
    fn test_insert_and_get() {
        let seq_number: u64 = 0;

        let mut table = ActiveTestMemTable::new();
        table.insert(b"foo", b"bar", seq_number);

        match table.get(b"foo") {
            LookupResult::Found((val, _seq_number)) => assert_eq!(val, b"bar"),
            _ => panic!("Expected Found"),
        }
    }

    #[test]
    fn test_delete() {
        let mut seq_number: u64 = 0;

        let mut table = ActiveTestMemTable::new();
        table.insert(b"foo", b"bar", seq_number);
        seq_number += 1;
        table.delete(b"foo", seq_number);

        match table.get(b"foo") {
            LookupResult::Deleted(_seq_number) => {}
            result => panic!("Expected Deleted, received {result:?}"),
        }
    }

    #[test]
    fn test_not_found() {
        let table = ActiveTestMemTable::new();
        assert!(matches!(table.get(b"missing"), LookupResult::NotFound));
    }

    // #[test]
    // fn test_flush() {
    //     let seq_number: u64 = 0;
    //     let mut table = ActiveTestMemTable::new();
    //     table.insert(b"a", b"1", seq_number);
    //     table.insert(b"b", b"2", seq_number);
    //     table.delete(b"c", seq_number);

    //     let flushed = table.flush();

    //     assert!(flushed.contains(&(b"a".to_vec(), Some(b"1".to_vec()))));
    //     assert!(flushed.contains(&(b"b".to_vec(), Some(b"2".to_vec()))));
    //     assert!(flushed.contains(&(b"c".to_vec(), None)));

    //     assert!(matches!(table.get(b"a"), LookupResult::NotFound));
    // }
}
