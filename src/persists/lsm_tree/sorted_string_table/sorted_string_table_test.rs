mod tests {
    //idk why clippy is marking them as unused here
    //TODO resolve this
    #[allow(unused_imports)]
    use crate::persists::lsm_tree::sorted_string_table::sorted_string_table::SortedStringTable;
    #[allow(unused_imports)]
    use std::path::PathBuf;

    #[test]
    fn read_string_table_and_verify() {
        let file_path = PathBuf::from("test_snapshots/test_sstable.sst");
        assert!(file_path.exists());

        let string_table = SortedStringTable::new(&file_path).expect("Failed to parse SSTable");

        let key = b"key2";
        let result = string_table.get(key);

        assert!(result.is_some(), "Key not found");

        let entry = result.unwrap();

        assert_eq!(entry.key, key);
        assert_eq!(entry.value, b"value2");

        let should_not_be_found = string_table.get(b"key_5");

        assert!(should_not_be_found.is_none());

        println!(
            "Found key: {:?}, value: {:?}",
            std::str::from_utf8(entry.key).unwrap(),
            std::str::from_utf8(entry.value).unwrap()
        );
    }
}
