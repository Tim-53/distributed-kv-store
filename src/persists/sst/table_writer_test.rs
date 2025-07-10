mod tests {

    use std::fs;
    use std::path::PathBuf;

    use crate::persists::sst::ss_table_reader::SSTableReader;
    
    use crate::persists::sst::sst_writer::SSTableWriter;

    #[test]
    fn test_write_simple_sstable() {
        let entries = vec![
            (b"key1".to_vec(), (Some(b"value1".to_vec()), 1)),
            (b"key2".to_vec(), (Some(b"value2".to_vec()), 2)),
            (b"key3".to_vec(), (Some(b"value3".to_vec()), 3)),
        ];

        let estimated_size: usize = entries
            .iter()
            .map(|(k, (v, _s))| {
                4 + k.len()
                    + 4
                    + v.as_ref()
                        .map_or(0, |v| v.len() + std::mem::size_of::<u64>())
            })
            .sum();

        let tmp_file = PathBuf::from("test_sstable.sst");

        let _ = fs::remove_file(&tmp_file);

        let writer = SSTableWriter {};

        writer
            .write_to_file(&tmp_file, entries, estimated_size as u32)
            .expect("write_to_file failed");
        assert!(tmp_file.exists());
        let metadata = fs::metadata(&tmp_file).unwrap();
        let file_size = metadata.len() as usize;

        assert!(file_size > 0);
        //TODO check metadata later
        // assert_eq!(file_size - 8 % BLOCK_SIZE, 0, "file not block-aligned");

        SSTableReader::parse_file(&tmp_file);

        // let _ = fs::remove_file(&tmp_file);
    }
}
