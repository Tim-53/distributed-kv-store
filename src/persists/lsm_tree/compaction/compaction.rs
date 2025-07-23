use crate::persists::lsm_tree::sorted_string_table::{
    sorted_string_table::SortedStringTable, sst_writer::SSTableWriter, table_result::TableResult,
};
use std::{cmp::Ordering, collections::BinaryHeap, path::PathBuf};

fn compact(tables: Vec<SortedStringTable>) -> Result<PathBuf, std::io::Error> {
    let mut heap = BinaryHeap::<CompactionHeapEntry>::with_capacity(tables.len());
    let mut writer = SSTableWriter::new(std::path::PathBuf::from(format!(
        "L0_{}.sst",
        uuid::Uuid::new_v4()
    )))
    .expect("failed to create writer");

    let mut iters = Vec::new();

    tables.iter().enumerate().for_each(|(index, table)| {
        let mut iter = table.iter();
        if let Some(first) = iter.next() {
            println!("pushed {:?}", first);
            heap.push(CompactionHeapEntry {
                table_result: first,
                table_index: index,
            });
        }
        iters.push(iter);
    });

    let mut last_added = heap
        .pop()
        .expect("There should be minimum one value to compact");

    writer.append_entry(&last_added.table_result);

    if let Some(next_value) = iters[last_added.table_index].next() {
        println!("pushed {:?}", next_value);
        heap.push(CompactionHeapEntry {
            table_result: next_value,
            table_index: last_added.table_index,
        });
    }

    while let Some(next) = heap.pop() {
        if let Some(next_from_same) = iters[next.table_index].next() {
            println!("pushed {:?}", next_from_same);
            heap.push(CompactionHeapEntry {
                table_result: next_from_same,
                table_index: next.table_index,
            });
        }

        if next.table_result.key != last_added.table_result.key {
            writer.append_entry(&next.table_result);
            last_added = next;
        }
    }

    writer.finalize()
}

struct CompactionHeapEntry<'a> {
    table_result: TableResult<'a>,
    table_index: usize,
}

impl<'a> PartialEq for CompactionHeapEntry<'a> {
    fn eq(&self, other: &Self) -> bool {
        self.table_result == other.table_result
    }
}

impl<'a> Eq for CompactionHeapEntry<'a> {}

impl<'a> PartialOrd for CompactionHeapEntry<'a> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        self.table_result.partial_cmp(&other.table_result)
    }
}

impl<'a> Ord for CompactionHeapEntry<'a> {
    fn cmp(&self, other: &Self) -> Ordering {
        self.table_result.cmp(&other.table_result)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    type Entry = (Vec<u8>, (Option<Vec<u8>>, u64));

    fn build_table(dir: &tempfile::TempDir, name: &str, entries: &[Entry]) -> SortedStringTable {
        let path = dir.path().join(name);

        SSTableWriter::write_to_file(&path, entries.to_vec(), 4 * 1024).unwrap();
        SortedStringTable::new(&path).unwrap()
    }

    #[test]
    fn compact_keeps_latest_seq() {
        let tmpdir = tempdir().unwrap();

        // Tabelle 1
        let t1 = build_table(
            &tmpdir,
            "tbl1.sst",
            &[
                (b"a".to_vec(), (Some(b"old".to_vec()), 1)),
                (b"b".to_vec(), (Some(b"x".to_vec()), 1)),
            ],
        );

        let t2 = build_table(
            &tmpdir,
            "tbl2.sst",
            &[
                (b"a".to_vec(), (Some(b"new".to_vec()), 2)),
                (b"c".to_vec(), (Some(b"y".to_vec()), 1)),
            ],
        );

        let out_path = compact(vec![t1, t2]).unwrap();

        let merged = SortedStringTable::new(&out_path).unwrap();
        let keys: Vec<_> = merged
            .iter()
            .map(|r| String::from_utf8_lossy(r.key).into_owned())
            .collect();

        println!("{:?}", keys);
        assert_eq!(keys, ["a", "b", "c"]);

        let merged = SortedStringTable::new(&out_path).unwrap();
        let values: Vec<_> = merged
            .iter()
            .map(|r| String::from_utf8_lossy(r.value).into_owned())
            .collect();

        println!("{:?}", values);
        assert_eq!(values, ["new", "x", "y"]);
    }
}
