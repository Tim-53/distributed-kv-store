#[cfg(test)]
mod tests {
    use crate::persists::{
        KvStore,
        memtable::{btree_map::BTreeMemTable, memtable_trait::MemTable},
    };
    use std::sync::Arc;
    use tokio::task::JoinSet;

    type TestKvStore = KvStore<64>;

    #[tokio::test]
    async fn test_insert_and_get() {
        let store = TestKvStore::new().await;

        store
            .put_value("foo", "bar")
            .await
            .expect("Failed to put value");

        match store.get_value("foo").await {
            Some(val) => assert_eq!("bar", val),
            _ => panic!("Expected Found"),
        }
    }

    #[tokio::test]
    async fn test_will_be_flushed() {
        let mut store = TestKvStore::new().await;
        let value = "abcdefgh";

        let _ = store.put_value("key1", value).await;
        let _ = store.put_value("key2", value).await;
        let _ = store.delete_value("key1").await;
        let _ = store.put_value("key3", value).await;
        let _ = store.put_value("key4", value).await;

        let active_store = store.store;
        let flushable_tables = store.flushable_tables;

        assert_eq!(flushable_tables.read().await.len(), 1);

        let active_keys: Vec<_> = active_store
            .read()
            .await
            .iter_all()
            .map(|(k, _)| String::from_utf8_lossy(k).to_string())
            .collect();
        let flush_keys: Vec<_> = flushable_tables.read().await[0]
            .iter_all()
            .map(|(k, _)| String::from_utf8_lossy(k).to_string())
            .collect();

        assert_eq!(active_keys, vec!["key4"]);
        assert_eq!(flush_keys, vec!["key1", "key2", "key3"]);
    }

    #[tokio::test]
    async fn seq_numbers_are_unique_and_monotone() {
        let store = TestKvStore::new().await;

        let value = "value";

        let mut seq_nums: Vec<u64> = Vec::new();

        for i in 0..5000 {
            seq_nums.push(
                store
                    .put_value(&i.to_string(), value)
                    .await
                    .expect("put failed"),
            );
        }

        seq_nums.sort();

        for i in 1..seq_nums.len() {
            assert!(seq_nums[i] > seq_nums[i - 1]);
        }
    }

    #[tokio::test]
    async fn seq_numbers_are_unique_and_monotone_parallel_insert() {
        let store = Arc::new(TestKvStore::new().await);
        let value = "value";

        let mut join_set = JoinSet::new();

        for i in 0..55000 {
            let key = i.to_string();
            let value = value.to_string();
            let store = Arc::clone(&store);

            join_set.spawn(async move { store.put_value(&key, &value).await.expect("put failed") });
        }

        let mut seq_nums: Vec<u64> = Vec::with_capacity(12000);

        while let Some(result) = join_set.join_next().await {
            let seq = result.expect("task panicked");
            seq_nums.push(seq);
        }

        // Sortieren und testen
        seq_nums.sort();

        for i in 1..seq_nums.len() {
            assert!(
                seq_nums[i] > seq_nums[i - 1],
                "duplicate or out-of-order seq at index {}: {} vs {}",
                i,
                seq_nums[i - 1],
                seq_nums[i]
            );
        }
    }

    #[tokio::test]
    async fn value_from_mem_is_returned_over_flushable() {
        let store = Arc::new(TestKvStore::new().await);

        let mut active_memtable = BTreeMemTable::<64>::new();
        active_memtable.insert(b"key1", b"correct_value", 300);

        {
            let mut store_guard = store.store.write().await;
            *store_guard = active_memtable;
        }

        let mut flush1 = BTreeMemTable::<64>::new();
        flush1.insert(b"key1", b"outdated_low", 100);

        let flush_arc1 = Arc::new(flush1);

        let mut flush2 = BTreeMemTable::<64>::new();
        flush2.insert(b"key1", b"outdated_high", 200);

        let flush_arc2 = Arc::new(flush2);

        let mut flushables_guard = store.flushable_tables.write().await;
        *flushables_guard = vec![flush_arc1, flush_arc2];

        let result = store.get_value("key1").await;
        assert_eq!(result, Some("correct_value".into()));
    }
    #[tokio::test]
    async fn value_is_selected_from_highest_seq_flushable_when_memtable_empty() {
        let store = Arc::new(TestKvStore::new().await);
        println!("hier0");
        let mut flush1 = BTreeMemTable::<64>::new();
        flush1.insert(b"key1", b"outdated_low", 100);
        let flush_arc1 = Arc::new(flush1);

        let mut flush2 = BTreeMemTable::<64>::new();
        flush2.insert(b"key1", b"correct_value", 200);
        let flush_arc2 = Arc::new(flush2);

        {
            let mut flushables_guard = store.flushable_tables.write().await;
            *flushables_guard = vec![flush_arc1, flush_arc2];
        }

        println!("hier");

        let result = store.get_value("key1").await;
        assert_eq!(result, Some("correct_value".into()));
    }
}
