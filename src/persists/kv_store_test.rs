#[cfg(test)]
mod tests {
    use crate::persists::KvStore;

    type TestKvStore = KvStore<{ 64 }>;

    #[tokio::test]
    async fn test_insert_and_get() {
        let mut store = TestKvStore::new().await;

        store
            .put_value("foo", "bar")
            .await
            .expect("Failed to put value");

        match store.get_value("foo") {
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

        assert_eq!(flushable_tables.len(), 1);

        let active_keys: Vec<_> = active_store
            .iter_all()
            .map(|(k, _)| String::from_utf8_lossy(k).to_string())
            .collect();
        let flush_keys: Vec<_> = flushable_tables[0]
            .iter_all()
            .map(|(k, _)| String::from_utf8_lossy(k).to_string())
            .collect();

        assert_eq!(active_keys, vec!["key4"]);
        assert_eq!(flush_keys, vec!["key1", "key2", "key3"]);
    }
}
