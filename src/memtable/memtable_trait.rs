pub enum LookupResult<'a> {
    NotFound,
    Deleted,
    Found(&'a [u8]),
}

pub trait MemTable {
    fn insert(&mut self, key: &[u8], value: &[u8]);
    fn get(&self, key: &[u8]) -> LookupResult;
    // fn range<'a>(
    //     &'a self,
    //     range: impl RangeBounds<&'a [u8]>,
    // ) -> Box<dyn Iterator<Item = (&'a [u8], LookupResult<'a>)> + 'a>;
    fn delete(&mut self, key: &[u8]) -> Option<Option<Vec<u8>>>;
    fn flush(&mut self) -> Vec<(Vec<u8>, Option<Vec<u8>>)>;
}
