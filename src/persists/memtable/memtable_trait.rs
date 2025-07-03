#[derive(Debug)]
pub enum LookupResult<'a> {
    NotFound,
    Deleted(u64),
    Found((&'a [u8], u64)),
}
pub type MemTableValue = (Option<Vec<u8>>, u64);

pub trait MemTable {
    fn insert(&mut self, key: &[u8], value: &[u8], seq_number: u64);
    fn get(&self, key: &[u8]) -> LookupResult;
    // fn range<'a>(
    //     &'a self,
    //     range: impl RangeBounds<&'a [u8]>,
    // ) -> Box<dyn Iterator<Item = (&'a [u8], LookupResult<'a>)> + 'a>;
    fn delete(&mut self, key: &[u8], seq_number: u64) -> Option<MemTableValue>;
    fn flush(&mut self) -> Vec<(Vec<u8>, MemTableValue)>;

    fn bytes_used(&self) -> usize;
    fn inc_bytes_used(&mut self, delta: usize);
    fn has_capacity(&self, value_length: usize) -> bool;
}
