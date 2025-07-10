use crate::persists::lsm_tree::sorted_string_table::SortedStringTable;

struct LsmManager {
    tree: Vec<Vec<SortedStringTable>>,
}

impl LsmManager {
    pub fn new() -> Self {
        //load config
        // load tables
        LsmManager { tree: Vec::new() }
    }
}
