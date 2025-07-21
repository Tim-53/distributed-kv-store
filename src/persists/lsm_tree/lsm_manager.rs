use std::{error::Error, fs, path::Path};

use crate::persists::lsm_tree::sorted_string_table::sorted_string_table::{
    SortedStringTable, TableResult,
};

pub struct LsmManager {
    tree: Vec<TreeLevel>,
}

impl LsmManager {
    pub fn new() -> Self {
        LsmManager { tree: Vec::new() }
    }

    pub async fn initialize(&mut self) -> Result<(), Box<dyn Error>> {
        //load config
        // load tables

        let path = "./some_folder"; // Path to the folder

        if Path::new(path).is_dir() {
            for entry in fs::read_dir(path)? {
                let entry = entry?;
                let path = entry.path();
                println!("{}", path.display());

                match TreeLevel::new(&path) {
                    Ok(tree) => self.tree.push(tree),
                    Err(_) => todo!(),
                }
            }
        }

        Ok(())
    }
    pub fn add_table(&mut self, path: &Path) {
        //TODO handle error
        let table = SortedStringTable::new(path).unwrap();
        self.tree[0].add(table);
    }

    pub fn get_value(&self, key: &[u8]) -> Option<TableResult<'_>> {
        for tree_level in &self.tree {
            if let Some(table_result) = tree_level.get_value(key) {
                return Some(table_result);
            };
        }
        None
    }
}

struct TreeLevel {
    tables: Vec<SortedStringTable>,
}

impl TreeLevel {
    pub fn new(path: &Path) -> Result<Self, Box<dyn Error>> {
        if !path.is_dir() {
            return Err(format!("Given path is not a directory: {}", path.display()).into());
        }

        let mut tables = Vec::new();

        for entry in fs::read_dir(path)? {
            let file_path = entry?.path();
            let table = SortedStringTable::new(&file_path)?;
            tables.push(table);
        }

        Ok(TreeLevel { tables })
    }

    pub fn add(&mut self, table: SortedStringTable) {
        self.tables.push(table);
    }

    pub fn get_value(&self, key: &[u8]) -> Option<TableResult<'_>> {
        self.tables
            .iter()
            .filter_map(|table| table.get(key)) // skips None, unwraps Some
            .max_by_key(|result| result.sequence_number)
    }
}
