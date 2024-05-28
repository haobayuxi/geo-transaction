

pub fn init_retwis_db() -> Vec<HashMap<u64, RwLock<Tuple>>> {
    let value: Vec<char> = vec!['a'; 40];
    let mut write_value = String::from("");
    write_value.extend(value.iter());
    let mut tables = Vec::new();
    let mut table = HashMap::new();
    for i in 0..MicroTableSize {
        table.insert(i, RwLock::new(Tuple::new(write_value.clone())));
    }
    tables.push(table);
    tables
}
