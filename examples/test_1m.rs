use lww_table::LwwDb;

pub fn main() {
    let mut db = lww_table::LwwDb::new();
    let start = std::time::Instant::now();
    for i in 0..1_000_000 {
        db.set("table", &i.to_string(), "from", i as f64);
        db.set("table", &i.to_string(), "to", i as f64 + 100.0);
    }
    // println!("1m set: {:?}", start.elapsed());

    // let start = std::time::Instant::now();
    // let data = db.export_updates(Default::default());
    // println!("1m export updates: {:?}", start.elapsed());
    // println!("1m export updates size {} bytes", data.len());

    // let start = std::time::Instant::now();
    // let mut new_db = lww_table::LwwDb::new();
    // new_db.import_updates(&data);
    // println!("1m import updates: {:?}", start.elapsed());

    let start = std::time::Instant::now();
    let data = db.export_snapshot();
    println!("1m export snapshot: {:?}", start.elapsed());
    println!("1m export snapshot size {} bytes", data.len());

    let start = std::time::Instant::now();
    let new_db = LwwDb::from_snapshot(&data);
    println!("1m import updates: {:?}", start.elapsed());
    assert!(db.table_eq(&new_db));
}
