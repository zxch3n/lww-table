use lww_table::LwwDb;

pub fn main() {
    let mut db = lww_table::LwwDb::new();
    let start = std::time::Instant::now();
    for j in 0..100 {
        for i in 0..10_000 {
            db.set("table", &i.to_string(), j.to_string().as_str(), i * j);
        }
    }
    println!("1m set: {:?}", start.elapsed());

    let start = std::time::Instant::now();
    let data = db.export_updates(Default::default());
    println!("1m export updates: {:?}", start.elapsed());
    println!("1m export updates size {} bytes", data.len());

    let start = std::time::Instant::now();
    let mut new_db = lww_table::LwwDb::new();
    new_db.import_updates(&data);
    println!("1m import updates: {:?}", start.elapsed());

    let start = std::time::Instant::now();
    let data = db.export_snapshot();
    println!("1m export snapshot: {:?}", start.elapsed());
    println!("1m export snapshot size {} bytes", data.len());

    let start = std::time::Instant::now();
    let mut new_db = LwwDb::from_snapshot(&data);
    println!("1m from_snapshot: {:?}", start.elapsed());
    assert!(db.table_eq(&mut new_db));
}
