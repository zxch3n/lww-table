use std::iter::once;

use lww_table::LwwDb;
use tabled::settings::Style;

pub fn main() {
    let mut table_builder = tabled::builder::Builder::new();
    let mut db = lww_table::LwwDb::new();
    let start = std::time::Instant::now();
    for i in 0..100_000 {
        for j in 0..10 {
            db.set("table", &i.to_string(), j.to_string().as_str(), i + j);
        }
    }

    table_builder
        .push_record(once("Set".to_string()).chain(once(format!("{:?}", start.elapsed()))));
    println!("1m set: {:?}", start.elapsed());

    let start = std::time::Instant::now();
    let data = db.export_updates(Default::default());
    println!("1m export updates: {:?}", start.elapsed());
    println!("1m export updates size {} bytes", data.len());
    table_builder.push_record(
        once("Export updates".to_string()).chain(once(format!("{:?}", start.elapsed()))),
    );
    table_builder
        .push_record(once("Updates size".to_string()).chain(once(format!("{} bytes", data.len()))));

    let start = std::time::Instant::now();
    let mut new_db = lww_table::LwwDb::new();
    new_db.import_updates(&data);
    println!("1m import updates: {:?}", start.elapsed());
    table_builder.push_record(
        once("Import updates".to_string()).chain(once(format!("{:?}", start.elapsed()))),
    );

    let start = std::time::Instant::now();
    let data = db.export_snapshot();
    println!("1m export snapshot: {:?}", start.elapsed());
    println!("1m export snapshot size {} bytes", data.len());
    table_builder.push_record(
        once("Export snapshot".to_string()).chain(once(format!("{:?}", start.elapsed()))),
    );
    table_builder.push_record(
        once("Snapshot size".to_string()).chain(once(format!("{} bytes", data.len()))),
    );

    let start = std::time::Instant::now();
    let mut new_db = LwwDb::from_snapshot(&data);
    println!("1m from_snapshot: {:?}", start.elapsed());
    table_builder.push_record(
        once("Import snapshot".to_string()).chain(once(format!("{:?}", start.elapsed()))),
    );
    assert!(db.check_eq(&mut new_db));
    let mut table = table_builder.build();
    table.with(Style::markdown());
    println!("{}", table);
}
