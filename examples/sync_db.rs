use lww_table::{LwwDb, VectorClock};

pub fn main() {
    let mut db = LwwDb::new();
    db.set("my_table", "row1", "col1", 1);
    db.set("my_table", "row1", "col2", 2);
    db.set("my_table", "row2", "col1", 3);
    db.set("my_table", "row2", "col2", 4);
    let mut db2 = LwwDb::new();
    db2.set("my_table", "row3", "col1", 3);
    db2.set("my_table", "row3", "col2", 5);

    let version: Vec<u8> = db2.version().encode();
    // you can send version via network, save to disk, etc.
    let bytes: Vec<u8> = db.export_updates(VectorClock::decode(&version));
    // you can send bytes via network, save to disk, etc.
    db2.import_updates(&bytes);

    // sync in the other direction
    db.import_updates(&db2.export_updates(db.version().clone()));

    // now two databases are in sync
    assert!(db.check_eq(&mut db2));
    println!("{}", db);
    println!("{}", db2);
}
