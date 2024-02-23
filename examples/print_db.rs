use lww_table::LwwDb;
use std::collections::HashSet;

pub fn main() {
    let mut db = LwwDb::new();
    db.set("my_table", "row1", "col1", 1);
    db.set("my_table", "row1", "col2", 2);
    db.set("my_table", "row2", "col1", 3);
    db.set("my_table", "row2", "col2", 4);
    assert_eq!(db.get_cell("my_table", "row1", "col1").unwrap(), &1.into());
    assert_eq!(
        db.iter_row("my_table", "row1").collect::<HashSet<_>>(),
        HashSet::from([("col1", &1.into()), ("col2", &2.into())])
    );
    println!("{}", db);
    db.delete_row("my_table", "row1");
    println!("{}", db);
}
