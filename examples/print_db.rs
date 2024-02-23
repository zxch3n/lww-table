use lww_table::LwwDb;

pub fn main() {
    let mut db = LwwDb::new();
    db.set("my_table", "row1", "col1", 1);
    db.set("my_table", "row1", "col2", 2);
    db.set("my_table", "row2", "col1", 3);
    db.set("my_table", "row2", "col2", 4);
    println!("{}", db);
    db.delete_row("my_table", "row1");
    println!("{}", db);
}
