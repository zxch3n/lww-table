# Lightweight & Fast LWW CRDT Table

- Supports millions of operations per second
- Suitable for real-time collaboration
- Supports delta updates
- It is a CRDT, which means it possesses strong eventual consistency and can be easily used in distributed environments. It allows for syncing tabular data via peer-to-peer connections, supports end-to-end encryption, and facilitates the development of local-first applications.

Currently, it functions solely as an in-memory table with a unique persistence format and is not a comprehensive database solution. It is not suitable for sparse tables yet.

## Usage

### Update DB

```rust
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
```

Output:

```log
LwwDb {
  # my_table
  +--------+------+------+
  | row_id | col2 | col1 |
  +--------+------+------+
  | row1   | 2    | 1    |
  +--------+------+------+
  | row2   | 4    | 3    |
  +--------+------+------+
}

LwwDb {
  # my_table
  +--------+------+------+
  | row_id | col2 | col1 |
  +--------+------+------+
  | row1   | null | null |
  +--------+------+------+
  | row2   | 4    | 3    |
  +--------+------+------+
}
```


### Sync DB

```rust
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

```

Output:

```log
LwwDb {
  # my_table
  +--------+------+------+
  | row_id | col2 | col1 |
  +--------+------+------+
  | row1   | 2    | 1    |
  +--------+------+------+
  | row2   | 4    | 3    |
  +--------+------+------+
  | row3   | 5    | 3    |
  +--------+------+------+
}

LwwDb {
  # my_table
  +--------+------+------+
  | row_id | col2 | col1 |
  +--------+------+------+
  | row1   | 2    | 1    |
  +--------+------+------+
  | row2   | 4    | 3    |
  +--------+------+------+
  | row3   | 5    | 3    |
  +--------+------+------+
}
```

## Performance

For a table created by the following code:

```rust no_run
let mut db = lww_table::LwwDb::new();
for i in 0..100_000 {
    for j in 0..10 {
        db.set("table", &i.to_string(), j.to_string().as_str(), i + j);
    }
}
```

The benchmark is conducted on MacBook Pro (13-inch, M1, 2020).

| Set             | 344.884ms     |
|-----------------|---------------|
| Export updates  | 272.93475ms   |
| Updates size    | 2552394 bytes |
| Import updates  | 329.477459ms  |
| Export snapshot | 26.450417ms   |
| Snapshot size   | 323420 bytes  |
| Import snapshot | 143.268833ms  |
