use smol_str::SmolStr;

#[derive(Debug, Clone)]
pub enum Value {
    Double(f64),
    Str(SmolStr),
    Null,
}
