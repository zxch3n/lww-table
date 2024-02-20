use fxhash::FxHashMap;
use smol_str::SmolStr;

#[derive(Debug, Clone, Hash, PartialEq, Eq)]
pub(crate) struct ColStr;
#[derive(Debug, Clone, Hash, Copy, PartialEq, Eq)]
pub(crate) struct AnyStr;

#[derive(Debug, Clone)]
pub(crate) struct StrPool<T = AnyStr> {
    map: FxHashMap<SmolStr, u32>,
    pool: Vec<SmolStr>,
    _phantom: std::marker::PhantomData<T>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) struct StrIndex<T = AnyStr> {
    index: u32,
    _phantom: std::marker::PhantomData<T>,
}

impl<T> StrPool<T> {
    pub fn new() -> Self {
        Self {
            pool: Vec::new(),
            map: FxHashMap::default(),
            _phantom: std::marker::PhantomData,
        }
    }

    pub fn intern(&mut self, s: &str) -> StrIndex<T> {
        if let Some(&i) = self.map.get(s) {
            return StrIndex {
                index: i,
                _phantom: std::marker::PhantomData,
            };
        }

        let i = self.pool.len() as u32;
        self.pool.push(s.into());
        self.map.insert(s.into(), i);
        StrIndex {
            index: i,
            _phantom: std::marker::PhantomData,
        }
    }

    pub fn get(&self, i: StrIndex<T>) -> &SmolStr {
        &self.pool[i.index as usize]
    }

    pub fn get_index(&self, s: &str) -> Option<StrIndex<T>> {
        self.map.get(s).map(|&i| StrIndex {
            index: i,
            _phantom: std::marker::PhantomData,
        })
    }
}
