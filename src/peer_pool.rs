use crate::clock::Peer;

#[derive(Debug, Clone)]
pub struct PeerPool {
    vec: Vec<Peer>,
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct PeerIdx(u32);

impl PeerIdx {
    pub fn cmp(&self, other: &Self, ctx: &PeerPool) -> std::cmp::Ordering {
        ctx.get(*self).cmp(&ctx.get(*other))
    }

    pub const NULL: Self = Self(0);
}

impl PeerPool {
    pub fn new() -> Self {
        Self { vec: Vec::new() }
    }

    pub fn intern(&mut self, peer: Peer) -> PeerIdx {
        let idx = self.vec.len() as u32;
        self.vec.push(peer);
        PeerIdx(idx + 1)
    }

    pub fn get(&self, idx: PeerIdx) -> Option<Peer> {
        if idx.0 == 0 {
            return None;
        }

        Some(self.vec[idx.0 as usize - 1])
    }
}
