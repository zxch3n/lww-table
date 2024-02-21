pub(super) struct BoolRleEncoder {
    buffer: Vec<u8>,
    last_value: bool,
    repeat: usize,
}

impl BoolRleEncoder {
    pub fn new() -> Self {
        Self {
            buffer: Vec::new(),
            last_value: false,
            repeat: 0,
        }
    }

    pub fn push(&mut self, value: bool) {
        if value == self.last_value {
            self.repeat += 1;
        } else {
            self.flush();
            self.last_value = value;
            self.repeat = 1;
        }
    }

    fn flush(&mut self) {
        leb128::write::unsigned(&mut self.buffer, self.repeat as u64).unwrap();
        self.repeat = 0;
    }

    pub fn finish(mut self) -> Vec<u8> {
        if self.repeat > 0 {
            self.flush();
        }

        self.buffer
    }
}

pub(super) struct BoolRleDecoder<'a> {
    buffer: &'a [u8],
    repeat: usize,
    value: bool,
}

impl<'a> BoolRleDecoder<'a> {
    pub fn new(buffer: &'a [u8]) -> Self {
        Self {
            buffer,
            repeat: 0,
            value: true,
        }
    }
}

impl<'a> Iterator for BoolRleDecoder<'a> {
    type Item = bool;

    fn next(&mut self) -> Option<Self::Item> {
        while self.repeat == 0 {
            if self.buffer.is_empty() {
                return None;
            }

            let repeat = leb128::read::unsigned(&mut self.buffer).unwrap();
            self.repeat = repeat as usize;
            self.value = !self.value;
        }

        self.repeat -= 1;
        Some(self.value)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn basic() {
        let mut encoder = BoolRleEncoder::new();
        encoder.push(true);
        encoder.push(false);

        let encoded = encoder.finish();
        let decoder = BoolRleDecoder::new(&encoded);
        let decoded: Vec<_> = decoder.collect();
        assert_eq!(decoded, vec![true, false]);
    }

    #[test]
    fn bool_rle() {
        let mut encoder = BoolRleEncoder::new();
        encoder.push(true);
        encoder.push(true);
        encoder.push(true);
        encoder.push(false);
        encoder.push(false);
        encoder.push(true);
        encoder.push(true);
        encoder.push(false);
        encoder.push(false);
        encoder.push(false);
        encoder.push(false);
        encoder.push(true);
        encoder.push(true);

        let encoded = encoder.finish();
        let decoder = BoolRleDecoder::new(&encoded);
        let decoded: Vec<_> = decoder.collect();
        assert_eq!(
            decoded,
            vec![
                true, true, true, false, false, true, true, false, false, false, false, true, true
            ]
        );
    }

    #[test]
    fn bool_rle_1() {
        let mut encoder = BoolRleEncoder::new();
        let mut ans = vec![];
        for i in 0..100 {
            let v = i % 3 == 0;
            encoder.push(v);
            ans.push(v);
        }

        let encoded = encoder.finish();
        let decoder = BoolRleDecoder::new(&encoded);
        let decoded: Vec<_> = decoder.collect();
        assert_eq!(decoded, ans);
    }

    #[test]
    fn bool_rle_2() {
        let mut encoder = BoolRleEncoder::new();
        let mut ans = vec![];
        for i in 0..100 {
            let v = i % 3 != 0;
            encoder.push(v);
            ans.push(v);
        }

        let encoded = encoder.finish();
        let decoder = BoolRleDecoder::new(&encoded);
        let decoded: Vec<_> = decoder.collect();
        assert_eq!(decoded, ans);
    }
}
