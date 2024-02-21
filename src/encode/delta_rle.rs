pub(super) struct DeltaRleEncoder {
    buffer: Vec<u8>,
    last_value: i64,
    last_delta: i64,
    repeat: i64,
}

impl DeltaRleEncoder {
    pub fn new() -> Self {
        Self {
            buffer: Vec::new(),
            last_value: 0,
            last_delta: 0,
            repeat: 0,
        }
    }

    pub fn push(&mut self, value: i64) {
        let delta = value - self.last_value;
        if delta == self.last_delta {
            self.repeat += 1;
        } else {
            self.flush();
            self.last_delta = delta;
            self.repeat = 1;
        }
        self.last_value = value;
    }

    fn flush(&mut self) {
        if self.repeat > 0 {
            leb128::write::signed(&mut self.buffer, self.repeat).unwrap();
            leb128::write::signed(&mut self.buffer, self.last_delta).unwrap();
            self.repeat = 0;
        }
    }

    pub fn finish(mut self) -> Vec<u8> {
        self.flush();
        self.buffer
    }
}

pub(super) struct DeltaRleDecoder<'a> {
    buffer: &'a [u8],
    value: i64,
    repeat: i64,
    last_delta: i64,
}

impl<'a> DeltaRleDecoder<'a> {
    pub(crate) fn new(buffer: &'a [u8]) -> Self {
        Self {
            buffer,
            value: 0,
            repeat: 0,
            last_delta: 0,
        }
    }
}

impl<'a> Iterator for DeltaRleDecoder<'a> {
    type Item = i64;

    fn next(&mut self) -> Option<Self::Item> {
        if self.repeat == 0 {
            if self.buffer.is_empty() {
                return None;
            }

            self.repeat = leb128::read::signed(&mut self.buffer).unwrap();
            self.last_delta = leb128::read::signed(&mut self.buffer).unwrap();
        }
        self.repeat -= 1;
        self.value += self.last_delta;
        Some(self.value)
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_delta_rle() {
        let mut encoder = DeltaRleEncoder::new();
        encoder.push(-111);
        encoder.push(-111);
        encoder.push(111);
        encoder.push(111);
        encoder.push(1);
        encoder.push(1);
        encoder.push(2);
        encoder.push(2);
        encoder.push(3);
        encoder.push(3);
        encoder.push(3);
        encoder.push(22);
        encoder.push(22);
        let bytes = encoder.finish();
        let decoder = DeltaRleDecoder::new(&bytes);
        let ans: Vec<_> = decoder.collect();
        assert_eq!(ans, vec![-111, -111, 111, 111, 1, 1, 2, 2, 3, 3, 3, 22, 22])
    }

    #[test]
    fn encode_decode_0() {
        let mut encoder = DeltaRleEncoder::new();
        let mut values = Vec::new();
        for v in 0..1000 {
            encoder.push(v);
            values.push(v);
        }
        let bytes = encoder.finish();
        let decoder = DeltaRleDecoder::new(&bytes);
        let ans: Vec<_> = decoder.collect();
        assert_eq!(ans, values);
    }

    #[test]
    fn encode_decode_1() {
        let mut encoder = DeltaRleEncoder::new();
        let mut values = Vec::new();
        for v in 0..1000 {
            let v = (v * v * 10 + 1) % 10007;
            encoder.push(v);
            values.push(v);
        }
        let bytes = encoder.finish();
        let decoder = DeltaRleDecoder::new(&bytes);
        let ans: Vec<_> = decoder.collect();
        assert_eq!(ans, values);
    }
}
