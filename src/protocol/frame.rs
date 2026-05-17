use bytes::{Buf, BufMut, BytesMut};

pub const FRAME_HEADER_CLIENT: u8 = 0x3c;
pub const FRAME_HEADER_SERVER: u8 = 0x3e;
pub const FRAME_HEADER_SIZE: usize = 3;

#[derive(Debug, Clone)]
pub struct Frame {
    pub header: u8,
    pub payload: Vec<u8>,
}

impl Frame {
    pub fn new(header: u8, payload: Vec<u8>) -> Self {
        Self { header, payload }
    }

    pub fn encode(&self) -> Vec<u8> {
        let len = self.payload.len() as u16;
        let mut buf = Vec::with_capacity(FRAME_HEADER_SIZE + self.payload.len());
        buf.push(self.header);
        buf.put_u16_le(len);
        buf.extend_from_slice(&self.payload);
        buf
    }

    pub fn encode_response(payload: &[u8]) -> Vec<u8> {
        let len = payload.len() as u16;
        let mut buf = Vec::with_capacity(FRAME_HEADER_SIZE + payload.len());
        buf.push(FRAME_HEADER_SERVER);
        buf.put_u16_le(len);
        buf.extend_from_slice(payload);
        buf
    }

    pub fn encode_command(payload: &[u8]) -> Vec<u8> {
        let len = payload.len() as u16;
        let mut buf = Vec::with_capacity(FRAME_HEADER_SIZE + payload.len());
        buf.push(FRAME_HEADER_CLIENT);
        buf.put_u16_le(len);
        buf.extend_from_slice(payload);
        buf
    }
}

#[derive(Debug, Default)]
pub struct FrameParser {
    header_byte: Option<u8>,
    expected_len: Option<u16>,
    buffer: BytesMut,
}

impl FrameParser {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn feed(&mut self, data: &[u8]) -> Vec<Vec<u8>> {
        self.buffer.extend_from_slice(data);
        let mut frames = Vec::new();

        loop {
            if self.header_byte.is_none() {
                if self.buffer.is_empty() {
                    break;
                }
                let h = self.buffer[0];
                if h != FRAME_HEADER_CLIENT && h != FRAME_HEADER_SERVER {
                    tracing::warn!(byte = h, "invalid frame header, skipping");
                    self.buffer.advance(1);
                    continue;
                }
                self.header_byte = Some(h);
                self.buffer.advance(1);
            }

            if self.expected_len.is_none() {
                if self.buffer.len() < 2 {
                    break;
                }
                self.expected_len = Some(self.buffer.get_u16_le());
            }

            let needed = self.expected_len.unwrap() as usize;
            if self.buffer.len() < needed {
                break;
            }

            let payload = self.buffer.split_to(needed).freeze().to_vec();
            frames.push(payload);

            self.header_byte = None;
            self.expected_len = None;
        }

        frames
    }

    pub fn reset(&mut self) {
        self.header_byte = None;
        self.expected_len = None;
        self.buffer.clear();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_frame_roundtrip() {
        let payload = vec![0x01, 0x02, 0x03];
        let frame = Frame::new(FRAME_HEADER_CLIENT, payload.clone());
        let encoded = frame.encode();

        let mut parser = FrameParser::new();
        let frames = parser.feed(&encoded);
        assert_eq!(frames.len(), 1);
        assert_eq!(frames[0], payload);
    }

    #[test]
    fn test_frame_partial_feed() {
        let payload = vec![0x01, 0x02, 0x03];
        let frame = Frame::new(FRAME_HEADER_CLIENT, payload.clone());
        let encoded = frame.encode();

        let mut parser = FrameParser::new();
        let frames1 = parser.feed(&encoded[..2]);
        assert!(frames1.is_empty());

        let frames2 = parser.feed(&encoded[2..]);
        assert_eq!(frames2.len(), 1);
        assert_eq!(frames2[0], payload);
    }

    #[test]
    fn test_multiple_frames() {
        let mut parser = FrameParser::new();
        let mut all_encoded = Vec::new();

        for i in 0..3 {
            let payload = vec![i; 5 + i as usize];
            let frame = Frame::new(FRAME_HEADER_CLIENT, payload);
            all_encoded.extend_from_slice(&frame.encode());
        }

        let frames = parser.feed(&all_encoded);
        assert_eq!(frames.len(), 3);
        assert_eq!(frames[0], vec![0; 5]);
        assert_eq!(frames[1], vec![1; 6]);
        assert_eq!(frames[2], vec![2; 7]);
    }
}
