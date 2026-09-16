use bytes::{Buf, BufMut, Bytes, BytesMut};
use std::io;
use tokio_util::codec::{Decoder, Encoder};

/// Represents a wire-protocol message containing an arbitrary byte payload.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Frame {
    pub payload: Bytes,
}

impl Frame {
    pub fn new(payload: impl Into<Bytes>) -> Self {
        Self {
            payload: payload.into(),
        }
    }
}

/// Codec for length-prefixed frames:
/// [ 4-byte Big-Endian Length (u32) ] [ Payload Bytes... ]
#[derive(Debug, Default)]
pub struct FrameCodec {
    /// Maximum allowed payload size to prevent OOM DOS attacks
    max_frame_size: usize,
}

impl FrameCodec {
    pub fn new(max_frame_size: usize) -> Self {
        Self { max_frame_size }
    }
}

const U32_SIZE: usize = std::mem::size_of::<u32>();
const DEFAULT_MAX_FRAME_SIZE: usize = 8 * 1024 * 1024; // 8 MB

impl Decoder for FrameCodec {
    type Item = Frame;
    type Error = io::Error;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        if src.len() < U32_SIZE {
            // Not enough bytes to read the length prefix; request more data
            return Ok(None);
        }

        // Read payload length prefix without advancing the buffer read head yet
        let mut length_bytes = [0u8; U32_SIZE];
        length_bytes.copy_from_slice(&src[..U32_SIZE]);
        let payload_len = u32::from_be_bytes(length_bytes) as usize;

        let max_size = if self.max_frame_size > 0 {
            self.max_frame_size
        } else {
            DEFAULT_MAX_FRAME_SIZE
        };

        if payload_len > max_size {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("Frame length {payload_len} exceeds maximum allowed size {max_size}"),
            ));
        }

        // Check if full payload has arrived
        if src.len() < U32_SIZE + payload_len {
            // Reserve extra capacity to avoid frequent re-allocations
            src.reserve((U32_SIZE + payload_len) - src.len());
            return Ok(None);
        }

        // Advance buffer past the length header
        src.advance(U32_SIZE);

        // Extract zero-copy slice for the payload
        let payload = src.split_to(payload_len).freeze();

        Ok(Some(Frame::new(payload)))
    }
}

impl Encoder<Frame> for FrameCodec {
    type Error = io::Error;

    fn encode(&mut self, item: Frame, dst: &mut BytesMut) -> Result<(), Self::Error> {
        let payload_len = item.payload.len();

        let max_size = if self.max_frame_size > 0 {
            self.max_frame_size
        } else {
            DEFAULT_MAX_FRAME_SIZE
        };

        if payload_len > max_size {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("Frame payload size {payload_len} exceeds maximum allowed size {max_size}"),
            ));
        }

        dst.reserve(U32_SIZE + payload_len);
        dst.put_u32(payload_len as u32);
        dst.put(item.payload);

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_encode_decode_roundtrip() {
        let mut codec = FrameCodec::new(1024);
        let mut buf = BytesMut::new();

        let original_frame = Frame::new("hello async broker");
        codec.encode(original_frame.clone(), &mut buf).unwrap();

        let decoded_frame = codec.decode(&mut buf).unwrap().unwrap();
        assert_eq!(original_frame, decoded_frame);
    }

    #[test]
    fn test_partial_read() {
        let mut codec = FrameCodec::new(1024);
        let mut buf = BytesMut::new();

        let original_frame = Frame::new("streaming frame payload");
        codec.encode(original_frame.clone(), &mut buf).unwrap();

        // Split buffer to simulate partial TCP chunk arrival
        let mut partial_buf = buf.split_to(8);

        // Should return Ok(None) because payload is incomplete
        assert!(codec.decode(&mut partial_buf).unwrap().is_none());

        // Append the rest of the bytes back
        partial_buf.unsplit(buf);

        // Now decode should succeed
        let decoded_frame = codec.decode(&mut partial_buf).unwrap().unwrap();
        assert_eq!(original_frame, decoded_frame);
    }
}
