//! Wire envelope for `publish`/`subscribe` payloads.
//!
//! etcd KV values are opaque bytes — no per-event metadata
//! channel for routing keys. So when an operator calls
//! `publish(topic, Some("only-this"), b"hi")`, we have to
//! round-trip the routing key inside the value itself.
//!
//! Format (little overhead, easy to parse, versioned):
//!
//! ```text
//! [ 1 byte ] version (currently 0x01)
//! [ 1 byte ] flag: 0x00 = no routing key, 0x01 = routing key follows
//! [ 2 bytes BE u16 ] routing_key length    \  only present if flag=0x01
//! [ N bytes UTF-8 ] routing_key bytes      /
//! [ rest ] caller payload (untouched)
//! ```
//!
//! Mirrors the consul plugin's envelope.rs — the format choice is
//! deliberate so an operator inspecting either backend's wire
//! values gets the same shape. We keep the modules separate
//! rather than a shared crate because each backend already owns
//! its own serialisation surface and a shared crate would couple
//! their release cadences.

use bytes::Bytes;

const VERSION: u8 = 0x01;
const FLAG_NONE: u8 = 0x00;
const FLAG_RK: u8 = 0x01;
const MAX_RK_LEN: usize = u16::MAX as usize;

#[derive(Debug, thiserror::Error)]
pub enum EnvelopeError {
    #[error("envelope: routing key {0} bytes exceeds max {MAX_RK_LEN}")]
    TooLong(usize),
    #[error("envelope: header truncated (need >= 2 bytes, got {0})")]
    Truncated(usize),
    #[error("envelope: unsupported version {0:#04x}")]
    UnsupportedVersion(u8),
    #[error("envelope: unknown flag {0:#04x}")]
    UnknownFlag(u8),
    #[error("envelope: routing-key length truncated")]
    LengthTruncated,
    #[error("envelope: routing-key body truncated (header says {expected}, have {actual})")]
    BodyTruncated { expected: usize, actual: usize },
    #[error("envelope: routing key not valid UTF-8")]
    InvalidUtf8,
}

pub fn encode(routing_key: Option<&str>, payload: &[u8]) -> Result<Bytes, EnvelopeError> {
    let mut out = Vec::with_capacity(payload.len() + 4);
    out.push(VERSION);
    match routing_key {
        None => out.push(FLAG_NONE),
        Some(rk) => {
            let rk_bytes = rk.as_bytes();
            if rk_bytes.len() > MAX_RK_LEN {
                return Err(EnvelopeError::TooLong(rk_bytes.len()));
            }
            out.push(FLAG_RK);
            out.extend_from_slice(&(rk_bytes.len() as u16).to_be_bytes());
            out.extend_from_slice(rk_bytes);
        }
    }
    out.extend_from_slice(payload);
    Ok(Bytes::from(out))
}

pub fn decode(bytes: &[u8]) -> Result<(Option<String>, Bytes), EnvelopeError> {
    if bytes.len() < 2 {
        return Err(EnvelopeError::Truncated(bytes.len()));
    }
    let version = bytes[0];
    if version != VERSION {
        return Err(EnvelopeError::UnsupportedVersion(version));
    }
    match bytes[1] {
        FLAG_NONE => Ok((None, Bytes::copy_from_slice(&bytes[2..]))),
        FLAG_RK => {
            if bytes.len() < 4 {
                return Err(EnvelopeError::LengthTruncated);
            }
            let rk_len = u16::from_be_bytes([bytes[2], bytes[3]]) as usize;
            let body_start: usize = 4;
            let body_end = body_start
                .checked_add(rk_len)
                .ok_or(EnvelopeError::LengthTruncated)?;
            if bytes.len() < body_end {
                return Err(EnvelopeError::BodyTruncated {
                    expected: rk_len,
                    actual: bytes.len() - body_start,
                });
            }
            let rk = std::str::from_utf8(&bytes[body_start..body_end])
                .map_err(|_| EnvelopeError::InvalidUtf8)?
                .to_owned();
            Ok((Some(rk), Bytes::copy_from_slice(&bytes[body_end..])))
        }
        flag => Err(EnvelopeError::UnknownFlag(flag)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn roundtrip_none() {
        let env = encode(None, b"hello").unwrap();
        let (rk, payload) = decode(&env).unwrap();
        assert_eq!(rk, None);
        assert_eq!(&payload[..], b"hello");
    }

    #[test]
    fn roundtrip_some() {
        let env = encode(Some("only-this"), b"deliver").unwrap();
        let (rk, payload) = decode(&env).unwrap();
        assert_eq!(rk.as_deref(), Some("only-this"));
        assert_eq!(&payload[..], b"deliver");
    }

    #[test]
    fn empty_payload_ok() {
        let env = encode(Some("rk"), b"").unwrap();
        let (rk, payload) = decode(&env).unwrap();
        assert_eq!(rk.as_deref(), Some("rk"));
        assert!(payload.is_empty());
    }

    #[test]
    fn empty_routing_key_string() {
        let env = encode(Some(""), b"x").unwrap();
        let (rk, payload) = decode(&env).unwrap();
        assert_eq!(rk.as_deref(), Some(""));
        assert_eq!(&payload[..], b"x");
    }

    #[test]
    fn unicode_routing_key_roundtrips() {
        let env = encode(Some("ключ-α"), b"x").unwrap();
        let (rk, payload) = decode(&env).unwrap();
        assert_eq!(rk.as_deref(), Some("ключ-α"));
        assert_eq!(&payload[..], b"x");
    }

    #[test]
    fn rejects_too_long_routing_key() {
        let big = "a".repeat(MAX_RK_LEN + 1);
        let err = encode(Some(&big), b"x").unwrap_err();
        assert!(matches!(err, EnvelopeError::TooLong(n) if n == MAX_RK_LEN + 1));
    }

    #[test]
    fn rejects_short_envelope() {
        assert!(matches!(
            decode(&[]).unwrap_err(),
            EnvelopeError::Truncated(0)
        ));
        assert!(matches!(
            decode(&[0x01]).unwrap_err(),
            EnvelopeError::Truncated(1)
        ));
    }

    #[test]
    fn rejects_unknown_version() {
        let err = decode(&[0x02, 0x00]).unwrap_err();
        assert!(matches!(err, EnvelopeError::UnsupportedVersion(0x02)));
    }

    #[test]
    fn rejects_unknown_flag() {
        let err = decode(&[0x01, 0x55]).unwrap_err();
        assert!(matches!(err, EnvelopeError::UnknownFlag(0x55)));
    }

    #[test]
    fn rejects_truncated_length() {
        let err = decode(&[0x01, 0x01, 0x00]).unwrap_err();
        assert!(matches!(err, EnvelopeError::LengthTruncated));
    }

    #[test]
    fn rejects_truncated_body() {
        let err = decode(&[0x01, 0x01, 0x00, 0x04, b'x', b'y']).unwrap_err();
        assert!(matches!(
            err,
            EnvelopeError::BodyTruncated {
                expected: 4,
                actual: 2
            }
        ));
    }

    #[test]
    fn rejects_invalid_utf8() {
        let err = decode(&[0x01, 0x01, 0x00, 0x02, 0xC3, 0x28]).unwrap_err();
        assert!(matches!(err, EnvelopeError::InvalidUtf8));
    }
}
