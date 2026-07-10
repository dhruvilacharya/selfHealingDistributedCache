use super::parser::RespValue;

/// Encode a RESP simple string: `+<s>\r\n`
pub fn encode_simple_string(s: &str) -> Vec<u8> {
    let mut out = Vec::with_capacity(1 + s.len() + 2);
    out.push(b'+');
    out.extend_from_slice(s.as_bytes());
    out.extend_from_slice(b"\r\n");
    out
}

/// Encode a RESP error: `-<msg>\r\n`
pub fn encode_error(msg: &str) -> Vec<u8> {
    let mut out = Vec::with_capacity(1 + msg.len() + 2);
    out.push(b'-');
    out.extend_from_slice(msg.as_bytes());
    out.extend_from_slice(b"\r\n");
    out
}

/// Encode a RESP integer: `:<n>\r\n`
pub fn encode_integer(n: i64) -> Vec<u8> {
    let s = n.to_string();
    let mut out = Vec::with_capacity(1 + s.len() + 2);
    out.push(b':');
    out.extend_from_slice(s.as_bytes());
    out.extend_from_slice(b"\r\n");
    out
}

/// Encode a RESP bulk string: `$<len>\r\n<data>\r\n`
pub fn encode_bulk_string(data: &[u8]) -> Vec<u8> {
    let len_str = data.len().to_string();
    let mut out = Vec::with_capacity(1 + len_str.len() + 2 + data.len() + 2);
    out.push(b'$');
    out.extend_from_slice(len_str.as_bytes());
    out.extend_from_slice(b"\r\n");
    out.extend_from_slice(data);
    out.extend_from_slice(b"\r\n");
    out
}

/// Encode a RESP null (null bulk string): `$-1\r\n`
pub fn encode_null() -> Vec<u8> {
    b"$-1\r\n".to_vec()
}

/// Encode a RESP array.
pub fn encode_array(items: &[RespValue]) -> Vec<u8> {
    let mut out = Vec::new();
    out.push(b'*');
    out.extend_from_slice(items.len().to_string().as_bytes());
    out.extend_from_slice(b"\r\n");
    for item in items {
        match item {
            RespValue::SimpleString(s) => out.extend_from_slice(&encode_simple_string(s)),
            RespValue::Error(e) => out.extend_from_slice(&encode_error(e)),
            RespValue::Integer(n) => out.extend_from_slice(&encode_integer(*n)),
            RespValue::BulkString(d) => out.extend_from_slice(&encode_bulk_string(d)),
            RespValue::Null => out.extend_from_slice(&encode_null()),
            RespValue::Array(inner) => out.extend_from_slice(&encode_array(inner)),
        }
    }
    out
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_encode_simple_string() {
        assert_eq!(encode_simple_string("OK"), b"+OK\r\n");
    }

    #[test]
    fn test_encode_error() {
        assert_eq!(encode_error("ERR something"), b"-ERR something\r\n");
    }

    #[test]
    fn test_encode_integer() {
        assert_eq!(encode_integer(42), b":42\r\n");
        assert_eq!(encode_integer(-1), b":-1\r\n");
        assert_eq!(encode_integer(0), b":0\r\n");
    }

    #[test]
    fn test_encode_bulk_string() {
        assert_eq!(encode_bulk_string(b"hello"), b"$5\r\nhello\r\n");
        assert_eq!(encode_bulk_string(b""), b"$0\r\n\r\n");
    }

    #[test]
    fn test_encode_null() {
        assert_eq!(encode_null(), b"$-1\r\n");
    }

    #[test]
    fn test_encode_array() {
        let items = vec![
            RespValue::BulkString(b"GET".to_vec()),
            RespValue::BulkString(b"foo".to_vec()),
        ];
        let encoded = encode_array(&items);
        assert_eq!(encoded, b"*2\r\n$3\r\nGET\r\n$3\r\nfoo\r\n");
    }

    #[test]
    fn test_encode_empty_array() {
        let items: Vec<RespValue> = vec![];
        let encoded = encode_array(&items);
        assert_eq!(encoded, b"*0\r\n");
    }
}
