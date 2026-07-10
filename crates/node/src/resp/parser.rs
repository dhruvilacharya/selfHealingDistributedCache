use std::str;

/// Represents a RESP2 value.
#[derive(Debug, Clone, PartialEq)]
pub enum RespValue {
    SimpleString(String),
    Error(String),
    Integer(i64),
    BulkString(Vec<u8>),
    Null,
    Array(Vec<RespValue>),
}

/// Result of attempting to parse a RESP value from a byte buffer.
#[derive(Debug)]
pub enum ParseResult {
    /// Successfully parsed a value; `usize` is the number of bytes consumed.
    Complete(RespValue, usize),
    /// Buffer does not contain a complete RESP value yet.
    Incomplete,
    /// A protocol-level error occurred.
    Error(String),
}

/// Locate the next `\r\n` in `buf` starting at `offset`.
/// Returns the position of `\r` (i.e. the line content is `buf[offset..pos]`).
fn find_crlf(buf: &[u8], offset: usize) -> Option<usize> {
    if buf.len() < offset + 2 {
        return None;
    }
    for i in offset..buf.len() - 1 {
        if buf[i] == b'\r' && buf[i + 1] == b'\n' {
            return Some(i);
        }
    }
    None
}

/// Read a line terminated by `\r\n` starting at `offset`.
/// Returns `(line_bytes, position_after_crlf)` or `None` if incomplete.
fn read_line(buf: &[u8], offset: usize) -> Option<(&[u8], usize)> {
    let cr = find_crlf(buf, offset)?;
    let line = &buf[offset..cr];
    Some((line, cr + 2))
}

/// Parse a single RESP value from `buf`.
pub fn parse_resp(buf: &[u8]) -> ParseResult {
    if buf.is_empty() {
        return ParseResult::Incomplete;
    }

    match buf[0] {
        b'+' => parse_simple_string(buf),
        b'-' => parse_error(buf),
        b':' => parse_integer(buf),
        b'$' => parse_bulk_string(buf),
        b'*' => parse_array(buf),
        _ => ParseResult::Error(format!(
            "unexpected byte: 0x{:02x}",
            buf[0]
        )),
    }
}

fn parse_simple_string(buf: &[u8]) -> ParseResult {
    // Skip the leading '+'
    match read_line(buf, 1) {
        Some((line, next)) => {
            match str::from_utf8(line) {
                Ok(s) => ParseResult::Complete(RespValue::SimpleString(s.to_string()), next),
                Err(e) => ParseResult::Error(format!("invalid UTF-8 in simple string: {}", e)),
            }
        }
        None => ParseResult::Incomplete,
    }
}

fn parse_error(buf: &[u8]) -> ParseResult {
    match read_line(buf, 1) {
        Some((line, next)) => {
            match str::from_utf8(line) {
                Ok(s) => ParseResult::Complete(RespValue::Error(s.to_string()), next),
                Err(e) => ParseResult::Error(format!("invalid UTF-8 in error: {}", e)),
            }
        }
        None => ParseResult::Incomplete,
    }
}

fn parse_integer(buf: &[u8]) -> ParseResult {
    match read_line(buf, 1) {
        Some((line, next)) => {
            match str::from_utf8(line) {
                Ok(s) => match s.parse::<i64>() {
                    Ok(n) => ParseResult::Complete(RespValue::Integer(n), next),
                    Err(e) => ParseResult::Error(format!("invalid integer: {}", e)),
                },
                Err(e) => ParseResult::Error(format!("invalid UTF-8 in integer: {}", e)),
            }
        }
        None => ParseResult::Incomplete,
    }
}

fn parse_bulk_string(buf: &[u8]) -> ParseResult {
    // Read the length line: `$<len>\r\n`
    let (len_line, after_len) = match read_line(buf, 1) {
        Some(v) => v,
        None => return ParseResult::Incomplete,
    };

    let len_str = match str::from_utf8(len_line) {
        Ok(s) => s,
        Err(e) => return ParseResult::Error(format!("invalid UTF-8 in bulk string length: {}", e)),
    };

    let len: i64 = match len_str.parse() {
        Ok(n) => n,
        Err(e) => return ParseResult::Error(format!("invalid bulk string length: {}", e)),
    };

    // Null bulk string
    if len < 0 {
        return ParseResult::Complete(RespValue::Null, after_len);
    }

    let len = len as usize;
    let data_end = after_len + len;
    let total_end = data_end + 2; // trailing \r\n

    if buf.len() < total_end {
        return ParseResult::Incomplete;
    }

    // Verify trailing CRLF
    if buf[data_end] != b'\r' || buf[data_end + 1] != b'\n' {
        return ParseResult::Error("bulk string not terminated by CRLF".to_string());
    }

    let data = buf[after_len..data_end].to_vec();
    ParseResult::Complete(RespValue::BulkString(data), total_end)
}

fn parse_array(buf: &[u8]) -> ParseResult {
    // Read the count line: `*<count>\r\n`
    let (count_line, after_count) = match read_line(buf, 1) {
        Some(v) => v,
        None => return ParseResult::Incomplete,
    };

    let count_str = match str::from_utf8(count_line) {
        Ok(s) => s,
        Err(e) => return ParseResult::Error(format!("invalid UTF-8 in array count: {}", e)),
    };

    let count: i64 = match count_str.parse() {
        Ok(n) => n,
        Err(e) => return ParseResult::Error(format!("invalid array count: {}", e)),
    };

    // Null array
    if count < 0 {
        return ParseResult::Complete(RespValue::Null, after_count);
    }

    let count = count as usize;
    let mut items = Vec::with_capacity(count);
    let mut offset = after_count;

    for _ in 0..count {
        match parse_resp(&buf[offset..]) {
            ParseResult::Complete(val, consumed) => {
                items.push(val);
                offset += consumed;
            }
            ParseResult::Incomplete => return ParseResult::Incomplete,
            ParseResult::Error(e) => return ParseResult::Error(e),
        }
    }

    ParseResult::Complete(RespValue::Array(items), offset)
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    fn b(s: &str) -> &[u8] {
        s.as_bytes()
    }

    #[test]
    fn test_parse_simple_string() {
        let input = b("+OK\r\n");
        match parse_resp(input) {
            ParseResult::Complete(RespValue::SimpleString(s), consumed) => {
                assert_eq!(s, "OK");
                assert_eq!(consumed, 5);
            }
            other => panic!("expected SimpleString, got {:?}", other),
        }
    }

    #[test]
    fn test_parse_error() {
        let input = b("-ERR test\r\n");
        match parse_resp(input) {
            ParseResult::Complete(RespValue::Error(s), consumed) => {
                assert_eq!(s, "ERR test");
                assert_eq!(consumed, 11);
            }
            other => panic!("expected Error, got {:?}", other),
        }
    }

    #[test]
    fn test_parse_integer() {
        let input = b(":42\r\n");
        match parse_resp(input) {
            ParseResult::Complete(RespValue::Integer(n), consumed) => {
                assert_eq!(n, 42);
                assert_eq!(consumed, 5);
            }
            other => panic!("expected Integer, got {:?}", other),
        }
    }

    #[test]
    fn test_parse_negative_integer() {
        let input = b(":-1\r\n");
        match parse_resp(input) {
            ParseResult::Complete(RespValue::Integer(n), _) => assert_eq!(n, -1),
            other => panic!("expected Integer(-1), got {:?}", other),
        }
    }

    #[test]
    fn test_parse_bulk_string() {
        let input = b("$5\r\nhello\r\n");
        match parse_resp(input) {
            ParseResult::Complete(RespValue::BulkString(data), consumed) => {
                assert_eq!(data, b"hello");
                assert_eq!(consumed, 11);
            }
            other => panic!("expected BulkString, got {:?}", other),
        }
    }

    #[test]
    fn test_parse_empty_bulk_string() {
        let input = b("$0\r\n\r\n");
        match parse_resp(input) {
            ParseResult::Complete(RespValue::BulkString(data), consumed) => {
                assert!(data.is_empty());
                assert_eq!(consumed, 6);
            }
            other => panic!("expected empty BulkString, got {:?}", other),
        }
    }

    #[test]
    fn test_parse_null_bulk() {
        let input = b("$-1\r\n");
        match parse_resp(input) {
            ParseResult::Complete(RespValue::Null, consumed) => {
                assert_eq!(consumed, 5);
            }
            other => panic!("expected Null, got {:?}", other),
        }
    }

    #[test]
    fn test_parse_null_array() {
        let input = b("*-1\r\n");
        match parse_resp(input) {
            ParseResult::Complete(RespValue::Null, consumed) => {
                assert_eq!(consumed, 5);
            }
            other => panic!("expected Null array, got {:?}", other),
        }
    }

    #[test]
    fn test_parse_empty_array() {
        let input = b("*0\r\n");
        match parse_resp(input) {
            ParseResult::Complete(RespValue::Array(items), consumed) => {
                assert!(items.is_empty());
                assert_eq!(consumed, 4);
            }
            other => panic!("expected empty Array, got {:?}", other),
        }
    }

    #[test]
    fn test_parse_get_command() {
        // *2\r\n$3\r\nGET\r\n$3\r\nfoo\r\n
        let input = b("*2\r\n$3\r\nGET\r\n$3\r\nfoo\r\n");
        match parse_resp(input) {
            ParseResult::Complete(RespValue::Array(items), _) => {
                assert_eq!(items.len(), 2);
                assert_eq!(
                    items[0],
                    RespValue::BulkString(b"GET".to_vec())
                );
                assert_eq!(
                    items[1],
                    RespValue::BulkString(b"foo".to_vec())
                );
            }
            other => panic!("expected Array, got {:?}", other),
        }
    }

    #[test]
    fn test_parse_set_command_with_ex() {
        // SET foo bar EX 60
        let input = b("*5\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\nbar\r\n$2\r\nEX\r\n$2\r\n60\r\n");
        match parse_resp(input) {
            ParseResult::Complete(RespValue::Array(items), _) => {
                assert_eq!(items.len(), 5);
                let strs: Vec<String> = items
                    .iter()
                    .map(|v| match v {
                        RespValue::BulkString(d) => String::from_utf8_lossy(d).to_string(),
                        _ => panic!("expected BulkString"),
                    })
                    .collect();
                assert_eq!(strs, vec!["SET", "foo", "bar", "EX", "60"]);
            }
            other => panic!("expected Array, got {:?}", other),
        }
    }

    #[test]
    fn test_parse_incomplete_simple_string() {
        // Missing \r\n
        let input = b("+OK");
        match parse_resp(input) {
            ParseResult::Incomplete => {}
            other => panic!("expected Incomplete, got {:?}", other),
        }
    }

    #[test]
    fn test_parse_incomplete_bulk_string() {
        // Header present but data incomplete
        let input = b("$5\r\nhel");
        match parse_resp(input) {
            ParseResult::Incomplete => {}
            other => panic!("expected Incomplete, got {:?}", other),
        }
    }

    #[test]
    fn test_parse_incomplete_array() {
        // Array header says 2 elements but only 1 present
        let input = b("*2\r\n$3\r\nGET\r\n");
        match parse_resp(input) {
            ParseResult::Incomplete => {}
            other => panic!("expected Incomplete, got {:?}", other),
        }
    }

    #[test]
    fn test_parse_empty_buffer() {
        let input = b("");
        match parse_resp(input) {
            ParseResult::Incomplete => {}
            other => panic!("expected Incomplete, got {:?}", other),
        }
    }

    #[test]
    fn test_parse_mixed_types_array() {
        // Array with a bulk string and an integer
        let input = b("*2\r\n$3\r\nfoo\r\n:100\r\n");
        match parse_resp(input) {
            ParseResult::Complete(RespValue::Array(items), _) => {
                assert_eq!(items.len(), 2);
                assert_eq!(items[0], RespValue::BulkString(b"foo".to_vec()));
                assert_eq!(items[1], RespValue::Integer(100));
            }
            other => panic!("expected Array, got {:?}", other),
        }
    }

    #[test]
    fn test_bytes_consumed_exact() {
        // Two values back-to-back; first should report exact consumed bytes
        let input = b("+OK\r\n+NO\r\n");
        match parse_resp(input) {
            ParseResult::Complete(RespValue::SimpleString(s), consumed) => {
                assert_eq!(s, "OK");
                assert_eq!(consumed, 5);
                // Parse the remainder
                match parse_resp(&input[consumed..]) {
                    ParseResult::Complete(RespValue::SimpleString(s2), _) => {
                        assert_eq!(s2, "NO");
                    }
                    other => panic!("expected second SimpleString, got {:?}", other),
                }
            }
            other => panic!("expected SimpleString, got {:?}", other),
        }
    }
}
