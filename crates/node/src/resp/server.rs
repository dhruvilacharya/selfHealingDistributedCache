use std::sync::Arc;
use std::time::Duration;

use bytes::{Buf, BytesMut};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

use crate::cache::CacheStore;
use super::encoder::{encode_bulk_string, encode_error, encode_integer, encode_null, encode_simple_string};
use super::parser::{parse_resp, ParseResult, RespValue};

// ---------------------------------------------------------------------------
// Server
// ---------------------------------------------------------------------------

/// Start a RESP (Redis-compatible) TCP server alongside the cache node.
pub async fn start_resp_server(
    addr: &str,
    store: Arc<CacheStore>,
) -> Result<(), Box<dyn std::error::Error>> {
    let listener = TcpListener::bind(addr).await?;
    tracing::info!("RESP server listening on {}", addr);

    loop {
        let (stream, peer) = listener.accept().await?;
        let store = store.clone();
        tokio::spawn(async move {
            tracing::debug!("RESP connection from {}", peer);
            if let Err(e) = handle_connection(stream, store).await {
                tracing::warn!("RESP connection error from {}: {}", peer, e);
            }
            tracing::debug!("RESP connection closed: {}", peer);
        });
    }
}

// ---------------------------------------------------------------------------
// Connection handler
// ---------------------------------------------------------------------------

async fn handle_connection(
    mut stream: TcpStream,
    store: Arc<CacheStore>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let mut buf = BytesMut::with_capacity(4096);

    loop {
        let n = stream.read_buf(&mut buf).await?;
        if n == 0 {
            // Connection closed by client
            return Ok(());
        }

        // Process as many complete commands as possible from the buffer.
        loop {
            if buf.is_empty() {
                break;
            }

            match parse_resp(&buf) {
                ParseResult::Complete(value, consumed) => {
                    let response = handle_command(&value, &store);
                    stream.write_all(&response).await?;
                    buf.advance(consumed);
                }
                ParseResult::Incomplete => {
                    // Need more data — break out and read again.
                    break;
                }
                ParseResult::Error(e) => {
                    let resp = encode_error(&format!("ERR {}", e));
                    stream.write_all(&resp).await?;
                    // Clear the buffer on protocol error to avoid an infinite loop.
                    buf.clear();
                    break;
                }
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Command dispatch
// ---------------------------------------------------------------------------

fn handle_command(value: &RespValue, store: &CacheStore) -> Vec<u8> {
    let args = match value {
        RespValue::Array(items) => items,
        _ => return encode_error("ERR expected array (RESP command)"),
    };

    if args.is_empty() {
        return encode_error("ERR empty command");
    }

    // Extract command name (first element, case-insensitive)
    let cmd = match extract_str(&args[0]) {
        Some(s) => s.to_uppercase(),
        None => return encode_error("ERR invalid command"),
    };

    match cmd.as_str() {
        "PING" => cmd_ping(args),
        "ECHO" => cmd_echo(args),
        "SET" => cmd_set(args, store),
        "GET" => cmd_get(args, store),
        "DEL" => cmd_del(args, store),
        "EXISTS" => cmd_exists(args, store),
        "TTL" => cmd_ttl(args, store),
        "EXPIRE" => cmd_expire(args, store),
        _ => encode_error("ERR unknown command"),
    }
}

// ---------------------------------------------------------------------------
// Individual command implementations
// ---------------------------------------------------------------------------

fn cmd_ping(args: &[RespValue]) -> Vec<u8> {
    if args.len() == 1 {
        encode_simple_string("PONG")
    } else if args.len() == 2 {
        // PING <message> — echo back the message as bulk string
        match extract_bytes(&args[1]) {
            Some(data) => encode_bulk_string(data),
            None => encode_error("ERR invalid argument"),
        }
    } else {
        encode_error("ERR wrong number of arguments for 'ping' command")
    }
}

fn cmd_echo(args: &[RespValue]) -> Vec<u8> {
    if args.len() != 2 {
        return encode_error("ERR wrong number of arguments for 'echo' command");
    }
    match extract_bytes(&args[1]) {
        Some(data) => encode_bulk_string(data),
        None => encode_error("ERR invalid argument"),
    }
}

fn cmd_set(args: &[RespValue], store: &CacheStore) -> Vec<u8> {
    // SET key value [EX seconds | PX milliseconds]
    if args.len() < 3 {
        return encode_error("ERR wrong number of arguments for 'set' command");
    }

    let key = match extract_str(&args[1]) {
        Some(s) => s.to_string(),
        None => return encode_error("ERR invalid key"),
    };

    let value = match extract_bytes(&args[2]) {
        Some(data) => bytes::Bytes::copy_from_slice(data),
        None => return encode_error("ERR invalid value"),
    };

    // Parse optional EX/PX
    let mut ttl: Option<Duration> = None;
    let mut i = 3;
    while i < args.len() {
        let opt = match extract_str(&args[i]) {
            Some(s) => s.to_uppercase(),
            None => return encode_error("ERR invalid option"),
        };
        match opt.as_str() {
            "EX" => {
                if i + 1 >= args.len() {
                    return encode_error("ERR wrong number of arguments for 'set' command");
                }
                let secs = match extract_str(&args[i + 1]).and_then(|s| s.parse::<u64>().ok()) {
                    Some(n) => n,
                    None => return encode_error("ERR value is not an integer or out of range"),
                };
                ttl = Some(Duration::from_secs(secs));
                i += 2;
            }
            "PX" => {
                if i + 1 >= args.len() {
                    return encode_error("ERR wrong number of arguments for 'set' command");
                }
                let ms = match extract_str(&args[i + 1]).and_then(|s| s.parse::<u64>().ok()) {
                    Some(n) => n,
                    None => return encode_error("ERR value is not an integer or out of range"),
                };
                ttl = Some(Duration::from_millis(ms));
                i += 2;
            }
            _ => return encode_error("ERR syntax error"),
        }
    }

    store.set(key, value, ttl);
    encode_simple_string("OK")
}

fn cmd_get(args: &[RespValue], store: &CacheStore) -> Vec<u8> {
    if args.len() != 2 {
        return encode_error("ERR wrong number of arguments for 'get' command");
    }
    let key = match extract_str(&args[1]) {
        Some(s) => s,
        None => return encode_error("ERR invalid key"),
    };

    match store.get(key) {
        Some(val) => encode_bulk_string(&val),
        None => encode_null(),
    }
}

fn cmd_del(args: &[RespValue], store: &CacheStore) -> Vec<u8> {
    if args.len() < 2 {
        return encode_error("ERR wrong number of arguments for 'del' command");
    }
    let mut deleted = 0i64;
    for arg in &args[1..] {
        if let Some(key) = extract_str(arg) {
            if store.delete(key) {
                deleted += 1;
            }
        }
    }
    encode_integer(deleted)
}

fn cmd_exists(args: &[RespValue], store: &CacheStore) -> Vec<u8> {
    if args.len() != 2 {
        return encode_error("ERR wrong number of arguments for 'exists' command");
    }
    let key = match extract_str(&args[1]) {
        Some(s) => s,
        None => return encode_error("ERR invalid key"),
    };
    encode_integer(if store.exists(key) { 1 } else { 0 })
}

fn cmd_ttl(args: &[RespValue], store: &CacheStore) -> Vec<u8> {
    if args.len() != 2 {
        return encode_error("ERR wrong number of arguments for 'ttl' command");
    }
    let key = match extract_str(&args[1]) {
        Some(s) => s,
        None => return encode_error("ERR invalid key"),
    };

    if !store.exists(key) {
        // Key doesn't exist
        return encode_integer(-2);
    }

    match store.ttl(key) {
        Some(dur) => encode_integer(dur.as_secs() as i64),
        None => encode_integer(-1), // exists but no TTL
    }
}

fn cmd_expire(args: &[RespValue], store: &CacheStore) -> Vec<u8> {
    if args.len() != 3 {
        return encode_error("ERR wrong number of arguments for 'expire' command");
    }
    let key = match extract_str(&args[1]) {
        Some(s) => s,
        None => return encode_error("ERR invalid key"),
    };
    let secs = match extract_str(&args[2]).and_then(|s| s.parse::<u64>().ok()) {
        Some(n) => n,
        None => return encode_error("ERR value is not an integer or out of range"),
    };

    let ok = store.expire(key, Duration::from_secs(secs));
    encode_integer(if ok { 1 } else { 0 })
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Extract a UTF-8 string slice from a `RespValue` (BulkString or SimpleString).
fn extract_str(v: &RespValue) -> Option<&str> {
    match v {
        RespValue::BulkString(data) => std::str::from_utf8(data).ok(),
        RespValue::SimpleString(s) => Some(s.as_str()),
        _ => None,
    }
}

/// Extract raw bytes from a `RespValue`.
fn extract_bytes(v: &RespValue) -> Option<&[u8]> {
    match v {
        RespValue::BulkString(data) => Some(data),
        RespValue::SimpleString(s) => Some(s.as_bytes()),
        _ => None,
    }
}

// ---------------------------------------------------------------------------
// Integration-style tests for command dispatch
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    fn make_store() -> Arc<CacheStore> {
        Arc::new(CacheStore::new())
    }

    fn array(parts: &[&str]) -> RespValue {
        RespValue::Array(
            parts
                .iter()
                .map(|s| RespValue::BulkString(s.as_bytes().to_vec()))
                .collect(),
        )
    }

    #[test]
    fn test_ping() {
        let store = make_store();
        let cmd = array(&["PING"]);
        let resp = handle_command(&cmd, &store);
        assert_eq!(resp, b"+PONG\r\n");
    }

    #[test]
    fn test_ping_with_message() {
        let store = make_store();
        let cmd = array(&["PING", "hello"]);
        let resp = handle_command(&cmd, &store);
        assert_eq!(resp, b"$5\r\nhello\r\n");
    }

    #[test]
    fn test_set_and_get() {
        let store = make_store();
        let set_cmd = array(&["SET", "mykey", "myvalue"]);
        let resp = handle_command(&set_cmd, &store);
        assert_eq!(resp, b"+OK\r\n");

        let get_cmd = array(&["GET", "mykey"]);
        let resp = handle_command(&get_cmd, &store);
        assert_eq!(resp, b"$7\r\nmyvalue\r\n");
    }

    #[test]
    fn test_set_with_ex() {
        let store = make_store();
        let set_cmd = array(&["SET", "k", "v", "EX", "10"]);
        let resp = handle_command(&set_cmd, &store);
        assert_eq!(resp, b"+OK\r\n");

        // TTL should be roughly 10 seconds
        let ttl_cmd = array(&["TTL", "k"]);
        let resp = handle_command(&ttl_cmd, &store);
        // Should be :10\r\n or :9\r\n
        let resp_str = String::from_utf8_lossy(&resp);
        assert!(resp_str.starts_with(':'));
    }

    #[test]
    fn test_set_with_px() {
        let store = make_store();
        let set_cmd = array(&["SET", "k", "v", "PX", "5000"]);
        let resp = handle_command(&set_cmd, &store);
        assert_eq!(resp, b"+OK\r\n");
    }

    #[test]
    fn test_get_missing() {
        let store = make_store();
        let get_cmd = array(&["GET", "nokey"]);
        let resp = handle_command(&get_cmd, &store);
        assert_eq!(resp, b"$-1\r\n");
    }

    #[test]
    fn test_del_existing() {
        let store = make_store();
        handle_command(&array(&["SET", "k", "v"]), &store);
        let resp = handle_command(&array(&["DEL", "k"]), &store);
        assert_eq!(resp, b":1\r\n");
        assert_eq!(handle_command(&array(&["GET", "k"]), &store), b"$-1\r\n");
    }

    #[test]
    fn test_del_missing() {
        let store = make_store();
        let resp = handle_command(&array(&["DEL", "nokey"]), &store);
        assert_eq!(resp, b":0\r\n");
    }

    #[test]
    fn test_exists() {
        let store = make_store();
        handle_command(&array(&["SET", "k", "v"]), &store);
        assert_eq!(handle_command(&array(&["EXISTS", "k"]), &store), b":1\r\n");
        assert_eq!(
            handle_command(&array(&["EXISTS", "nokey"]), &store),
            b":0\r\n"
        );
    }

    #[test]
    fn test_ttl_no_key() {
        let store = make_store();
        assert_eq!(handle_command(&array(&["TTL", "nokey"]), &store), b":-2\r\n");
    }

    #[test]
    fn test_ttl_no_expiry() {
        let store = make_store();
        handle_command(&array(&["SET", "k", "v"]), &store);
        assert_eq!(handle_command(&array(&["TTL", "k"]), &store), b":-1\r\n");
    }

    #[test]
    fn test_expire() {
        let store = make_store();
        handle_command(&array(&["SET", "k", "v"]), &store);
        assert_eq!(
            handle_command(&array(&["EXPIRE", "k", "60"]), &store),
            b":1\r\n"
        );
        // TTL should now be ~60
        let resp = handle_command(&array(&["TTL", "k"]), &store);
        let resp_str = String::from_utf8_lossy(&resp);
        assert!(resp_str.starts_with(':'));
    }

    #[test]
    fn test_expire_missing_key() {
        let store = make_store();
        assert_eq!(
            handle_command(&array(&["EXPIRE", "nokey", "60"]), &store),
            b":0\r\n"
        );
    }

    #[test]
    fn test_unknown_command() {
        let store = make_store();
        let resp = handle_command(&array(&["FOOBAR"]), &store);
        assert_eq!(resp, b"-ERR unknown command\r\n");
    }

    #[test]
    fn test_case_insensitive() {
        let store = make_store();
        handle_command(&array(&["set", "k", "v"]), &store);
        let resp = handle_command(&array(&["get", "k"]), &store);
        assert_eq!(resp, b"$1\r\nv\r\n");
    }

    #[test]
    fn test_wrong_arg_count() {
        let store = make_store();
        let resp = handle_command(&array(&["SET", "only_key"]), &store);
        assert!(String::from_utf8_lossy(&resp).contains("wrong number of arguments"));
    }

    #[test]
    fn test_echo() {
        let store = make_store();
        let resp = handle_command(&array(&["ECHO", "hello"]), &store);
        assert_eq!(resp, b"$5\r\nhello\r\n");
    }
}
