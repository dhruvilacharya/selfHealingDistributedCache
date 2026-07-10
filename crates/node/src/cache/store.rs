use bytes::Bytes;
use dashmap::DashMap;
use std::time::{Duration, Instant};

#[derive(Clone)]
pub struct CacheEntry {
    pub value: Bytes,
    pub expires_at: Option<Instant>,
}

pub struct CacheStore {
    data: DashMap<String, CacheEntry>,
    /// Reference epoch used for converting Instant <-> epoch milliseconds.
    /// Set once at store creation.
    epoch: Instant,
}

impl CacheStore {
    pub fn new() -> Self {
        Self {
            data: DashMap::new(),
            epoch: Instant::now(),
        }
    }

    /// Convert an Instant to milliseconds since this store's epoch.
    pub fn instant_to_epoch_ms(&self, instant: Instant) -> u64 {
        instant
            .checked_duration_since(self.epoch)
            .map(|d| d.as_millis() as u64)
            .unwrap_or(0)
    }

    /// Convert milliseconds since this store's epoch back to an Instant.
    pub fn epoch_ms_to_instant(&self, ms: u64) -> Instant {
        self.epoch + Duration::from_millis(ms)
    }

    /// Get the store's epoch Instant (used by rebalancer for TTL conversion).
    pub fn epoch(&self) -> Instant {
        self.epoch
    }

    pub fn set(&self, key: String, value: Bytes, ttl: Option<Duration>) {
        let expires_at = ttl.map(|d| Instant::now() + d);
        self.data.insert(key, CacheEntry { value, expires_at });
    }

    /// Returns None if key doesn't exist OR is expired (lazy expiry on read).
    pub fn get(&self, key: &str) -> Option<Bytes> {
        let entry = self.data.get(key)?;
        if let Some(expires_at) = entry.expires_at {
            if expires_at <= Instant::now() {
                // Expired — drop the ref before removing
                drop(entry);
                self.data.remove(key);
                return None;
            }
        }
        Some(entry.value.clone())
    }

    pub fn delete(&self, key: &str) -> bool {
        self.data.remove(key).is_some()
    }

    /// Returns the number of non-expired keys (approximate).
    pub fn len(&self) -> usize {
        let now = Instant::now();
        self.data
            .iter()
            .filter(|entry| match entry.value().expires_at {
                Some(expires_at) => expires_at > now,
                None => true,
            })
            .count()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Returns remaining TTL for a key, or None if no TTL or key doesn't exist.
    /// Returns Some(Duration::ZERO) if expired but not yet cleaned.
    pub fn ttl(&self, key: &str) -> Option<Duration> {
        let entry = self.data.get(key)?;
        match entry.expires_at {
            Some(expires_at) => {
                let now = Instant::now();
                if expires_at > now {
                    Some(expires_at - now)
                } else {
                    Some(Duration::ZERO)
                }
            }
            None => None,
        }
    }

    /// Check if a key exists (not expired).
    pub fn exists(&self, key: &str) -> bool {
        self.get(key).is_some()
    }

    /// Set expiry on an existing key. Returns false if key doesn't exist.
    pub fn expire(&self, key: &str, ttl: Duration) -> bool {
        if let Some(mut entry) = self.data.get_mut(key) {
            entry.expires_at = Some(Instant::now() + ttl);
            true
        } else {
            false
        }
    }

    /// Provide access to the underlying DashMap for expiry sweeper.
    pub fn data(&self) -> &DashMap<String, CacheEntry> {
        &self.data
    }

    /// Get all non-expired entries as (key, value_bytes, epoch_ms_expiry_option).
    /// Used by the TransferKeys RPC to stream data to a joining node.
    /// Expired entries are skipped.
    pub fn entries_for_transfer(&self) -> Vec<(String, Vec<u8>, Option<u64>)> {
        let now = Instant::now();
        let mut result = Vec::new();
        for entry in self.data.iter() {
            let cache_entry = entry.value();
            // Skip expired keys
            if let Some(expires_at) = cache_entry.expires_at {
                if expires_at <= now {
                    continue;
                }
            }
            let expires_at_ms = cache_entry.expires_at.map(|inst| {
                // Send remaining TTL in ms from now
                inst.saturating_duration_since(now).as_millis() as u64
            });
            result.push((
                entry.key().clone(),
                cache_entry.value.to_vec(),
                expires_at_ms,
            ));
        }
        result
    }

    /// Insert a key with a remaining TTL in milliseconds (from a transfer).
    /// If `remaining_ms` is Some(0) or already expired, skip the insert.
    pub fn set_from_transfer(&self, key: String, value: Bytes, remaining_ms: Option<u64>) {
        let ttl = match remaining_ms {
            Some(0) => return, // Already expired, skip
            Some(ms) => Some(Duration::from_millis(ms)),
            None => None,
        };
        self.set(key, value, ttl);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_set_get_roundtrip() {
        let store = CacheStore::new();
        store.set("hello".into(), Bytes::from("world"), None);
        let val = store.get("hello").unwrap();
        assert_eq!(val, Bytes::from("world"));
    }

    #[test]
    fn test_get_nonexistent() {
        let store = CacheStore::new();
        assert!(store.get("missing").is_none());
    }

    #[test]
    fn test_delete_removes_key() {
        let store = CacheStore::new();
        store.set("key".into(), Bytes::from("val"), None);
        assert!(store.delete("key"));
        assert!(store.get("key").is_none());
    }

    #[test]
    fn test_ttl_expiry() {
        let store = CacheStore::new();
        store.set("ttl_key".into(), Bytes::from("ttl_val"), Some(Duration::from_millis(50)));
        // Immediate read should succeed
        assert!(store.get("ttl_key").is_some());
        // Wait for expiry
        std::thread::sleep(Duration::from_millis(100));
        assert!(store.get("ttl_key").is_none());
    }

    #[test]
    fn test_no_ttl() {
        let store = CacheStore::new();
        store.set("persist".into(), Bytes::from("forever"), None);
        std::thread::sleep(Duration::from_millis(50));
        assert!(store.get("persist").is_some());
    }

    #[test]
    fn test_overwrite() {
        let store = CacheStore::new();
        store.set("key".into(), Bytes::from("v1"), None);
        store.set("key".into(), Bytes::from("v2"), None);
        assert_eq!(store.get("key").unwrap(), Bytes::from("v2"));
    }

    #[test]
    fn test_exists() {
        let store = CacheStore::new();
        store.set("e".into(), Bytes::from("x"), None);
        assert!(store.exists("e"));
        store.delete("e");
        assert!(!store.exists("e"));
    }

    #[test]
    fn test_expire() {
        let store = CacheStore::new();
        store.set("exp".into(), Bytes::from("val"), None);
        assert!(store.expire("exp", Duration::from_millis(50)));
        // Should exist immediately
        assert!(store.exists("exp"));
        std::thread::sleep(Duration::from_millis(100));
        assert!(!store.exists("exp"));
    }

    #[test]
    fn test_ttl_remaining() {
        let store = CacheStore::new();
        store.set("ttl_rem".into(), Bytes::from("val"), Some(Duration::from_secs(1)));
        let remaining = store.ttl("ttl_rem").unwrap();
        // Should be roughly 1s, allow some slack
        assert!(remaining > Duration::from_millis(900));
        assert!(remaining <= Duration::from_secs(1));
    }

    #[test]
    fn test_entries_for_transfer() {
        let store = CacheStore::new();
        store.set("no_ttl".into(), Bytes::from("v1"), None);
        store.set("with_ttl".into(), Bytes::from("v2"), Some(Duration::from_secs(60)));
        store.set("expired".into(), Bytes::from("v3"), Some(Duration::from_millis(1)));
        std::thread::sleep(Duration::from_millis(10));

        let entries = store.entries_for_transfer();
        // "expired" should be skipped
        assert_eq!(entries.len(), 2);
        let keys: Vec<&str> = entries.iter().map(|(k, _, _)| k.as_str()).collect();
        assert!(keys.contains(&"no_ttl"));
        assert!(keys.contains(&"with_ttl"));
        assert!(!keys.contains(&"expired"));

        // Check TTL values
        for (key, _, expires_at_ms) in &entries {
            if key == "no_ttl" {
                assert!(expires_at_ms.is_none());
            } else if key == "with_ttl" {
                // Should be roughly 60000ms remaining
                let ms = expires_at_ms.unwrap();
                assert!(ms > 59_000 && ms <= 60_000);
            }
        }
    }

    #[test]
    fn test_set_from_transfer_with_ttl() {
        let store = CacheStore::new();
        store.set_from_transfer("transferred".into(), Bytes::from("data"), Some(5000));
        let val = store.get("transferred").unwrap();
        assert_eq!(val, Bytes::from("data"));
        let remaining = store.ttl("transferred").unwrap();
        assert!(remaining > Duration::from_millis(4000));
        assert!(remaining <= Duration::from_millis(5000));
    }

    #[test]
    fn test_set_from_transfer_no_ttl() {
        let store = CacheStore::new();
        store.set_from_transfer("permanent".into(), Bytes::from("data"), None);
        assert!(store.get("permanent").is_some());
        assert!(store.ttl("permanent").is_none());
    }

    #[test]
    fn test_set_from_transfer_expired_skipped() {
        let store = CacheStore::new();
        store.set_from_transfer("already_dead".into(), Bytes::from("data"), Some(0));
        // Should be skipped (already expired)
        assert!(store.get("already_dead").is_none());
    }

    #[test]
    fn test_epoch_conversion() {
        let store = CacheStore::new();
        let now = Instant::now();
        let ms = store.instant_to_epoch_ms(now);
        // now is after epoch, so ms should be >= 0
        let back = store.epoch_ms_to_instant(ms);
        // Should round-trip approximately
        let diff = back.saturating_duration_since(now);
        assert!(diff < Duration::from_millis(10));
    }
}
