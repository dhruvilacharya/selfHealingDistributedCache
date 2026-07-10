use crate::cache::CacheStore;
use std::sync::Arc;
use std::time::Duration;
use tokio::time::interval;

/// Spawns a background task that periodically purges expired keys.
/// This is needed in addition to lazy expiry on read, so memory doesn't
/// balloon with unread expired keys.
pub fn start_expiry_sweeper(
    store: Arc<CacheStore>,
    sweep_interval: Duration,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let mut ticker = interval(sweep_interval);
        loop {
            ticker.tick().await;
            let now = std::time::Instant::now();
            store.data().retain(|_, entry| match entry.expires_at {
                Some(expires_at) => expires_at > now,
                None => true, // no expiry, keep
            });
        }
    })
}
