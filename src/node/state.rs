use std::path::Path;

use anyhow::{Context, Result};
use sqlx::sqlite::{SqliteConnectOptions, SqlitePoolOptions};
use sqlx::{FromRow, Row, SqlitePool};

#[derive(Debug, Clone, FromRow)]
pub struct CachedContact {
    pub public_key: Vec<u8>,
    pub name: String,
    pub contact_type: i64,
    pub last_advert: Option<i64>,
    pub lat: Option<f64>,
    pub lon: Option<f64>,
}

#[derive(Debug, Clone, FromRow)]
pub struct CachedChannel {
    pub idx: i64,
    pub name: String,
    pub config: Option<Vec<u8>>,
}

pub struct NodeState {
    pool: SqlitePool,
}

impl NodeState {
    pub async fn open<P: AsRef<Path>>(path: P) -> Result<Self> {
        let path = path.as_ref();

        let options = SqliteConnectOptions::new()
            .filename(path)
            .create_if_missing(true);

        // Single connection on purpose: the Core actor serializes all DB
        // access (one task owns &NodeState through Arc and only one task
        // ever calls into it at a time), so concurrent connections would
        // sit idle. Using the pool API instead of a bare SqliteConnection
        // keeps the ergonomic sqlx::query(&self.pool) call sites.
        let pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect_with(options)
            .await
            .context("failed to open SQLite database")?;

        let state = Self { pool };

        state.migrate().await?;
        Ok(state)
    }

    async fn migrate(&self) -> Result<()> {
        sqlx::query(
            "CREATE TABLE IF NOT EXISTS contacts (
                public_key BLOB PRIMARY KEY,
                name TEXT NOT NULL,
                contact_type INTEGER NOT NULL,
                last_advert INTEGER,
                lat REAL,
                lon REAL
            )",
        )
        .execute(&self.pool)
        .await?;

        sqlx::query(
            "CREATE TABLE IF NOT EXISTS channels (
                idx INTEGER PRIMARY KEY,
                name TEXT NOT NULL,
                config BLOB
            )",
        )
        .execute(&self.pool)
        .await?;

        sqlx::query(
            "CREATE TABLE IF NOT EXISTS messages (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                msg_type TEXT NOT NULL,
                from_key BLOB,
                channel_idx INTEGER,
                text TEXT NOT NULL,
                timestamp INTEGER NOT NULL
            )",
        )
        .execute(&self.pool)
        .await?;

        sqlx::query(
            "CREATE TABLE IF NOT EXISTS kv_store (
                key TEXT PRIMARY KEY,
                value BLOB NOT NULL
            )",
        )
        .execute(&self.pool)
        .await?;

        sqlx::query(
            "CREATE TABLE IF NOT EXISTS raw_rx (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ts INTEGER NOT NULL,
                code INTEGER NOT NULL,
                payload BLOB NOT NULL
            )",
        )
        .execute(&self.pool)
        .await?;

        // Drop obsolete kv_store entries from prior versions. self_info/
        // device_info/battery_info used to be JSON; identity.* was a SHA256
        // pseudo-keypair that no longer exists.
        sqlx::query(
            "DELETE FROM kv_store WHERE key IN \
             ('self_info', 'device_info', 'battery_info', \
              'identity.seed', 'identity.pk')",
        )
        .execute(&self.pool)
        .await?;

        Ok(())
    }

    // --- Contacts ---

    pub async fn upsert_contact(&self, contact: &CachedContact) -> Result<()> {
        sqlx::query(
            "INSERT OR REPLACE INTO contacts (public_key, name, contact_type, last_advert, lat, lon)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
        )
        .bind(&contact.public_key)
        .bind(&contact.name)
        .bind(contact.contact_type)
        .bind(contact.last_advert)
        .bind(contact.lat)
        .bind(contact.lon)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn get_contacts(&self) -> Result<Vec<CachedContact>> {
        let rows = sqlx::query_as::<_, CachedContact>("SELECT * FROM contacts ORDER BY name")
            .fetch_all(&self.pool)
            .await?;
        Ok(rows)
    }

    // --- Channels ---

    pub async fn upsert_channel(&self, channel: &CachedChannel) -> Result<()> {
        sqlx::query("INSERT OR REPLACE INTO channels (idx, name, config) VALUES (?1, ?2, ?3)")
            .bind(channel.idx)
            .bind(&channel.name)
            .bind(&channel.config)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    pub async fn delete_channel(&self, idx: i64) -> Result<()> {
        sqlx::query("DELETE FROM channels WHERE idx = ?1")
            .bind(idx)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    pub async fn get_channels(&self) -> Result<Vec<CachedChannel>> {
        let rows = sqlx::query_as::<_, CachedChannel>("SELECT * FROM channels ORDER BY idx")
            .fetch_all(&self.pool)
            .await?;
        Ok(rows)
    }

    // --- Messages ---

    pub async fn insert_message(
        &self,
        msg_type: &str,
        from_key: Option<&[u8]>,
        channel_idx: Option<i64>,
        text: &str,
        timestamp: i64,
    ) -> Result<i64> {
        let result = sqlx::query(
            "INSERT INTO messages (msg_type, from_key, channel_idx, text, timestamp)
             VALUES (?1, ?2, ?3, ?4, ?5)",
        )
        .bind(msg_type)
        .bind(from_key)
        .bind(channel_idx)
        .bind(text)
        .bind(timestamp)
        .execute(&self.pool)
        .await?;
        Ok(result.last_insert_rowid())
    }

    // --- Key-value store ---

    pub async fn kv_set(&self, key: &str, value: &[u8]) -> Result<()> {
        sqlx::query("INSERT OR REPLACE INTO kv_store (key, value) VALUES (?1, ?2)")
            .bind(key)
            .bind(value)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    pub async fn kv_get(&self, key: &str) -> Result<Option<Vec<u8>>> {
        let row = sqlx::query("SELECT value FROM kv_store WHERE key = ?1")
            .bind(key)
            .fetch_optional(&self.pool)
            .await?;
        Ok(row.map(|r| r.get(0)))
    }

    // --- Raw RX recording ---

    pub async fn insert_raw_rx(&self, code: u8, payload: &[u8]) -> Result<()> {
        let ts = chrono::Utc::now().timestamp();
        sqlx::query("INSERT INTO raw_rx (ts, code, payload) VALUES (?1, ?2, ?3)")
            .bind(ts)
            .bind(code as i64)
            .bind(payload)
            .execute(&self.pool)
            .await?;
        Ok(())
    }
}
