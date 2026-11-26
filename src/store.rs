use crate::protocol::{Receipt, fingerprint};
use rusqlite::{Connection, OptionalExtension, TransactionBehavior, params};
use serde::Serialize;
use std::{path::Path, sync::Arc, time::Duration};

use crate::Result;
use tokio::sync::Mutex;

#[derive(Clone)]
pub struct Store(Arc<Mutex<Connection>>);

pub enum Admission {
    Accepted,
    Conflict,
    Full,
}

pub struct Pending {
    pub id: String,
    pub payload: String,
}

#[derive(Serialize)]
pub struct Counts {
    pub accepted: i64,
    pub pending: i64,
    pub delivered: i64,
}

impl Store {
    pub fn open(path: &Path, role: &str, gateway_id: &str) -> Result<Self> {
        if let Some(parent) = path
            .parent()
            .filter(|parent| !parent.as_os_str().is_empty())
        {
            std::fs::create_dir_all(parent)?;
        }

        let mut db = Connection::open(path)?;
        db.busy_timeout(Duration::from_secs(2))?;
        let mode: String = db.query_row("PRAGMA journal_mode = WAL", [], |row| row.get(0))?;
        if !mode.eq_ignore_ascii_case("wal") {
            return Err(format!("SQLite WAL unavailable at {}", path.display()).into());
        }
        db.pragma_update(None, "synchronous", "FULL")?;

        let tx = db.transaction_with_behavior(TransactionBehavior::Immediate)?;
        tx.execute_batch(
            "CREATE TABLE IF NOT EXISTS store_identity (
                slot INTEGER PRIMARY KEY CHECK (slot = 1),
                role TEXT NOT NULL,
                gateway_id TEXT NOT NULL
            );",
        )?;
        tx.execute(
            "INSERT INTO store_identity VALUES (1, ?1, ?2) ON CONFLICT(slot) DO NOTHING",
            params![role, gateway_id],
        )?;
        let identity: (String, String) =
            tx.query_row("SELECT role, gateway_id FROM store_identity", [], |row| {
                Ok((row.get(0)?, row.get(1)?))
            })?;
        if identity.0 != role || identity.1 != gateway_id {
            return Err(format!(
                "{} belongs to {} for gateway {}",
                path.display(),
                identity.0,
                identity.1
            )
            .into());
        }
        if role == "gateway" {
            tx.execute_batch(
                "CREATE TABLE IF NOT EXISTS outbox (
                    seq INTEGER PRIMARY KEY,
                    id TEXT NOT NULL UNIQUE,
                    fingerprint TEXT NOT NULL,
                    payload TEXT
                );
                CREATE INDEX IF NOT EXISTS outbox_pending ON outbox(seq)
                    WHERE payload IS NOT NULL;",
            )?;
        } else {
            tx.execute_batch(
                "CREATE TABLE IF NOT EXISTS inbox (
                    id TEXT PRIMARY KEY NOT NULL,
                    fingerprint TEXT NOT NULL,
                    payload TEXT NOT NULL,
                    attempts INTEGER NOT NULL DEFAULT 1
                );",
            )?;
        }
        tx.commit()?;
        Ok(Self(Arc::new(Mutex::new(db))))
    }

    async fn run<T: Send + 'static>(
        &self,
        operation: impl FnOnce(&mut Connection) -> rusqlite::Result<T> + Send + 'static,
    ) -> Result<T> {
        // Keep the SQLite lock through the operation even if the HTTP client disconnects.
        let mut connection = Arc::clone(&self.0).lock_owned().await;
        let result = tokio::task::spawn_blocking(move || operation(&mut connection)).await??;
        Ok(result)
    }

    pub async fn admit(&self, id: String, payload: String, limit: i64) -> Result<Admission> {
        let hash = fingerprint(&payload);
        self.run(move |db| {
            let tx = db.transaction_with_behavior(TransactionBehavior::Immediate)?;
            let existing: Option<String> = tx
                .query_row(
                    "SELECT fingerprint FROM outbox WHERE id = ?1",
                    [&id],
                    |row| row.get(0),
                )
                .optional()?;
            if let Some(existing) = existing {
                return Ok(if existing == hash {
                    Admission::Accepted
                } else {
                    Admission::Conflict
                });
            }

            let pending: i64 = tx.query_row(
                "SELECT COUNT(*) FROM outbox WHERE payload IS NOT NULL",
                [],
                |row| row.get(0),
            )?;
            if pending >= limit {
                return Ok(Admission::Full);
            }

            tx.execute(
                "INSERT INTO outbox (id, fingerprint, payload) VALUES (?1, ?2, ?3)",
                params![id, hash, payload],
            )?;
            tx.commit()?;
            Ok(Admission::Accepted)
        })
        .await
    }

    pub async fn pending(&self, limit: i64) -> Result<Vec<Pending>> {
        self.run(move |db| {
            let mut query = db.prepare_cached(
                "SELECT id, payload FROM outbox WHERE payload IS NOT NULL ORDER BY seq LIMIT ?1",
            )?;
            query
                .query_map([limit], |row| {
                    Ok(Pending {
                        id: row.get(0)?,
                        payload: row.get(1)?,
                    })
                })?
                .collect()
        })
        .await
    }

    pub async fn confirm(&self, receipt: Receipt) -> Result<bool> {
        self.run(move |db| {
            let changed = db.execute(
                "UPDATE outbox SET payload = NULL
                 WHERE id = ?1 AND fingerprint = ?2 AND payload IS NOT NULL",
                params![receipt.id, receipt.fingerprint],
            )?;
            Ok(changed == 1)
        })
        .await
    }

    pub async fn counts(&self) -> Result<Counts> {
        self.run(|db| {
            db.query_row(
                "SELECT (SELECT COUNT(*) FROM outbox),
                        (SELECT COUNT(*) FROM outbox WHERE payload IS NOT NULL)",
                [],
                |row| {
                    let accepted: i64 = row.get(0)?;
                    let pending: i64 = row.get(1)?;
                    Ok(Counts {
                        accepted,
                        pending,
                        delivered: accepted - pending,
                    })
                },
            )
        })
        .await
    }

    pub async fn record(&self, id: String, payload: String, hash: String) -> Result<bool> {
        self.run(move |db| {
            let changed = db.execute(
                "INSERT INTO inbox (id, fingerprint, payload) VALUES (?1, ?2, ?3)
                 ON CONFLICT(id) DO UPDATE SET attempts = attempts + 1
                 WHERE inbox.fingerprint = excluded.fingerprint",
                params![id, hash, payload],
            )?;
            Ok(changed == 1)
        })
        .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn capacity_receipts_and_idempotency_survive_reopening() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("gateway.db");
        let store = Store::open(&path, "gateway", "lab").unwrap();
        assert!(matches!(
            store.admit("a".into(), "one".into(), 1).await.unwrap(),
            Admission::Accepted
        ));
        assert!(matches!(
            store.admit("b".into(), "two".into(), 1).await.unwrap(),
            Admission::Full
        ));
        assert!(
            !store
                .confirm(Receipt {
                    id: "a".into(),
                    fingerprint: fingerprint("two")
                })
                .await
                .unwrap()
        );
        drop(store);

        let store = Store::open(&path, "gateway", "lab").unwrap();
        assert_eq!(store.pending(10).await.unwrap()[0].payload, "one");
        assert!(
            store
                .confirm(Receipt {
                    id: "a".into(),
                    fingerprint: fingerprint("one")
                })
                .await
                .unwrap()
        );
        assert!(matches!(
            store.admit("a".into(), "one".into(), 1).await.unwrap(),
            Admission::Accepted
        ));
        assert!(matches!(
            store.admit("a".into(), "two".into(), 1).await.unwrap(),
            Admission::Conflict
        ));
        assert!(matches!(
            store.admit("b".into(), "two".into(), 1).await.unwrap(),
            Admission::Accepted
        ));
        let counts = store.counts().await.unwrap();
        assert_eq!(
            (counts.accepted, counts.delivered, counts.pending),
            (2, 1, 1)
        );
        assert!(Store::open(&path, "gateway", "other").is_err());
    }
}
