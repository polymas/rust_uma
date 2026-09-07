//! 下游 wss token 表。控制台是唯一来源；edge 通过心跳响应拿到启用中的
//! `{id, secret}` 集合并本地校验。只有启用/禁用两个状态，没有过期。

use std::{io, path::PathBuf, sync::Mutex};

use rand::RngCore;
use serde::{Deserialize, Serialize};

use super::now_ms;
use super::store::{load_json, save_json};

#[derive(Clone, Serialize, Deserialize)]
pub struct Token {
    pub id: String,
    pub secret: String,
    pub name: String,
    pub enabled: bool,
    pub created_at_ms: u64,
    pub updated_at_ms: u64,
}

#[derive(Default, Serialize, Deserialize)]
struct TokensFile {
    version: u64,
    tokens: Vec<Token>,
}

/// What edges receive: only enabled tokens.
#[derive(Clone, Serialize, Deserialize, PartialEq, Eq, Debug)]
pub struct TokenGrant {
    pub id: String,
    pub secret: String,
}

/// Admin/panel view: never exposes the full secret after creation.
#[derive(Clone, Serialize)]
pub struct TokenView {
    pub id: String,
    pub name: String,
    pub prefix: String,
    pub enabled: bool,
    pub created_at_ms: u64,
    pub updated_at_ms: u64,
}

pub struct TokenStore {
    path: PathBuf,
    inner: Mutex<TokensFile>,
}

impl TokenStore {
    pub fn open(path: PathBuf) -> io::Result<Self> {
        let file = load_json::<TokensFile>(&path)?.unwrap_or_default();
        Ok(Self {
            path,
            inner: Mutex::new(file),
        })
    }

    pub fn version(&self) -> u64 {
        self.lock().version
    }

    pub fn grants(&self) -> (u64, Vec<TokenGrant>) {
        let inner = self.lock();
        let grants = inner
            .tokens
            .iter()
            .filter(|t| t.enabled)
            .map(|t| TokenGrant {
                id: t.id.clone(),
                secret: t.secret.clone(),
            })
            .collect();
        (inner.version, grants)
    }

    pub fn list(&self) -> Vec<TokenView> {
        self.lock().tokens.iter().map(view).collect()
    }

    /// Returns the created token with its full secret — the only time it is
    /// ever handed out.
    pub fn create(&self, name: &str) -> io::Result<Token> {
        let now = now_ms();
        let token = Token {
            id: random_hex(4),
            secret: random_hex(24),
            name: name.trim().to_owned(),
            enabled: true,
            created_at_ms: now,
            updated_at_ms: now,
        };
        self.mutate(|file| {
            file.tokens.push(token.clone());
            true
        })?;
        Ok(token)
    }

    pub fn set_enabled(&self, id: &str, enabled: bool) -> io::Result<bool> {
        self.mutate(|file| {
            let Some(token) = file.tokens.iter_mut().find(|t| t.id == id) else {
                return false;
            };
            token.enabled = enabled;
            token.updated_at_ms = now_ms();
            true
        })
    }

    pub fn rename(&self, id: &str, name: &str) -> io::Result<bool> {
        self.mutate(|file| {
            let Some(token) = file.tokens.iter_mut().find(|t| t.id == id) else {
                return false;
            };
            token.name = name.trim().to_owned();
            token.updated_at_ms = now_ms();
            true
        })
    }

    pub fn delete(&self, id: &str) -> io::Result<bool> {
        self.mutate(|file| {
            let before = file.tokens.len();
            file.tokens.retain(|t| t.id != id);
            file.tokens.len() != before
        })
    }

    /// Applies `f` under the lock; if it reports a change, bumps the version
    /// and persists. The lock is held across the (tiny) file write on purpose:
    /// two admin actions racing must not interleave their snapshots.
    fn mutate(&self, f: impl FnOnce(&mut TokensFile) -> bool) -> io::Result<bool> {
        let mut inner = self.lock();
        if !f(&mut inner) {
            return Ok(false);
        }
        inner.version += 1;
        save_json(&self.path, &*inner)?;
        Ok(true)
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, TokensFile> {
        self.inner.lock().unwrap_or_else(|e| e.into_inner())
    }
}

fn view(token: &Token) -> TokenView {
    TokenView {
        id: token.id.clone(),
        name: token.name.clone(),
        prefix: token.secret.chars().take(6).collect(),
        enabled: token.enabled,
        created_at_ms: token.created_at_ms,
        updated_at_ms: token.updated_at_ms,
    }
}

fn random_hex(bytes: usize) -> String {
    let mut buf = vec![0u8; bytes];
    rand::rng().fill_bytes(&mut buf);
    hex::encode(buf)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn lifecycle_persists_and_versions() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("tokens.json");
        let store = TokenStore::open(path.clone()).unwrap();
        assert_eq!(store.version(), 0);

        let a = store.create("alpha").unwrap();
        let b = store.create("beta").unwrap();
        assert_eq!(a.secret.len(), 48);
        assert_ne!(a.secret, b.secret);
        assert_eq!(store.version(), 2);
        assert_eq!(store.grants().1.len(), 2);

        assert!(store.set_enabled(&a.id, false).unwrap());
        let (version, grants) = store.grants();
        assert_eq!(version, 3);
        assert_eq!(grants.len(), 1);
        assert_eq!(grants[0].id, b.id);

        assert!(!store.set_enabled("nope", true).unwrap());
        assert_eq!(store.version(), 3, "no-op must not bump the version");

        let reopened = TokenStore::open(path).unwrap();
        assert_eq!(reopened.version(), 3);
        let list = reopened.list();
        assert_eq!(list.len(), 2);
        assert!(list.iter().all(|t| t.prefix.len() == 6));
        assert!(reopened.delete(&b.id).unwrap());
        assert!(reopened.grants().1.is_empty());
    }
}
