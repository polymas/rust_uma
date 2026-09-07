//! 下游 token 集合。来源优先级：console 下发 > 环境变量静态列表。
//! 两者都空（含启动后还没收到 console 下发）→ 拒绝所有握手。

use std::{collections::HashMap, sync::RwLock};

use crate::console::tokens::TokenGrant;

pub const STATIC_TOKEN_ID: &str = "env";

struct Inner {
    version: u64,
    /// secret → token id
    console: HashMap<String, String>,
    statics: Vec<String>,
    received_from_console: bool,
}

pub struct TokenSet {
    inner: RwLock<Inner>,
}

impl TokenSet {
    pub fn new(statics: Vec<String>) -> Self {
        Self {
            inner: RwLock::new(Inner {
                version: 0,
                console: HashMap::new(),
                statics,
                received_from_console: false,
            }),
        }
    }

    pub fn version(&self) -> u64 {
        self.read().version
    }

    /// Replaces the console set; returns true if anything changed.
    pub fn replace(&self, version: u64, grants: Vec<TokenGrant>) -> bool {
        let mut inner = self.inner.write().unwrap_or_else(|e| e.into_inner());
        let next: HashMap<String, String> = grants.into_iter().map(|g| (g.secret, g.id)).collect();
        let changed = next != inner.console || !inner.received_from_console;
        inner.version = version;
        inner.console = next;
        inner.received_from_console = true;
        changed
    }

    /// `Some(token_id)` when accepted. Console tokens win over the static
    /// list; the static list only matters while the console has never
    /// answered, so an outage does not lock everyone out.
    pub fn check(&self, secret: &str) -> Option<String> {
        if secret.is_empty() {
            return None;
        }
        let inner = self.read();
        if let Some(id) = inner.console.get(secret) {
            return Some(id.clone());
        }
        if !inner.received_from_console && inner.statics.iter().any(|s| s == secret) {
            return Some(STATIC_TOKEN_ID.to_owned());
        }
        None
    }

    pub fn is_live(&self, token_id: &str) -> bool {
        let inner = self.read();
        if token_id == STATIC_TOKEN_ID {
            return !inner.received_from_console;
        }
        inner.console.values().any(|id| id == token_id)
    }

    fn read(&self) -> std::sync::RwLockReadGuard<'_, Inner> {
        self.inner.read().unwrap_or_else(|e| e.into_inner())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn grant(id: &str, secret: &str) -> TokenGrant {
        TokenGrant {
            id: id.into(),
            secret: secret.into(),
        }
    }

    #[test]
    fn empty_set_rejects_everything() {
        let set = TokenSet::new(vec![]);
        assert_eq!(set.check("anything"), None);
        assert_eq!(set.check(""), None);
    }

    #[test]
    fn statics_only_until_console_answers() {
        let set = TokenSet::new(vec!["s1".into()]);
        assert_eq!(set.check("s1").as_deref(), Some("env"));
        assert!(set.is_live("env"));
        assert!(set.replace(3, vec![grant("a", "sa")]));
        assert_eq!(set.version(), 3);
        assert_eq!(set.check("sa").as_deref(), Some("a"));
        assert_eq!(set.check("s1"), None, "console set supersedes env list");
        assert!(!set.is_live("env"));
        assert!(
            !set.replace(3, vec![grant("a", "sa")]),
            "identical set is not a change"
        );
        assert!(
            set.replace(4, vec![]),
            "console says nobody: even an empty set is authoritative"
        );
        assert_eq!(set.check("sa"), None);
        assert!(!set.is_live("a"));
    }
}
