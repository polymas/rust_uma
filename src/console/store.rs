//! 原子 JSON 落盘：写临时文件再 rename，进程中途被杀也不会留下半个文件。
//! 只在管理动作时调用，频率极低，同步 I/O 无所谓。

use std::{fs, io, path::Path};

use serde::{Serialize, de::DeserializeOwned};

pub fn save_json<T: Serialize>(path: &Path, value: &T) -> io::Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    let tmp = path.with_extension("json.tmp");
    let body = serde_json::to_vec_pretty(value).map_err(io::Error::other)?;
    fs::write(&tmp, body)?;
    fs::rename(&tmp, path)
}

/// Missing file → `Ok(None)`; a corrupt file is an error so we refuse to start
/// rather than silently forget every token.
pub fn load_json<T: DeserializeOwned>(path: &Path) -> io::Result<Option<T>> {
    match fs::read(path) {
        Ok(bytes) => serde_json::from_slice(&bytes)
            .map(Some)
            .map_err(io::Error::other),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(error),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn roundtrip_and_missing() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("x.json");
        assert!(load_json::<Vec<u32>>(&path).unwrap().is_none());
        save_json(&path, &vec![1u32, 2, 3]).unwrap();
        assert_eq!(load_json::<Vec<u32>>(&path).unwrap(), Some(vec![1, 2, 3]));
        assert!(!path.with_extension("json.tmp").exists());
    }
}
