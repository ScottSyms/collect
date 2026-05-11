use anyhow::{Context, Result};
use std::collections::HashSet;
use std::fs::{self, OpenOptions};
use std::io::{BufRead, BufReader, Write};
use std::path::{Path, PathBuf};

pub(crate) fn manifest_path(out_dir: &Path) -> PathBuf {
    out_dir.join(".collect-file-completed")
}

pub(crate) fn load_completed(path: &Path) -> Result<HashSet<String>> {
    let file = match fs::File::open(path) {
        Ok(file) => file,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(HashSet::new()),
        Err(error) => {
            return Err(error).with_context(|| format!("open manifest {}", path.display()))
        }
    };

    let reader = BufReader::new(file);
    let mut completed = HashSet::new();

    for line in reader.lines() {
        let line = line.with_context(|| format!("read manifest {}", path.display()))?;
        let line = line.trim();
        if !line.is_empty() {
            completed.insert(line.to_string());
        }
    }

    Ok(completed)
}

pub(crate) fn append_completed(path: &Path, key: &str) -> Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("create manifest dir {}", parent.display()))?;
    }

    let mut file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
        .with_context(|| format!("open manifest {}", path.display()))?;

    writeln!(file, "{}", key).with_context(|| format!("write manifest {}", path.display()))?;
    Ok(())
}
