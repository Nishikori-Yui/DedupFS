use std::path::{Component, Path, PathBuf};

use anyhow::{bail, Context, Result};

pub fn normalize_library_name(raw_name: &str) -> Result<String> {
    let name = raw_name.trim();
    if name.is_empty() {
        bail!("library name cannot be empty");
    }
    if name == "." || name == ".." {
        bail!("library name cannot be traversal token");
    }
    if name.contains('/') || name.contains('\\') {
        bail!("library name must be direct child of /libraries");
    }
    if name.contains('~') || name.contains('$') {
        bail!("library name cannot contain shell expansion syntax");
    }
    Ok(name.to_string())
}

pub fn validate_relative_path(raw_path: &str) -> Result<PathBuf> {
    if raw_path.starts_with('/') {
        bail!("path must be relative");
    }
    if raw_path.contains('~') {
        bail!("home expansion is not allowed");
    }
    if raw_path.contains('$') {
        bail!("environment variable expansion is not allowed");
    }

    let path = Path::new(raw_path);
    for component in path.components() {
        match component {
            Component::Normal(_) | Component::CurDir => {}
            Component::ParentDir => bail!("path traversal is not allowed"),
            Component::RootDir | Component::Prefix(_) => bail!("path must remain relative"),
        }
    }

    Ok(path.to_path_buf())
}

pub fn to_posix_relative_path(path: &Path) -> Result<String> {
    let mut parts = Vec::new();
    for component in path.components() {
        match component {
            Component::Normal(value) => parts.push(value.to_string_lossy().to_string()),
            Component::CurDir => {}
            _ => bail!("relative path contains forbidden component"),
        }
    }

    if parts.is_empty() {
        bail!("empty relative path is not allowed");
    }

    Ok(parts.join("/"))
}

pub fn resolve_root_under_libraries(libraries_root_real: &Path, root: &Path) -> Result<PathBuf> {
    let root_real = root
        .canonicalize()
        .with_context(|| format!("failed to resolve library root: {}", root.display()))?;

    if !root_real.starts_with(libraries_root_real) {
        bail!("path escapes /libraries: {}", root_real.display());
    }

    Ok(root_real)
}

#[cfg(test)]
mod tests {
    use super::validate_relative_path;

    #[test]
    fn validate_relative_path_rejects_path_traversal() {
        assert!(validate_relative_path("../escape.bin").is_err());
        assert!(validate_relative_path("nested/../../escape.bin").is_err());
    }

    #[test]
    fn validate_relative_path_rejects_shell_expansion_tokens() {
        assert!(validate_relative_path("~/private.bin").is_err());
        assert!(validate_relative_path("$HOME/private.bin").is_err());
    }

    #[test]
    fn validate_relative_path_accepts_normal_relative_path() {
        assert!(validate_relative_path("media/photo.jpg").is_ok());
    }
}
