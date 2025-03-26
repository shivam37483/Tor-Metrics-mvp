use sha2::{Digest, Sha256};

/// Computes a digest for a file using its raw content.
///
/// Following the maintainer's recommendation and the original implementation,
/// this calculates a SHA-256 hash of the entire raw file content.
///
/// # Arguments
///
/// * `raw_content` - The raw bytes of the file content.
///
/// # Returns
///
/// A hexadecimal string representation of the SHA-256 digest.
pub fn compute_file_digest(raw_content: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(raw_content);
    let result = hasher.finalize();
    hex::encode(result)
}

/// Computes a digest for an individual assignment using its raw line bytes and file digest.
///
/// Following the maintainer's recommendation and the original implementation,
/// this calculates a SHA-256 hash of the raw line bytes combined with the file digest
/// to ensure uniqueness across files.
///
/// # Arguments
///
/// * `raw_line` - The raw bytes of the assignment line.
/// * `file_digest` - The digest of the file this assignment belongs to.
///
/// # Returns
///
/// A hexadecimal string representation of the SHA-256 digest.
pub fn compute_assignment_digest(raw_line: &[u8], file_digest: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(raw_line);
    hasher.update(file_digest.as_bytes()); // Include file digest to ensure uniqueness
    let result = hasher.finalize();
    hex::encode(result)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_compute_file_digest() {
        let content = b"bridge-pool-assignment 2022-04-09 00:29:37";
        let digest = compute_file_digest(content);
        assert!(!digest.is_empty());
        assert_eq!(digest.len(), 64); // SHA-256 produces a 32-byte (64 hex char) digest
    }

    #[test]
    fn test_compute_assignment_digest() {
        let line = b"005fd4d7decbb250055b861579e6fdc79ad17bee email transport=obfs4";
        let file_digest = "abcdef1234567890abcdef1234567890abcdef1234567890abcdef1234567890";
        let digest = compute_assignment_digest(line, file_digest);
        assert!(!digest.is_empty());
        assert_eq!(digest.len(), 64);
    }

    #[test]
    fn test_assignment_digests_are_unique_with_same_line() {
        let line = b"005fd4d7decbb250055b861579e6fdc79ad17bee email transport=obfs4";
        let file_digest1 = "abcdef1234567890abcdef1234567890abcdef1234567890abcdef1234567890";
        let file_digest2 = "fedcba0987654321fedcba0987654321fedcba0987654321fedcba0987654321";
        
        let digest1 = compute_assignment_digest(line, file_digest1);
        let digest2 = compute_assignment_digest(line, file_digest2);
        
        // Same line but different file digests should produce different assignment digests
        assert_ne!(digest1, digest2);
    }
} 