pub type HashMap<K, V> = std::collections::HashMap<K, V, foldhash::quality::RandomState>;
pub type HashSet<K> = std::collections::HashSet<K, foldhash::quality::RandomState>;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VecSide {
    Back,
    Front,
}

/// Constant-time string comparison for auth tokens
pub fn tokens_match(a: &str, b: &str) -> bool {
    a.len() == b.len()
        && a.bytes()
            .zip(b.bytes())
            .fold(0u8, |acc, (x, y)| acc | (x ^ y))
            == 0
}
