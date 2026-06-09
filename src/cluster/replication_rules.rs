use super::config::ClusterConfig;

/// Human: Decide whether a bucket/key should be enqueued for peer replication.
/// Agent: Empty include list = all keys; exclude wins; skips multipart staging keys.
pub fn should_replicate_key(cluster: &ClusterConfig, bucket: &str, key: &str) -> bool {
    if key.contains("/_multipart/") || key.starts_with(".multipart/") {
        return false;
    }
    let object_path = format!("{bucket}/{key}");
    for ex in &cluster.replication_exclude_prefixes {
        if key.starts_with(ex) || object_path.starts_with(ex) {
            return false;
        }
    }
    if cluster.replication_prefixes.is_empty() {
        return true;
    }
    cluster.replication_prefixes.iter().any(|p| {
        key.starts_with(p) || object_path.starts_with(p)
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cluster::config::{ClusterConfig, ClusterMode};

    fn cfg_with_rules(include: &[&str], exclude: &[&str]) -> ClusterConfig {
        let mut c = ClusterConfig::standalone();
        c.mode = ClusterMode::Replicated;
        c.replication_prefixes = include.iter().map(|s| (*s).to_string()).collect();
        c.replication_exclude_prefixes = exclude.iter().map(|s| (*s).to_string()).collect();
        c
    }

    #[test]
    fn empty_include_replicates_all() {
        let c = cfg_with_rules(&[], &[]);
        assert!(should_replicate_key(&c, "music", "song.mp3"));
    }

    #[test]
    fn include_prefix_filters() {
        let c = cfg_with_rules(&["users/"], &[]);
        assert!(should_replicate_key(&c, "media", "users/alice.bin"));
        assert!(!should_replicate_key(&c, "media", "public.bin"));
    }

    #[test]
    fn exclude_prefix_wins() {
        let c = cfg_with_rules(&[], &["tmp/"]);
        assert!(!should_replicate_key(&c, "b", "tmp/scratch"));
        assert!(should_replicate_key(&c, "b", "data.bin"));
    }
}
