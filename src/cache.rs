//! Team code mapping cache for cross-platform market matching.
//!
//! This module provides bidirectional mapping between Polymarket and Kalshi
//! team codes to enable accurate market discovery across platforms.

use anyhow::Result;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::Path;

const CACHE_FILE: &str = "kalshi_team_cache.json";

/// Team cache format enum for backward compatibility
#[derive(Debug, Clone, Deserialize)]
#[serde(untagged)]
enum TeamCacheFormat {
    /// V2: Current format with explicit forward/reverse (reverse is rebuilt)
    V2 {
        #[serde(deserialize_with = "deserialize_boxed_map")]
        forward: HashMap<Box<str>, Box<str>>,
    },
    /// V1: Legacy format with top-level "mappings" field
    V1 {
        #[serde(deserialize_with = "deserialize_boxed_map")]
        mappings: HashMap<Box<str>, Box<str>>,
    },
}

/// Team code cache - bidirectional mapping between Poly and Kalshi team codes
#[derive(Debug, Clone, Serialize, Default)]
pub struct TeamCache {
    /// Forward: "league:poly_code" -> "kalshi_code"
    #[serde(serialize_with = "serialize_boxed_map")]
    forward: HashMap<Box<str>, Box<str>>,
    /// Reverse: "league:kalshi_code" -> "poly_code"
    #[serde(skip)]
    reverse: HashMap<Box<str>, Box<str>>,
}

fn serialize_boxed_map<S>(
    map: &HashMap<Box<str>, Box<str>>,
    serializer: S,
) -> Result<S::Ok, S::Error>
where
    S: serde::Serializer,
{
    use serde::ser::SerializeMap;
    let mut ser_map = serializer.serialize_map(Some(map.len()))?;
    for (k, v) in map {
        ser_map.serialize_entry(k.as_ref(), v.as_ref())?;
    }
    ser_map.end()
}

fn deserialize_boxed_map<'de, D>(deserializer: D) -> Result<HashMap<Box<str>, Box<str>>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let string_map: HashMap<String, String> = HashMap::deserialize(deserializer)?;
    Ok(string_map
        .into_iter()
        .map(|(k, v)| (k.into_boxed_str(), v.into_boxed_str()))
        .collect())
}

#[allow(dead_code)]
impl TeamCache {
    /// Load cache from JSON file
    pub fn load() -> Self {
        Self::load_from(CACHE_FILE)
    }

    /// Load from specific path
    pub fn load_from<P: AsRef<Path>>(path: P) -> Self {
        let mut cache = match std::fs::read_to_string(path.as_ref()) {
            Ok(contents) => {
                // Try to parse as either V2 or V1 format
                match serde_json::from_str::<TeamCacheFormat>(&contents) {
                    Ok(TeamCacheFormat::V2 { forward }) => {
                        tracing::debug!("Loaded team cache (v2 format)");
                        Self {
                            forward,
                            reverse: HashMap::new(),
                        }
                    }
                    Ok(TeamCacheFormat::V1 { mappings }) => {
                        tracing::debug!("Loaded team cache (v1 legacy format), migrating to v2");
                        Self {
                            forward: mappings,
                            reverse: HashMap::new(),
                        }
                    }
                    Err(e) => {
                        tracing::warn!("Failed to parse team cache: {}", e);
                        Self::default()
                    }
                }
            }
            Err(_) => {
                tracing::info!("No team cache found at {:?}, starting empty", path.as_ref());
                Self::default()
            }
        };
        cache.rebuild_reverse();
        cache
    }

    /// Save cache to JSON file
    pub fn save(&self) -> Result<()> {
        self.save_to(CACHE_FILE)
    }

    /// Save to specific path
    pub fn save_to<P: AsRef<Path>>(&self, path: P) -> Result<()> {
        let json = serde_json::to_string_pretty(&self)?;
        std::fs::write(path, json)?;
        Ok(())
    }

    /// Get Kalshi code for a Polymarket team code
    /// e.g., ("epl", "che") -> "cfc"
    pub fn poly_to_kalshi(&self, league: &str, poly_code: &str) -> Option<String> {
        let mut key_buf = String::with_capacity(league.len() + 1 + poly_code.len());
        key_buf.push_str(&league.to_ascii_lowercase());
        key_buf.push(':');
        key_buf.push_str(&poly_code.to_ascii_lowercase());
        self.forward.get(key_buf.as_str()).map(|s| s.to_string())
    }

    /// Get Polymarket code for a Kalshi team code (reverse lookup)
    /// e.g., ("epl", "cfc") -> "che"
    pub fn kalshi_to_poly(&self, league: &str, kalshi_code: &str) -> Option<String> {
        let mut key_buf = String::with_capacity(league.len() + 1 + kalshi_code.len());
        key_buf.push_str(&league.to_ascii_lowercase());
        key_buf.push(':');
        key_buf.push_str(&kalshi_code.to_ascii_lowercase());

        self.reverse
            .get(key_buf.as_str())
            .map(|s| s.to_string())
            .or_else(|| Some(kalshi_code.to_ascii_lowercase()))
    }

    /// Add or update a mapping
    pub fn insert(&mut self, league: &str, poly_code: &str, kalshi_code: &str) {
        let league_lower = league.to_ascii_lowercase();
        let poly_lower = poly_code.to_ascii_lowercase();
        let kalshi_lower = kalshi_code.to_ascii_lowercase();

        let forward_key: Box<str> = format!("{}:{}", league_lower, poly_lower).into();
        let reverse_key: Box<str> = format!("{}:{}", league_lower, kalshi_lower).into();

        self.forward.insert(forward_key, kalshi_lower.into());
        self.reverse.insert(reverse_key, poly_lower.into());
    }

    /// Number of mappings
    pub fn len(&self) -> usize {
        self.forward.len()
    }

    pub fn is_empty(&self) -> bool {
        self.forward.is_empty()
    }

    /// Rebuild reverse lookup map from forward mappings
    fn rebuild_reverse(&mut self) {
        self.reverse.clear();
        self.reverse.reserve(self.forward.len());
        for (key, kalshi_code) in &self.forward {
            if let Some((league, poly)) = key.split_once(':') {
                let reverse_key: Box<str> = format!("{}:{}", league, kalshi_code).into();
                self.reverse.insert(reverse_key, poly.into());
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_cache_lookup() {
        let mut cache = TeamCache::default();
        cache.insert("epl", "che", "cfc");
        cache.insert("epl", "mun", "mun");

        assert_eq!(cache.poly_to_kalshi("epl", "che"), Some("cfc".to_string()));
        assert_eq!(cache.poly_to_kalshi("epl", "CHE"), Some("cfc".to_string()));
        assert_eq!(cache.kalshi_to_poly("epl", "cfc"), Some("che".to_string()));
    }

    #[test]
    fn test_load_v2_format() {
        // V2 format: {"forward": {...}}
        let v2_json = r#"{
            "forward": {
                "epl:che": "cfc",
                "epl:mun": "mun",
                "nba:lal": "lal"
            }
        }"#;

        let test_file = ".test_cache_v2.json";
        std::fs::write(test_file, v2_json).unwrap();

        let cache = TeamCache::load_from(test_file);

        // Clean up
        let _ = std::fs::remove_file(test_file);

        assert_eq!(cache.len(), 3);
        assert_eq!(cache.poly_to_kalshi("epl", "che"), Some("cfc".to_string()));
        assert_eq!(cache.poly_to_kalshi("epl", "mun"), Some("mun".to_string()));
        assert_eq!(cache.poly_to_kalshi("nba", "lal"), Some("lal".to_string()));

        // Verify reverse lookup works (should be rebuilt)
        assert_eq!(cache.kalshi_to_poly("epl", "cfc"), Some("che".to_string()));
    }

    #[test]
    fn test_load_v1_legacy_format() {
        // V1 legacy format: {"mappings": {...}}
        let v1_json = r#"{
            "mappings": {
                "epl:che": "cfc",
                "epl:ars": "ars",
                "bun:bay": "bmu"
            }
        }"#;

        let test_file = ".test_cache_v1.json";
        std::fs::write(test_file, v1_json).unwrap();

        let cache = TeamCache::load_from(test_file);

        // Clean up
        let _ = std::fs::remove_file(test_file);

        // Should load legacy format and migrate to v2
        assert_eq!(cache.len(), 3);
        assert_eq!(cache.poly_to_kalshi("epl", "che"), Some("cfc".to_string()));
        assert_eq!(cache.poly_to_kalshi("epl", "ars"), Some("ars".to_string()));
        assert_eq!(cache.poly_to_kalshi("bun", "bay"), Some("bmu".to_string()));

        // Verify reverse lookup works (should be rebuilt from forward mappings)
        assert_eq!(cache.kalshi_to_poly("epl", "cfc"), Some("che".to_string()));
        assert_eq!(cache.kalshi_to_poly("epl", "ars"), Some("ars".to_string()));
        assert_eq!(cache.kalshi_to_poly("bun", "bmu"), Some("bay".to_string()));
    }

    #[test]
    fn test_save_creates_v2_format() {
        let mut cache = TeamCache::default();
        cache.insert("epl", "che", "cfc");
        cache.insert("nba", "lal", "lal");

        let test_file = ".test_cache_save.json";
        cache.save_to(test_file).unwrap();

        // Read back and verify it's in v2 format
        let contents = std::fs::read_to_string(test_file).unwrap();
        assert!(contents.contains("\"forward\""));
        assert!(!contents.contains("\"mappings\""));

        // Verify it can be loaded back
        let loaded = TeamCache::load_from(test_file);

        // Clean up
        let _ = std::fs::remove_file(test_file);

        assert_eq!(loaded.len(), 2);
        assert_eq!(loaded.poly_to_kalshi("epl", "che"), Some("cfc".to_string()));
    }

    #[test]
    fn test_reverse_rebuild_with_collisions() {
        // Test case where multiple poly codes map to same kalshi code
        let v1_json = r#"{
            "mappings": {
                "epl:che": "cfc",
                "epl:chelsea": "cfc"
            }
        }"#;

        let test_file = ".test_cache_collision.json";
        std::fs::write(test_file, v1_json).unwrap();

        let cache = TeamCache::load_from(test_file);

        // Clean up
        let _ = std::fs::remove_file(test_file);

        // Both forward lookups should work
        assert_eq!(cache.poly_to_kalshi("epl", "che"), Some("cfc".to_string()));
        assert_eq!(
            cache.poly_to_kalshi("epl", "chelsea"),
            Some("cfc".to_string())
        );

        // Reverse lookup will have one of them (whichever was processed last)
        let reverse = cache.kalshi_to_poly("epl", "cfc");
        assert!(reverse.is_some());
        let val = reverse.unwrap();
        assert!(val == "che" || val == "chelsea");
    }
}
