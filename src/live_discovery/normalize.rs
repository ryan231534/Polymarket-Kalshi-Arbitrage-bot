//! Team name normalization and matchup key generation.
//!
//! This module provides functions to canonicalize team names and build
//! consistent matchup keys that can be used to join events across exchanges.

use chrono::{DateTime, Datelike, Timelike, Utc};
use std::collections::HashMap;

/// Configuration for normalization
#[derive(Debug, Clone)]
pub struct NormalizationConfig {
    /// Team alias dictionary (maps various names to canonical name)
    pub team_aliases: HashMap<String, String>,
    /// Whether to use date-only matching (ignore time component)
    pub date_only: bool,
}

impl Default for NormalizationConfig {
    fn default() -> Self {
        Self {
            team_aliases: build_default_aliases(),
            date_only: true,
        }
    }
}

/// Normalize a team name to a canonical form.
///
/// Rules applied:
/// 1. Lowercase
/// 2. Strip punctuation
/// 3. Collapse whitespace
/// 4. Apply common abbreviation expansions
/// 5. Apply team alias dictionary
///
/// # Examples
///
/// ```ignore
/// assert_eq!(normalize_team_name("L.A. Lakers", &config), "los angeles lakers");
/// assert_eq!(normalize_team_name("NY Giants", &config), "new york giants");
/// ```
pub fn normalize_team_name(name: &str, config: &NormalizationConfig) -> String {
    // Step 1: Lowercase
    let mut normalized = name.to_lowercase();

    // Step 2: Strip punctuation (keep spaces and alphanumeric)
    normalized = normalized
        .chars()
        .map(|c| {
            if c.is_alphanumeric() || c.is_whitespace() {
                c
            } else {
                ' '
            }
        })
        .collect();

    // Step 3: Collapse whitespace
    normalized = normalized.split_whitespace().collect::<Vec<_>>().join(" ");

    // Step 4: Apply common abbreviation expansions
    normalized = expand_abbreviations(&normalized);

    // Step 5: Check if this is a known team alias BEFORE mascot stripping
    // This prevents stripping mascots from pro teams like "Detroit Lions" or "St. Louis Cardinals"
    if let Some(canonical) = config.team_aliases.get(&normalized) {
        return canonical.clone();
    }

    // Step 6: Strip mascots (college sports teams often include them)
    // Only applied if not matched by alias above
    normalized = strip_mascot(&normalized);

    // Step 7: Check aliases again after mascot stripping (for college team variants)
    if let Some(canonical) = config.team_aliases.get(&normalized) {
        return canonical.clone();
    }

    normalized.trim().to_string()
}

/// Expand common abbreviations in team names
fn expand_abbreviations(name: &str) -> String {
    let tokens: Vec<&str> = name.split_whitespace().collect();
    if tokens.is_empty() {
        return name.to_string();
    }

    // Check for two-letter abbreviations that become "x y" after punctuation stripping
    // E.g., "L.A." -> "l a" after lowercase + strip punctuation
    if tokens.len() >= 2 && tokens[0].len() == 1 && tokens[1].len() == 1 {
        let two_letter = format!("{} {}", tokens[0], tokens[1]);
        let two_letter_expansion = match two_letter.as_str() {
            "l a" => Some("los angeles"),
            "n y" => Some("new york"),
            "s f" => Some("san francisco"),
            "k c" => Some("kansas city"),
            "t b" => Some("tampa bay"),
            "g b" => Some("green bay"),
            "n e" => Some("new england"),
            "n o" => Some("new orleans"),
            "d c" => Some("washington"),
            _ => None,
        };
        if let Some(expanded) = two_letter_expansion {
            return if tokens.len() == 2 {
                expanded.to_string()
            } else {
                format!("{} {}", expanded, tokens[2..].join(" "))
            };
        }
    }

    // City/region abbreviations that only make sense at START of team name
    // Map first token if it's a known abbreviation
    let first_expansion = match tokens[0] {
        "la" | "l.a." | "l.a" => Some("los angeles"),
        "ny" | "n.y." | "n.y" => Some("new york"),
        "sf" | "s.f." | "s.f" => Some("san francisco"),
        "kc" | "k.c." | "k.c" => Some("kansas city"),
        "tb" | "t.b." | "t.b" => Some("tampa bay"),
        "gb" | "g.b." | "g.b" => Some("green bay"),
        "ne" | "n.e." | "n.e" => Some("new england"),
        "no" | "n.o." | "n.o" => Some("new orleans"),
        "dc" | "d.c." | "d.c" => Some("washington"),
        "okc" | "o.k.c." => Some("oklahoma city"),
        "phx" => Some("phoenix"),
        "mt" | "mt." => Some("mount"),
        "ft" | "ft." => Some("fort"),
        _ => None,
    };

    // Build result: expanded first token (if any) + rest of tokens
    match first_expansion {
        Some(expanded) => {
            if tokens.len() == 1 {
                expanded.to_string()
            } else {
                format!("{} {}", expanded, tokens[1..].join(" "))
            }
        }
        None => tokens.join(" "),
    }
}

/// Strip mascot names from team names to normalize college teams.
///
/// Example: "Florida Gators" -> "florida", "Michigan State Spartans" -> "michigan state"
fn strip_mascot(name: &str) -> String {
    // Common college basketball and football mascots (in lowercase)
    // Only strip if the last word matches a known mascot
    let mascots = [
        // A
        "aggies",
        "anteaters",
        "aztecs",
        // B
        "badgers",
        "banana slugs",
        "beach",
        "bearcats",
        "bears",
        "beavers",
        "bengals",
        "billiken",
        "billikens",
        "bison",
        "blazers",
        "blue devils",
        "bobcats",
        "boilermakers",
        "broncos",
        "braves",
        "bruins",
        "buccaneers",
        "buckeyes",
        "buffaloes",
        "bulls",
        "bulldogs",
        // C
        "camels",
        "cardinals",
        "cavaliers",
        "chanticleers",
        "chippewas",
        "colonels",
        "commodores",
        "cornhuskers",
        "cougars",
        "cowboys",
        "crimson",
        "crimson tide",
        "crusaders",
        "cyclones",
        // D
        "ducks",
        "deacons",
        "demon deacons",
        "demons",
        "dolphins",
        "dons",
        // E
        "eagles",
        // F
        "falcons",
        "fighting illini",
        "fighting irish",
        "flames",
        "flyers",
        "friars",
        // G
        "gators",
        "gamecocks",
        "gaels",
        "golden bears",
        "golden eagles",
        "golden gophers",
        "golden flash",
        "golden hurricane",
        "gophers",
        "governors",
        "grizzlies",
        "great danes",
        // H
        "hawks",
        "hatters",
        "highlanders",
        "hilltoppers",
        "hokies",
        "hornets",
        "huskies",
        "hoosiers",
        "horned frogs",
        "hurricanes",
        // I
        "ichabods",
        "illini",
        "indians",
        "irish",
        // J
        "jaguars",
        "jaspers",
        "javelinas",
        "jayhawks",
        // K
        "kangaroos",
        "knights",
        // L
        "lancers",
        "leopards",
        "lumberjacks",
        "lions",
        "lobos",
        "longhorns",
        "leathernecks",
        // M
        "maroons",
        "matadors",
        "mean green",
        "midshipmen",
        "miners",
        "minutemen",
        "monarchs",
        "mountaineers",
        "musketeers",
        "mustangs",
        "mavericks",
        // N
        "nittany lions",
        // O
        "ospreys",
        "orange",
        "orangemen",
        "owls",
        // P
        "paladins",
        "panthers",
        "patriots",
        "peacocks",
        "penguins",
        "phoenix",
        "pilots",
        "pioneers",
        "privateers",
        "purple eagles",
        // R
        "racers",
        "rainbow warriors",
        "raiders",
        "rams",
        "rattlers",
        "razorbacks",
        "rebels",
        "redhawks",
        "red raiders",
        "red storm",
        "red wolves",
        "runnin rebels",
        "roadrunners",
        "rockets",
        "roos",
        // S
        "sailors",
        "saints",
        "salukis",
        "scarlet knights",
        "screaming eagles",
        "seahawks",
        "seawolves",
        "seminoles",
        "shockers",
        "skyhawks",
        "sooners",
        "spartans",
        "spiders",
        "stags",
        "sun devils",
        "sycamores",
        // T
        "tar heels",
        "terps",
        "terrapins",
        "terriers",
        "texans",
        "thunderbirds",
        "thundering herd",
        "tigers",
        "titans",
        "titans",
        "toads",
        "toreros",
        "tribe",
        "tritons",
        "trojans",
        "trojans",
        // U
        "utes",
        // V
        "vikings",
        "vandals",
        "volunteers",
        "vaqueros",
        // W
        "wildcats",
        "wolf pack",
        "wolfpack",
        "wolverines",
        "warhawks",
        // X-Z
        "yellow jackets",
        "zips",
    ];

    let words: Vec<&str> = name.split_whitespace().collect();
    if words.len() <= 1 {
        return name.to_string();
    }

    // Check if last word is a mascot
    let last = words.last().unwrap().to_lowercase();
    let mut result = if mascots.contains(&last.as_str()) {
        words[..words.len() - 1].join(" ")
    } else if words.len() >= 2 {
        // Check if last two words are a mascot
        let last_two =
            format!("{} {}", words[words.len() - 2], words[words.len() - 1]).to_lowercase();
        if mascots.contains(&last_two.as_str()) {
            words[..words.len() - 2].join(" ")
        } else {
            name.to_string()
        }
    } else {
        name.to_string()
    };

    // Normalize "st" suffix to "state" for college teams
    // This handles "Michigan St." → "michigan st" → "michigan state"
    // Only do this when "st" is the last word (abbreviation for State)
    if result.ends_with(" st") {
        result = format!("{}state", &result[..result.len() - 2]);
    }

    result
}

/// Build a matchup key from two team names and a start time.
///
/// The key is symmetric (A vs B == B vs A) and includes the date for uniqueness.
///
/// Format: `{team1}_vs_{team2}_{YYYY-MM-DD}` where team1 < team2 alphabetically
pub fn build_matchup_key(
    team_a: &str,
    team_b: &str,
    start_time: &DateTime<Utc>,
    config: &NormalizationConfig,
) -> String {
    let norm_a = normalize_team_name(team_a, config);
    let norm_b = normalize_team_name(team_b, config);

    // Sort alphabetically for symmetric keys
    let (first, second) = if norm_a <= norm_b {
        (norm_a, norm_b)
    } else {
        (norm_b, norm_a)
    };

    // Use date only (or full timestamp)
    let time_part = if config.date_only {
        format!(
            "{:04}-{:02}-{:02}",
            start_time.year(),
            start_time.month(),
            start_time.day()
        )
    } else {
        start_time.format("%Y-%m-%dT%H").to_string()
    };

    format!("{}_vs_{}_{}", first, second, time_part)
}

/// Build matchup key with a time bucket (for fuzzy matching).
///
/// Uses a 6-hour bucket to handle timezone differences.
pub fn build_matchup_key_bucketed(
    team_a: &str,
    team_b: &str,
    start_time: &DateTime<Utc>,
    config: &NormalizationConfig,
    bucket_hours: u32,
) -> String {
    let norm_a = normalize_team_name(team_a, config);
    let norm_b = normalize_team_name(team_b, config);

    let (first, second) = if norm_a <= norm_b {
        (norm_a, norm_b)
    } else {
        (norm_b, norm_a)
    };

    // Bucket time
    let hour = start_time.hour();
    let bucket = hour / bucket_hours;
    let time_part = format!(
        "{:04}-{:02}-{:02}-B{}",
        start_time.year(),
        start_time.month(),
        start_time.day(),
        bucket
    );

    format!("{}_vs_{}_{}", first, second, time_part)
}

/// Check if two times are within tolerance of each other
pub fn times_within_tolerance(
    time_a: &DateTime<Utc>,
    time_b: &DateTime<Utc>,
    tolerance_hours: u32,
) -> bool {
    let diff = (*time_a - *time_b).num_hours().unsigned_abs();
    diff <= tolerance_hours as u64
}

/// Build the default team alias dictionary
fn build_default_aliases() -> HashMap<String, String> {
    let mut aliases = HashMap::new();

    // NFL teams
    add_aliases(
        &mut aliases,
        "arizona cardinals",
        &["cardinals", "az cardinals", "ari cardinals"],
    );
    add_aliases(&mut aliases, "atlanta falcons", &["falcons", "atl falcons"]);
    add_aliases(
        &mut aliases,
        "baltimore ravens",
        &["ravens", "bal ravens", "balt ravens"],
    );
    add_aliases(&mut aliases, "buffalo bills", &["bills", "buf bills"]);
    add_aliases(
        &mut aliases,
        "carolina panthers",
        &["panthers", "car panthers"],
    );
    add_aliases(&mut aliases, "chicago bears", &["bears", "chi bears"]);
    add_aliases(
        &mut aliases,
        "cincinnati bengals",
        &["bengals", "cin bengals", "cincy bengals"],
    );
    add_aliases(&mut aliases, "cleveland browns", &["browns", "cle browns"]);
    add_aliases(&mut aliases, "dallas cowboys", &["cowboys", "dal cowboys"]);
    add_aliases(&mut aliases, "denver broncos", &["broncos", "den broncos"]);
    add_aliases(&mut aliases, "detroit lions", &["lions", "det lions"]);
    add_aliases(
        &mut aliases,
        "green bay packers",
        &["packers", "gb packers"],
    );
    add_aliases(&mut aliases, "houston texans", &["texans", "hou texans"]);
    add_aliases(
        &mut aliases,
        "indianapolis colts",
        &["colts", "ind colts", "indy colts"],
    );
    add_aliases(
        &mut aliases,
        "jacksonville jaguars",
        &["jaguars", "jax jaguars", "jags"],
    );
    add_aliases(&mut aliases, "kansas city chiefs", &["chiefs", "kc chiefs"]);
    add_aliases(
        &mut aliases,
        "las vegas raiders",
        &["raiders", "lv raiders", "oakland raiders"],
    );
    add_aliases(
        &mut aliases,
        "los angeles chargers",
        &[
            "chargers",
            "la chargers",
            "lac chargers",
            "san diego chargers",
        ],
    );
    add_aliases(
        &mut aliases,
        "los angeles rams",
        &[
            "rams",
            "la rams",
            "lar rams",
            "st louis rams",
            "saint louis rams",
        ],
    );
    add_aliases(
        &mut aliases,
        "miami dolphins",
        &["dolphins", "mia dolphins"],
    );
    add_aliases(
        &mut aliases,
        "minnesota vikings",
        &["vikings", "min vikings"],
    );
    add_aliases(
        &mut aliases,
        "new england patriots",
        &["patriots", "ne patriots", "pats"],
    );
    add_aliases(&mut aliases, "new orleans saints", &["saints", "no saints"]);
    add_aliases(
        &mut aliases,
        "new york giants",
        &["giants", "ny giants", "nyg"],
    );
    add_aliases(&mut aliases, "new york jets", &["jets", "ny jets", "nyj"]);
    add_aliases(
        &mut aliases,
        "philadelphia eagles",
        &["eagles", "phi eagles", "philly eagles"],
    );
    add_aliases(
        &mut aliases,
        "pittsburgh steelers",
        &["steelers", "pit steelers"],
    );
    add_aliases(
        &mut aliases,
        "san francisco 49ers",
        &["49ers", "niners", "sf 49ers"],
    );
    add_aliases(
        &mut aliases,
        "seattle seahawks",
        &["seahawks", "sea seahawks"],
    );
    add_aliases(
        &mut aliases,
        "tampa bay buccaneers",
        &["buccaneers", "bucs", "tb buccaneers", "tb bucs"],
    );
    add_aliases(&mut aliases, "tennessee titans", &["titans", "ten titans"]);
    add_aliases(
        &mut aliases,
        "washington commanders",
        &[
            "commanders",
            "was commanders",
            "washington football team",
            "washington redskins",
        ],
    );

    // NBA teams
    add_aliases(&mut aliases, "atlanta hawks", &["hawks", "atl hawks"]);
    add_aliases(&mut aliases, "boston celtics", &["celtics", "bos celtics"]);
    add_aliases(
        &mut aliases,
        "brooklyn nets",
        &["nets", "bkn nets", "new jersey nets"],
    );
    add_aliases(
        &mut aliases,
        "charlotte hornets",
        &["hornets", "cha hornets", "charlotte bobcats"],
    );
    add_aliases(&mut aliases, "chicago bulls", &["bulls", "chi bulls"]);
    add_aliases(
        &mut aliases,
        "cleveland cavaliers",
        &["cavaliers", "cavs", "cle cavaliers"],
    );
    add_aliases(
        &mut aliases,
        "dallas mavericks",
        &["mavericks", "mavs", "dal mavericks"],
    );
    add_aliases(&mut aliases, "denver nuggets", &["nuggets", "den nuggets"]);
    add_aliases(&mut aliases, "detroit pistons", &["pistons", "det pistons"]);
    add_aliases(
        &mut aliases,
        "golden state warriors",
        &["warriors", "gsw", "gs warriors"],
    );
    add_aliases(&mut aliases, "houston rockets", &["rockets", "hou rockets"]);
    add_aliases(&mut aliases, "indiana pacers", &["pacers", "ind pacers"]);
    add_aliases(
        &mut aliases,
        "los angeles clippers",
        &["clippers", "la clippers", "lac"],
    );
    add_aliases(
        &mut aliases,
        "los angeles lakers",
        &["lakers", "la lakers", "lal"],
    );
    add_aliases(
        &mut aliases,
        "memphis grizzlies",
        &["grizzlies", "mem grizzlies", "vancouver grizzlies"],
    );
    add_aliases(&mut aliases, "miami heat", &["heat", "mia heat"]);
    add_aliases(&mut aliases, "milwaukee bucks", &["bucks", "mil bucks"]);
    add_aliases(
        &mut aliases,
        "minnesota timberwolves",
        &["timberwolves", "wolves", "min timberwolves"],
    );
    add_aliases(
        &mut aliases,
        "new orleans pelicans",
        &["pelicans", "nop", "no pelicans", "new orleans hornets"],
    );
    add_aliases(
        &mut aliases,
        "new york knicks",
        &["knicks", "ny knicks", "nyk"],
    );
    add_aliases(
        &mut aliases,
        "oklahoma city thunder",
        &["thunder", "okc thunder", "seattle supersonics"],
    );
    add_aliases(&mut aliases, "orlando magic", &["magic", "orl magic"]);
    add_aliases(
        &mut aliases,
        "philadelphia 76ers",
        &["76ers", "sixers", "phi 76ers"],
    );
    add_aliases(&mut aliases, "phoenix suns", &["suns", "phx suns"]);
    add_aliases(
        &mut aliases,
        "portland trail blazers",
        &["trail blazers", "blazers", "por trail blazers"],
    );
    add_aliases(&mut aliases, "sacramento kings", &["kings", "sac kings"]);
    add_aliases(&mut aliases, "san antonio spurs", &["spurs", "sa spurs"]);
    add_aliases(&mut aliases, "toronto raptors", &["raptors", "tor raptors"]);
    add_aliases(&mut aliases, "utah jazz", &["jazz", "uta jazz"]);
    add_aliases(
        &mut aliases,
        "washington wizards",
        &["wizards", "was wizards", "washington bullets"],
    );

    // NHL teams (abbreviated)
    add_aliases(&mut aliases, "boston bruins", &["bruins", "bos bruins"]);
    add_aliases(
        &mut aliases,
        "toronto maple leafs",
        &["maple leafs", "leafs", "tor maple leafs"],
    );
    add_aliases(
        &mut aliases,
        "new york rangers",
        &["rangers", "ny rangers", "nyr"],
    );
    add_aliases(
        &mut aliases,
        "new york islanders",
        &["islanders", "ny islanders", "nyi"],
    );
    add_aliases(
        &mut aliases,
        "montreal canadiens",
        &["canadiens", "habs", "mtl canadiens"],
    );
    add_aliases(
        &mut aliases,
        "detroit red wings",
        &["red wings", "det red wings"],
    );
    add_aliases(
        &mut aliases,
        "chicago blackhawks",
        &["blackhawks", "chi blackhawks"],
    );
    add_aliases(
        &mut aliases,
        "pittsburgh penguins",
        &["penguins", "pens", "pit penguins"],
    );
    add_aliases(
        &mut aliases,
        "washington capitals",
        &["capitals", "caps", "was capitals"],
    );
    add_aliases(
        &mut aliases,
        "colorado avalanche",
        &["avalanche", "avs", "col avalanche"],
    );
    add_aliases(
        &mut aliases,
        "tampa bay lightning",
        &["lightning", "bolts", "tb lightning"],
    );
    add_aliases(
        &mut aliases,
        "florida panthers",
        &["panthers", "fla panthers"],
    );
    add_aliases(
        &mut aliases,
        "vegas golden knights",
        &["golden knights", "vgk", "vegas knights"],
    );
    add_aliases(&mut aliases, "edmonton oilers", &["oilers", "edm oilers"]);
    add_aliases(&mut aliases, "calgary flames", &["flames", "cgy flames"]);
    add_aliases(
        &mut aliases,
        "vancouver canucks",
        &["canucks", "van canucks"],
    );

    // MLB teams
    add_aliases(
        &mut aliases,
        "new york yankees",
        &["yankees", "yanks", "ny yankees", "nyy"],
    );
    add_aliases(&mut aliases, "new york mets", &["mets", "ny mets", "nym"]);
    add_aliases(
        &mut aliases,
        "boston red sox",
        &["red sox", "sox", "bos red sox"],
    );
    add_aliases(
        &mut aliases,
        "los angeles dodgers",
        &["dodgers", "la dodgers", "lad"],
    );
    add_aliases(
        &mut aliases,
        "los angeles angels",
        &["angels", "la angels", "laa", "anaheim angels"],
    );
    add_aliases(&mut aliases, "chicago cubs", &["cubs", "chi cubs"]);
    add_aliases(
        &mut aliases,
        "chicago white sox",
        &["white sox", "chi white sox", "chw"],
    );
    add_aliases(&mut aliases, "houston astros", &["astros", "hou astros"]);
    add_aliases(&mut aliases, "atlanta braves", &["braves", "atl braves"]);
    add_aliases(
        &mut aliases,
        "philadelphia phillies",
        &["phillies", "phils", "phi phillies"],
    );
    add_aliases(&mut aliases, "san diego padres", &["padres", "sd padres"]);
    add_aliases(
        &mut aliases,
        "san francisco giants",
        &["giants", "sf giants"],
    );
    add_aliases(&mut aliases, "texas rangers", &["rangers", "tex rangers"]);
    add_aliases(
        &mut aliases,
        "seattle mariners",
        &["mariners", "sea mariners"],
    );
    add_aliases(
        &mut aliases,
        "arizona diamondbacks",
        &["diamondbacks", "dbacks", "ari diamondbacks"],
    );
    add_aliases(
        &mut aliases,
        "baltimore orioles",
        &["orioles", "os", "bal orioles"],
    );
    add_aliases(
        &mut aliases,
        "saint louis cardinals",
        &["st louis cardinals", "stl cardinals"],
    );

    // College Football (NCAAF/CFB) - Top programs
    // NOTE: For college teams, canonical is the SHORT school name (no mascot)
    add_college_aliases(
        &mut aliases,
        "alabama",
        &["alabama crimson tide", "bama", "crimson tide"],
    );
    add_college_aliases(
        &mut aliases,
        "georgia",
        &["georgia bulldogs", "uga", "dawgs"],
    );
    add_college_aliases(
        &mut aliases,
        "ohio state",
        &["ohio state buckeyes", "osu", "buckeyes"],
    );
    add_college_aliases(
        &mut aliases,
        "michigan",
        &["michigan wolverines", "wolverines", "umich"],
    );
    add_college_aliases(
        &mut aliases,
        "texas",
        &["texas longhorns", "longhorns", "ut"],
    );
    add_college_aliases(
        &mut aliases,
        "usc",
        &["usc trojans", "trojans", "southern cal"],
    );
    add_college_aliases(
        &mut aliases,
        "notre dame",
        &["notre dame fighting irish", "irish", "nd", "fighting irish"],
    );
    add_college_aliases(&mut aliases, "clemson", &["clemson tigers", "tigers"]);
    add_college_aliases(
        &mut aliases,
        "penn state",
        &["penn state nittany lions", "psu", "nittany lions"],
    );
    add_college_aliases(
        &mut aliases,
        "florida state",
        &["florida state seminoles", "fsu", "seminoles", "noles"],
    );
    add_college_aliases(&mut aliases, "florida", &["florida gators", "gators", "uf"]);
    add_college_aliases(
        &mut aliases,
        "lsu",
        &["lsu tigers", "louisiana state", "tigers"],
    );
    add_college_aliases(
        &mut aliases,
        "oklahoma",
        &["oklahoma sooners", "sooners", "ou"],
    );
    add_college_aliases(&mut aliases, "oregon", &["oregon ducks", "ducks"]);
    add_college_aliases(
        &mut aliases,
        "tennessee",
        &["tennessee volunteers", "vols", "volunteers"],
    );

    // College Basketball (NCAAMB/CBB) - Top programs
    add_college_aliases(&mut aliases, "duke", &["duke blue devils", "blue devils"]);
    add_college_aliases(
        &mut aliases,
        "kentucky",
        &["kentucky wildcats", "uk wildcats", "wildcats"],
    );
    add_college_aliases(
        &mut aliases,
        "kansas",
        &["kansas jayhawks", "ku", "jayhawks"],
    );
    add_college_aliases(
        &mut aliases,
        "north carolina",
        &["north carolina tar heels", "unc", "tar heels"],
    );
    add_college_aliases(&mut aliases, "gonzaga", &["gonzaga bulldogs", "zags"]);
    add_college_aliases(&mut aliases, "villanova", &["villanova wildcats", "nova"]);
    add_college_aliases(
        &mut aliases,
        "uconn",
        &["uconn huskies", "connecticut", "huskies"],
    );
    add_college_aliases(
        &mut aliases,
        "purdue",
        &["purdue boilermakers", "boilermakers"],
    );
    add_college_aliases(&mut aliases, "houston", &["houston cougars", "coogs"]);
    add_college_aliases(&mut aliases, "baylor", &["baylor bears", "bears"]);

    // Additional common college teams for better matching
    add_college_aliases(&mut aliases, "tcu", &["tcu horned frogs", "horned frogs"]);
    add_college_aliases(&mut aliases, "missouri", &["missouri tigers"]);
    add_college_aliases(
        &mut aliases,
        "mississippi state",
        &["mississippi state bulldogs"],
    );
    add_college_aliases(&mut aliases, "auburn", &["auburn tigers"]);
    add_college_aliases(
        &mut aliases,
        "arkansas",
        &["arkansas razorbacks", "razorbacks"],
    );
    add_college_aliases(
        &mut aliases,
        "south carolina",
        &["south carolina gamecocks", "gamecocks"],
    );
    add_college_aliases(
        &mut aliases,
        "ole miss",
        &["ole miss rebels", "mississippi rebels"],
    );
    add_college_aliases(
        &mut aliases,
        "texas a m",
        &["texas a m aggies", "aggies", "tamu"],
    );
    add_college_aliases(
        &mut aliases,
        "vanderbilt",
        &["vanderbilt commodores", "commodores"],
    );
    add_college_aliases(&mut aliases, "indiana", &["indiana hoosiers", "hoosiers"]);
    add_college_aliases(&mut aliases, "iowa", &["iowa hawkeyes", "hawkeyes"]);
    add_college_aliases(
        &mut aliases,
        "michigan state",
        &["michigan state spartans", "spartans", "msu"],
    );
    add_college_aliases(
        &mut aliases,
        "illinois",
        &["illinois fighting illini", "fighting illini", "illini"],
    );
    add_college_aliases(&mut aliases, "wisconsin", &["wisconsin badgers", "badgers"]);
    add_college_aliases(
        &mut aliases,
        "minnesota",
        &["minnesota golden gophers", "golden gophers", "gophers"],
    );
    add_college_aliases(&mut aliases, "northwestern", &["northwestern wildcats"]);
    add_college_aliases(
        &mut aliases,
        "nebraska",
        &["nebraska cornhuskers", "cornhuskers"],
    );
    add_college_aliases(
        &mut aliases,
        "maryland",
        &["maryland terrapins", "terrapins", "terps"],
    );
    add_college_aliases(
        &mut aliases,
        "rutgers",
        &["rutgers scarlet knights", "scarlet knights"],
    );
    add_college_aliases(&mut aliases, "ucla", &["ucla bruins", "bruins"]);
    add_college_aliases(&mut aliases, "arizona", &["arizona wildcats"]);
    add_college_aliases(
        &mut aliases,
        "arizona state",
        &["arizona state sun devils", "sun devils", "asu"],
    );
    add_college_aliases(
        &mut aliases,
        "colorado",
        &["colorado buffaloes", "buffaloes", "buffs"],
    );
    add_college_aliases(&mut aliases, "utah", &["utah utes", "utes"]);
    add_college_aliases(&mut aliases, "stanford", &["stanford cardinal", "cardinal"]);
    add_college_aliases(
        &mut aliases,
        "california",
        &["california golden bears", "cal bears", "cal"],
    );
    add_college_aliases(&mut aliases, "washington", &["washington huskies"]);
    add_college_aliases(
        &mut aliases,
        "washington state",
        &["washington state cougars", "wsu"],
    );
    add_college_aliases(
        &mut aliases,
        "oregon state",
        &["oregon state beavers", "beavers"],
    );

    // EPL teams (abbreviated)
    add_aliases(
        &mut aliases,
        "manchester united",
        &["man united", "man utd", "manu"],
    );
    add_aliases(&mut aliases, "manchester city", &["man city", "mcfc"]);
    add_aliases(&mut aliases, "chelsea", &["chelsea fc", "cfc"]);
    add_aliases(&mut aliases, "arsenal", &["arsenal fc", "afc"]);
    add_aliases(&mut aliases, "liverpool", &["liverpool fc", "lfc"]);
    add_aliases(
        &mut aliases,
        "tottenham hotspur",
        &["tottenham", "spurs fc", "thfc"],
    );

    aliases
}

/// Helper to add multiple aliases pointing to a canonical name
fn add_aliases(map: &mut HashMap<String, String>, canonical: &str, aliases: &[&str]) {
    for alias in aliases {
        map.insert(alias.to_string(), canonical.to_string());
    }
    // Also map canonical to itself for consistency
    map.insert(canonical.to_string(), canonical.to_string());
}

/// Helper for college teams: canonical is the SHORT school name (no mascot).
/// All variants (including "school + mascot") map to the short canonical.
fn add_college_aliases(
    map: &mut HashMap<String, String>,
    canonical_short: &str,
    variants: &[&str],
) {
    for variant in variants {
        map.insert(variant.to_string(), canonical_short.to_string());
    }
    // Map canonical to itself
    map.insert(canonical_short.to_string(), canonical_short.to_string());
}

/// Parse team names from a match title string.
///
/// Common formats:
/// - "Team A vs Team B"
/// - "Team A @ Team B" (away @ home)
/// - "Team A - Team B"
/// - "Team A v Team B"
///
/// Returns (away_team, home_team) if parseable, or (team_a, team_b) if order is unclear
pub fn parse_teams_from_title(title: &str) -> Option<(String, String)> {
    // Try various delimiters
    let delimiters = [" vs ", " vs. ", " @ ", " v ", " - ", " at "];

    for delim in delimiters {
        if let Some(pos) = title.to_lowercase().find(delim) {
            let team_a = title[..pos].trim().to_string();
            let team_b = title[pos + delim.len()..].trim().to_string();

            if !team_a.is_empty() && !team_b.is_empty() {
                // @ means away @ home
                if delim == " @ " || delim == " at " {
                    return Some((team_a, team_b)); // (away, home)
                } else {
                    return Some((team_a, team_b)); // order as given
                }
            }
        }
    }

    None
}

/// Extract team code from Kalshi ticker suffix
///
/// Example: "KXNFLGAME-26JAN01BUFDET-BUF" -> "BUF"
pub fn extract_team_from_ticker(ticker: &str) -> Option<String> {
    ticker.rsplit('-').next().map(|s| s.to_uppercase())
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;

    #[test]
    fn test_normalize_team_name_basic() {
        let config = NormalizationConfig::default();

        assert_eq!(
            normalize_team_name("Los Angeles Lakers", &config),
            "los angeles lakers"
        );
        assert_eq!(
            normalize_team_name("L.A. Lakers", &config),
            "los angeles lakers"
        );
        assert_eq!(
            normalize_team_name("LA Lakers", &config),
            "los angeles lakers"
        );
    }

    #[test]
    fn test_normalize_team_name_abbreviations() {
        let config = NormalizationConfig::default();

        assert_eq!(normalize_team_name("NY Giants", &config), "new york giants");
        assert_eq!(
            normalize_team_name("KC Chiefs", &config),
            "kansas city chiefs"
        );
        assert_eq!(
            normalize_team_name("GB Packers", &config),
            "green bay packers"
        );
        assert_eq!(
            normalize_team_name("St. Louis Cardinals", &config),
            "saint louis cardinals"
        );
    }

    #[test]
    fn test_normalize_team_name_aliases() {
        let config = NormalizationConfig::default();

        assert_eq!(normalize_team_name("Bills", &config), "buffalo bills");
        assert_eq!(
            normalize_team_name("Patriots", &config),
            "new england patriots"
        );
        assert_eq!(normalize_team_name("Pats", &config), "new england patriots");
        assert_eq!(normalize_team_name("49ers", &config), "san francisco 49ers");
        assert_eq!(
            normalize_team_name("Niners", &config),
            "san francisco 49ers"
        );
    }

    #[test]
    fn test_build_matchup_key_symmetric() {
        let config = NormalizationConfig::default();
        let time = Utc.with_ymd_and_hms(2026, 1, 5, 18, 0, 0).unwrap();

        let key1 = build_matchup_key("Buffalo Bills", "Detroit Lions", &time, &config);
        let key2 = build_matchup_key("Detroit Lions", "Buffalo Bills", &time, &config);

        assert_eq!(key1, key2, "Keys should be symmetric");
        assert!(key1.contains("buffalo bills"));
        assert!(key1.contains("detroit lions"));
        assert!(key1.contains("2026-01-05"));
    }

    #[test]
    fn test_build_matchup_key_normalized() {
        let config = NormalizationConfig::default();
        let time = Utc.with_ymd_and_hms(2026, 1, 5, 18, 0, 0).unwrap();

        let key1 = build_matchup_key("Bills", "Lions", &time, &config);
        let key2 = build_matchup_key("Buffalo Bills", "Detroit Lions", &time, &config);

        assert_eq!(key1, key2, "Aliases should normalize to same key");
    }

    #[test]
    fn test_times_within_tolerance() {
        let time1 = Utc.with_ymd_and_hms(2026, 1, 5, 18, 0, 0).unwrap();
        let time2 = Utc.with_ymd_and_hms(2026, 1, 5, 20, 0, 0).unwrap();
        let time3 = Utc.with_ymd_and_hms(2026, 1, 6, 2, 0, 0).unwrap();

        assert!(times_within_tolerance(&time1, &time2, 6)); // 2 hours apart
        assert!(times_within_tolerance(&time1, &time3, 8)); // 8 hours apart
        assert!(!times_within_tolerance(&time1, &time3, 6)); // outside 6 hour tolerance
    }

    #[test]
    fn test_parse_teams_from_title() {
        let cases = vec![
            (
                "Buffalo Bills vs Detroit Lions",
                ("Buffalo Bills", "Detroit Lions"),
            ),
            ("Bills @ Lions", ("Bills", "Lions")),
            (
                "KC Chiefs - Denver Broncos",
                ("KC Chiefs", "Denver Broncos"),
            ),
            ("Patriots v Jets", ("Patriots", "Jets")),
            ("Lakers at Celtics", ("Lakers", "Celtics")),
        ];

        for (title, expected) in cases {
            let result = parse_teams_from_title(title);
            assert!(result.is_some(), "Failed to parse: {}", title);
            let (a, b) = result.unwrap();
            assert_eq!(a, expected.0, "Team A mismatch for: {}", title);
            assert_eq!(b, expected.1, "Team B mismatch for: {}", title);
        }
    }

    #[test]
    fn test_extract_team_from_ticker() {
        assert_eq!(
            extract_team_from_ticker("KXNFLGAME-26JAN01BUFDET-BUF"),
            Some("BUF".to_string())
        );
        assert_eq!(
            extract_team_from_ticker("KXNBAGAME-25DEC31LALGSW-LAL"),
            Some("LAL".to_string())
        );
    }

    #[test]
    fn test_college_mascot_stripping() {
        let config = NormalizationConfig::default();

        // Test that mascots are stripped and short names are canonical
        assert_eq!(normalize_team_name("Florida Gators", &config), "florida");
        assert_eq!(normalize_team_name("Florida", &config), "florida");
        assert_eq!(normalize_team_name("TCU Horned Frogs", &config), "tcu");
        assert_eq!(normalize_team_name("TCU", &config), "tcu");
        assert_eq!(normalize_team_name("Baylor Bears", &config), "baylor");
        assert_eq!(normalize_team_name("Baylor", &config), "baylor");
        assert_eq!(normalize_team_name("Missouri Tigers", &config), "missouri");
        assert_eq!(
            normalize_team_name("Michigan State Spartans", &config),
            "michigan state"
        );
        assert_eq!(
            normalize_team_name("Michigan St.", &config),
            "michigan state"
        );
    }

    #[test]
    fn test_college_matchup_keys_match() {
        let config = NormalizationConfig::default();
        let time = Utc.with_ymd_and_hms(2026, 1, 4, 18, 0, 0).unwrap();

        // Same matchup from different formats should produce identical keys
        let key_poly = build_matchup_key("Florida Gators", "Missouri Tigers", &time, &config);
        let key_kalshi = build_matchup_key("Florida", "Missouri", &time, &config);
        assert_eq!(key_poly, key_kalshi, "Poly and Kalshi keys should match");
        assert!(key_poly.contains("florida"), "Key should contain 'florida'");
        assert!(
            key_poly.contains("missouri"),
            "Key should contain 'missouri'"
        );
        assert!(
            !key_poly.contains("gators"),
            "Key should NOT contain mascot 'gators'"
        );
        assert!(
            !key_poly.contains("tigers"),
            "Key should NOT contain mascot 'tigers'"
        );

        // TCU vs Baylor
        let key1 = build_matchup_key("TCU Horned Frogs", "Baylor Bears", &time, &config);
        let key2 = build_matchup_key("TCU", "Baylor", &time, &config);
        assert_eq!(key1, key2, "Keys should match");
        assert!(key1.contains("baylor"), "Key should contain 'baylor'");
        assert!(key1.contains("tcu"), "Key should contain 'tcu'");

        // Michigan State with abbreviation vs full name
        let key3 = build_matchup_key(
            "Michigan State Spartans",
            "Ohio State Buckeyes",
            &time,
            &config,
        );
        let key4 = build_matchup_key("Michigan St.", "Ohio State", &time, &config);
        assert_eq!(key3, key4, "Keys should match even with St. abbreviation");
    }

    #[test]
    fn test_expand_abbreviations_start_only() {
        // City abbreviations should only expand at START of string
        assert_eq!(expand_abbreviations("ne patriots"), "new england patriots");
        assert_eq!(expand_abbreviations("la lakers"), "los angeles lakers");
        assert_eq!(expand_abbreviations("ny giants"), "new york giants");
        assert_eq!(expand_abbreviations("sf 49ers"), "san francisco 49ers");
        assert_eq!(expand_abbreviations("kc chiefs"), "kansas city chiefs");
        assert_eq!(expand_abbreviations("gb packers"), "green bay packers");
        assert_eq!(
            expand_abbreviations("tb buccaneers"),
            "tampa bay buccaneers"
        );
        assert_eq!(expand_abbreviations("no saints"), "new orleans saints");
        assert_eq!(expand_abbreviations("dc united"), "washington united");
        assert_eq!(expand_abbreviations("okc thunder"), "oklahoma city thunder");
        assert_eq!(expand_abbreviations("phx suns"), "phoenix suns");

        // CRITICAL: Mid-string "ne" should NOT be expanded
        assert_eq!(
            expand_abbreviations("uc irvine anteaters"),
            "uc irvine anteaters"
        );
        assert!(!expand_abbreviations("uc irvine anteaters").contains("new england"));

        // Mid-string "la" should NOT be expanded
        assert_eq!(expand_abbreviations("ucla bruins"), "ucla bruins");
        assert!(!expand_abbreviations("ucla bruins").contains("los angeles"));

        // Mid-string "ny" should NOT be expanded
        assert_eq!(
            expand_abbreviations("penn state nittany lions"),
            "penn state nittany lions"
        );
        assert!(!expand_abbreviations("penn state nittany lions").contains("new york"));
    }

    #[test]
    fn test_normalize_team_name_no_midstring_expansion() {
        let config = NormalizationConfig::default();

        // UC Irvine should stay as "uc irvine", NOT get "new england" injected
        let uc_irvine = normalize_team_name("UC Irvine Anteaters", &config);
        assert!(
            uc_irvine.contains("uc irvine"),
            "Should contain 'uc irvine', got: {}",
            uc_irvine
        );
        assert!(
            !uc_irvine.contains("new england"),
            "Should NOT contain 'new england', got: {}",
            uc_irvine
        );

        // UCLA should stay as "ucla", NOT get "los angeles" injected mid-word
        let ucla = normalize_team_name("UCLA Bruins", &config);
        assert!(
            ucla.contains("ucla"),
            "Should contain 'ucla', got: {}",
            ucla
        );

        // Penn State Nittany Lions - "nittany" should NOT trigger "ny" expansion
        let penn_state = normalize_team_name("Penn State Nittany Lions", &config);
        assert!(
            !penn_state.contains("new york"),
            "Should NOT contain 'new york', got: {}",
            penn_state
        );
    }

    #[test]
    fn test_normalize_team_name_start_abbreviations_work() {
        let config = NormalizationConfig::default();

        // NE Patriots should expand to new england patriots
        let ne_pats = normalize_team_name("NE Patriots", &config);
        assert!(
            ne_pats.starts_with("new england"),
            "Should start with 'new england', got: {}",
            ne_pats
        );

        // LA Lakers should expand to los angeles lakers
        let la_lakers = normalize_team_name("LA Lakers", &config);
        assert!(
            la_lakers.starts_with("los angeles"),
            "Should start with 'los angeles', got: {}",
            la_lakers
        );

        // NY Knicks should expand to new york knicks
        let ny_knicks = normalize_team_name("NY Knicks", &config);
        assert!(
            ny_knicks.starts_with("new york"),
            "Should start with 'new york', got: {}",
            ny_knicks
        );
    }
}
