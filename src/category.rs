//! Derives the `Category`/`BetType` pair broadcast on `UmaEvent` from Gamma
//! catalog data — never from on-chain data (see the doc comment on
//! `GammaMarket::sports_market_type`/`question` in `enrichment.rs` for why).
//!
//! The actual rules live in `config/category_rules.json`, compiled into the
//! binary via `include_str!` (see `docs/WORKFLOW.md`/CLAUDE.md: no runtime
//! file dependency, changing a rule is a normal code change that goes
//! through the regular test → cross-compile → deploy flow). This module only
//! owns the *matching* logic; add new buckets/keywords/tag IDs to the JSON
//! file, not here.

use std::sync::LazyLock;

use regex::Regex;
use serde::Deserialize;

use crate::model::{BetType, Category};

const RULES_JSON: &str = include_str!("../config/category_rules.json");

static RULES: LazyLock<CompiledRules> = LazyLock::new(|| CompiledRules::parse(RULES_JSON));

/// Classifies one market from the signals `enrichment.rs::compact_market`
/// already has on hand. Pure/local (string and integer comparisons only) —
/// safe to call from the enrichment background sync, never from the
/// propose/dispute hot path.
///
/// `bet_type` is only ever non-`Unspecified` when `category` resolves to
/// `Sports`/`Esports`/`Weather` — the bet-type taxonomy doesn't apply to any
/// other category.
pub fn classify(
    tag_ids: &[u32],
    sports_market_type: Option<&str>,
    question: Option<&str>,
) -> (Category, BetType) {
    let rules = &*RULES;
    let category = rules.category_for(tag_ids);
    let bet_type = if matches!(
        category,
        Category::Sports | Category::Esports | Category::Weather
    ) {
        rules.bet_type_for(category, tag_ids, sports_market_type, question)
    } else {
        BetType::Unspecified
    };
    (category, bet_type)
}

struct CompiledRules {
    tag_rules: Vec<(u32, Category)>,
    tag_bet_type_rules: Vec<(u32, BetType)>,
    // Pre-lowercased "contains" needles, matched against a lowercased
    // `sports_market_type`.
    sports_market_type_rules: Vec<(String, BetType)>,
    question_text_rules: Vec<(Regex, BetType)>,
    // Deliberately one fallback per Category rather than one shared value —
    // see the numeric-grouping doc comment on `BetType` in proto/uma.proto:
    // a shared fallback would let e.g. a Weather market's bet_type land
    // outside the Weather numeric block, breaking "every BetType for
    // category X falls in X's block".
    sports_fallback_bet_type: BetType,
    weather_fallback_bet_type: BetType,
}

impl CompiledRules {
    fn parse(json: &str) -> Self {
        let raw: RawRules =
            serde_json::from_str(json).expect("config/category_rules.json must be valid JSON");
        let tag_rules = raw
            .tag_rules
            .into_iter()
            .map(|rule| (rule.tag_id, parse_category(&rule.category)))
            .collect();
        let tag_bet_type_rules = raw
            .tag_bet_type_rules
            .into_iter()
            .map(|rule| (rule.tag_id, parse_bet_type(&rule.bet_type)))
            .collect();
        let sports_market_type_rules = raw
            .sports_market_type_rules
            .into_iter()
            .map(|rule| (rule.contains.to_lowercase(), parse_bet_type(&rule.bet_type)))
            .collect();
        let question_text_rules = raw
            .question_text_rules
            .into_iter()
            .map(|rule| {
                // Case-insensitive: the exact capitalization of a Gamma
                // `question` string isn't a contract, and rules should keep
                // matching regardless (e.g. `hurricane` shows up both
                // lowercase mid-sentence and capitalized at a title's start).
                let compiled = Regex::new(&format!("(?i){}", rule.regex)).unwrap_or_else(|error| {
                    panic!(
                        "config/category_rules.json: invalid regex {:?}: {error}",
                        rule.regex
                    )
                });
                (compiled, parse_bet_type(&rule.bet_type))
            })
            .collect();
        Self {
            tag_rules,
            tag_bet_type_rules,
            sports_market_type_rules,
            question_text_rules,
            sports_fallback_bet_type: parse_bet_type(&raw.sports_fallback_bet_type),
            weather_fallback_bet_type: parse_bet_type(&raw.weather_fallback_bet_type),
        }
    }

    fn category_for(&self, tag_ids: &[u32]) -> Category {
        for &(tag_id, category) in &self.tag_rules {
            if tag_ids.contains(&tag_id) {
                return category;
            }
        }
        if tag_ids.is_empty() {
            Category::Unspecified
        } else {
            Category::Other
        }
    }

    fn bet_type_for(
        &self,
        category: Category,
        tag_ids: &[u32],
        sports_market_type: Option<&str>,
        question: Option<&str>,
    ) -> BetType {
        for &(tag_id, bet_type) in &self.tag_bet_type_rules {
            if tag_ids.contains(&tag_id) {
                return bet_type;
            }
        }
        let fallback = if category == Category::Weather {
            self.weather_fallback_bet_type
        } else {
            self.sports_fallback_bet_type
        };
        if let Some(sports_market_type) = sports_market_type {
            let lower = sports_market_type.to_lowercase();
            return self
                .sports_market_type_rules
                .iter()
                .find(|(needle, _)| lower.contains(needle.as_str()))
                .map_or(fallback, |(_, bet_type)| *bet_type);
        }
        if let Some(question) = question {
            return self
                .question_text_rules
                .iter()
                .find(|(regex, _)| regex.is_match(question))
                .map_or(fallback, |(_, bet_type)| *bet_type);
        }
        BetType::Unspecified
    }
}

#[derive(Deserialize)]
struct RawRules {
    tag_rules: Vec<RawTagCategoryRule>,
    tag_bet_type_rules: Vec<RawTagBetTypeRule>,
    sports_market_type_rules: Vec<RawContainsRule>,
    question_text_rules: Vec<RawRegexRule>,
    sports_fallback_bet_type: String,
    weather_fallback_bet_type: String,
}

#[derive(Deserialize)]
struct RawTagCategoryRule {
    tag_id: u32,
    category: String,
}

#[derive(Deserialize)]
struct RawTagBetTypeRule {
    tag_id: u32,
    bet_type: String,
}

#[derive(Deserialize)]
struct RawContainsRule {
    contains: String,
    bet_type: String,
}

#[derive(Deserialize)]
struct RawRegexRule {
    regex: String,
    bet_type: String,
}

fn parse_category(name: &str) -> Category {
    match name {
        "SPORTS" => Category::Sports,
        "ESPORTS" => Category::Esports,
        "POLITICS" => Category::Politics,
        "CRYPTO" => Category::Crypto,
        "CULTURE" => Category::Culture,
        "WEATHER" => Category::Weather,
        "OTHER" => Category::Other,
        "UNSPECIFIED" => Category::Unspecified,
        other => panic!("config/category_rules.json: unknown category {other:?}"),
    }
}

fn parse_bet_type(name: &str) -> BetType {
    match name {
        "MONEYLINE" => BetType::Moneyline,
        "SPREAD" => BetType::Spread,
        "OVER_UNDER" => BetType::OverUnder,
        "GAME_WINNER" => BetType::GameWinner,
        "OUTRIGHT" => BetType::Outright,
        "TEMP_HIGH" => BetType::TempHigh,
        "TEMP_LOW" => BetType::TempLow,
        "PRECIPITATION" => BetType::Precipitation,
        "STORM" => BetType::Storm,
        "SPORTS_PROP" => BetType::SportsProp,
        "WEATHER_OTHER" => BetType::WeatherOther,
        "UNSPECIFIED" => BetType::Unspecified,
        other => panic!("config/category_rules.json: unknown bet_type {other:?}"),
    }
}

#[cfg(test)]
mod tests {
    use super::classify;
    use crate::model::{BetType, Category};

    #[test]
    fn built_in_rules_file_parses() {
        // Forces `RULES` to actually run once — a bad regex/unknown
        // enum name in config/category_rules.json panics here instead of
        // at first real use in production.
        let _ = classify(&[], None, None);
    }

    #[test]
    fn no_enrichment_is_fully_unspecified() {
        assert_eq!(
            classify(&[], None, None),
            (Category::Unspecified, BetType::Unspecified)
        );
    }

    #[test]
    fn known_non_bettable_tag_is_other_with_unspecified_bet_type() {
        // tag 2 = Politics; presence of a question that would otherwise read
        // as "moneyline" text must not leak a bet_type into a non-sports
        // category.
        assert_eq!(
            classify(&[2], None, Some("Trump vs. Harris")),
            (Category::Politics, BetType::Unspecified)
        );
    }

    #[test]
    fn unknown_tag_is_other() {
        assert_eq!(
            classify(&[999_999], None, None),
            (Category::Other, BetType::Unspecified)
        );
    }

    // The sports/esports fixtures below are all real Gamma markets fetched
    // live via `curl https://gamma-api.polymarket.com/markets/<id>` in the
    // session that added this test — not hand-written samples (see
    // docs/WORKFLOW.md 1.2 on why: ancillary/enrichment-adjacent format bugs
    // have twice only shown up against real data).

    #[test]
    fn plain_matchup_is_moneyline() {
        // https://gamma-api.polymarket.com/markets/3931114
        // "New York Yankees vs. Los Angeles Angels", sportsMarketType: "moneyline".
        assert_eq!(
            classify(
                &[1],
                Some("moneyline"),
                Some("New York Yankees vs. Los Angeles Angels")
            ),
            (Category::Sports, BetType::Moneyline)
        );
    }

    #[test]
    fn spreads_sports_market_type_is_spread() {
        // https://gamma-api.polymarket.com/markets/4080237
        // "Spread: Los Angeles Angels (-2.5)", sportsMarketType: "spreads".
        assert_eq!(
            classify(
                &[1],
                Some("spreads"),
                Some("Spread: Los Angeles Angels (-2.5)")
            ),
            (Category::Sports, BetType::Spread)
        );
    }

    #[test]
    fn sport_specific_handicap_variant_is_spread() {
        // https://gamma-api.polymarket.com/markets/4162494
        // "Game Handicap: Dunas Mykhailo (-1.5) vs Ivanytskyi Viktor (+1.5)",
        // sportsMarketType: "table_tennis_game_handicap".
        assert_eq!(
            classify(&[1], Some("table_tennis_game_handicap"), None),
            (Category::Sports, BetType::Spread)
        );
    }

    #[test]
    fn sport_specific_totals_variant_is_over_under() {
        // https://gamma-api.polymarket.com/markets/4162493
        // "Dunas Mykhailo vs. Ivanytskyi Viktor: Total Games O/U 4.5",
        // sportsMarketType: "table_tennis_match_totals".
        assert_eq!(
            classify(&[1], Some("table_tennis_match_totals"), None),
            (Category::Sports, BetType::OverUnder)
        );
    }

    #[test]
    fn period_scoped_totals_variant_is_over_under() {
        // https://gamma-api.polymarket.com/markets/3183077
        // "Lowestoft Town FC vs. Cambridge City FC: Lowestoft Town FC 1st Half
        // O/U 1.5", sportsMarketType: "soccer_first_half_team_totals".
        assert_eq!(
            classify(&[1], Some("soccer_first_half_team_totals"), None),
            (Category::Sports, BetType::OverUnder)
        );
    }

    #[test]
    fn period_scoped_winner_variant_is_moneyline() {
        // https://gamma-api.polymarket.com/markets/4165979
        // "New York Yankees to win the 1st inning?",
        // sportsMarketType: "baseball_team_inning1_winner".
        assert_eq!(
            classify(&[1], Some("baseball_team_inning1_winner"), None),
            (Category::Sports, BetType::Moneyline)
        );
    }

    #[test]
    fn esports_child_moneyline_is_game_winner_under_esports_category() {
        // https://gamma-api.polymarket.com/markets/3915841
        // "LoL: Estral Esports vs Vivo Keyd Stars Academy - Game 1 Winner",
        // sportsMarketType: "child_moneyline", tags include both 64 (Esports)
        // and 1 (Sports) — Esports must win the Category tie-break.
        assert_eq!(
            classify(&[1, 64], Some("child_moneyline"), None),
            (Category::Esports, BetType::GameWinner)
        );
    }

    #[test]
    fn outright_championship_futures_falls_back_to_question_text() {
        // https://gamma-api.polymarket.com/markets/2772194
        // "Will Shakhtar Donetsk win the 2026-27 UEFA Champions League
        // Championship?" — sportsMarketType is null for outright/futures
        // markets, only the question text carries the signal.
        assert_eq!(
            classify(
                &[1],
                None,
                Some("Will Shakhtar Donetsk win the 2026-27 UEFA Champions League Championship?")
            ),
            (Category::Sports, BetType::Outright)
        );
    }

    #[test]
    fn unrecognized_sports_market_type_is_sports_prop_not_a_text_fallback() {
        // sportsMarketType present but not in the rule table must stop at
        // SPORTS_PROP — it must not fall through to text rules, since
        // Polymarket already told us this is a structured bet, just an
        // unmapped one.
        assert_eq!(
            classify(
                &[1],
                Some("some_future_bet_shape"),
                Some("Team A vs. Team B")
            ),
            (Category::Sports, BetType::SportsProp)
        );
    }

    #[test]
    fn highest_temperature_tag_is_temp_high_under_weather_category() {
        // https://gamma-api.polymarket.com/markets/2247194
        // "Will the highest temperature in Atlanta be between 88-89°F on May
        // 15?", tags include 104596 (Highest temperature) and 84 (Weather).
        assert_eq!(
            classify(&[84, 104596], None, None),
            (Category::Weather, BetType::TempHigh)
        );
    }

    #[test]
    fn lowest_temperature_tag_is_temp_low_under_weather_category() {
        // https://gamma-api.polymarket.com/markets/4051092
        // "Will the lowest temperature in Hong Kong be 26°C on September
        // 3?", tags include 104597 (Lowest temperature) and 84 (Weather).
        assert_eq!(
            classify(&[84, 104597], None, None),
            (Category::Weather, BetType::TempLow)
        );
    }

    #[test]
    fn hurricane_tag_is_storm_even_alongside_an_unmapped_science_tag() {
        // https://gamma-api.polymarket.com/markets/1058988
        // "Will any Category 4 hurricane make landfall in the US in before
        // 2027?", tags include 84 (Weather), 102023 (hurricane), and 74
        // (Science) — the unrelated Science tag must not change the result.
        assert_eq!(
            classify(&[74, 84, 102023], None, None),
            (Category::Weather, BetType::Storm)
        );
    }

    #[test]
    fn precipitation_tag_is_precipitation_under_weather_category() {
        assert_eq!(
            classify(&[84, 103041], None, None),
            (Category::Weather, BetType::Precipitation)
        );
    }

    #[test]
    fn weather_question_text_fallback_when_no_specific_tag_matched() {
        assert_eq!(
            classify(
                &[84],
                None,
                Some("Will it snow in New York's Central Park on Christmas Eve?")
            ),
            (Category::Weather, BetType::Precipitation)
        );
    }

    #[test]
    fn weather_market_with_no_matching_signal_falls_back_to_weather_other_not_sports_prop() {
        // Real example: https://gamma-api.polymarket.com/markets/1058988's
        // sibling "Will fewer than 950 tornadoes occur in the United States
        // in 2026?" — tagged Weather, no sub-tag/sportsMarketType/question
        // pattern matches any specific bucket. Must land on the Weather
        // block's own fallback (WEATHER_OTHER), never the Sports block's
        // (SPORTS_PROP) — see the per-Category fallback split in
        // config/category_rules.json and the numeric-grouping doc comment
        // on `BetType` in proto/uma.proto.
        assert_eq!(
            classify(
                &[84],
                None,
                Some("Will fewer than 950 tornadoes occur in the United States in 2026?")
            ),
            (Category::Weather, BetType::WeatherOther)
        );
    }
}
