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
/// `bet_type` is `Unspecified` for any `Category` without a `BetTypeGroup` in
/// `config/category_rules.json` (currently: `Unspecified`/`Other`) — the
/// bet-type taxonomy doesn't apply there.
pub fn classify(
    tag_ids: &[u32],
    sports_market_type: Option<&str>,
    question: Option<&str>,
) -> (Category, BetType) {
    let rules = &*RULES;
    let category = rules.category_for(tag_ids);
    let bet_type = rules
        .bet_type_groups
        .iter()
        .find(|group| group.categories.contains(&category))
        .map_or(BetType::Unspecified, |group| {
            group.bet_type_for(tag_ids, sports_market_type, question)
        });
    (category, bet_type)
}

struct CompiledRules {
    tag_rules: Vec<(u32, Category)>,
    bet_type_groups: Vec<BetTypeGroup>,
}

/// One `BetType` numeric block (see the doc comment on `BetType` in
/// proto/uma.proto) and the rules that resolve into it. Matching is always
/// scoped to a single group — `classify` picks the group whose `categories`
/// contains the event's resolved `Category` first, then only that group's
/// own tag/sportsMarketType/question rules ever run. This is deliberate, not
/// an optimization: two groups' text rules can otherwise look similar enough
/// to collide (the Sports group's "Will X win the Y?" pattern also reads
/// like a Politics election-winner question) and a flat, ungrouped rule list
/// would let one group's value leak onto another `Category`'s events.
struct BetTypeGroup {
    categories: Vec<Category>,
    tag_bet_type_rules: Vec<(u32, BetType)>,
    // Pre-lowercased "contains" needles, matched against a lowercased
    // `sports_market_type`. Empty for every group except Sports/Esports —
    // Gamma's `sportsMarketType` field is never populated for any other
    // category.
    sports_market_type_rules: Vec<(String, BetType)>,
    question_text_rules: Vec<(Regex, BetType)>,
    fallback_bet_type: BetType,
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
        let bet_type_groups = raw
            .bet_type_groups
            .into_iter()
            .map(BetTypeGroup::parse)
            .collect();
        Self {
            tag_rules,
            bet_type_groups,
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
}

impl BetTypeGroup {
    fn parse(raw: RawBetTypeGroup) -> Self {
        let categories = raw
            .categories
            .iter()
            .map(|name| parse_category(name))
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
            categories,
            tag_bet_type_rules,
            sports_market_type_rules,
            question_text_rules,
            fallback_bet_type: parse_bet_type(&raw.fallback_bet_type),
        }
    }

    fn bet_type_for(
        &self,
        tag_ids: &[u32],
        sports_market_type: Option<&str>,
        question: Option<&str>,
    ) -> BetType {
        for &(tag_id, bet_type) in &self.tag_bet_type_rules {
            if tag_ids.contains(&tag_id) {
                return bet_type;
            }
        }
        if let Some(sports_market_type) = sports_market_type {
            let lower = sports_market_type.to_lowercase();
            return self
                .sports_market_type_rules
                .iter()
                .find(|(needle, _)| lower.contains(needle.as_str()))
                .map_or(self.fallback_bet_type, |(_, bet_type)| *bet_type);
        }
        if let Some(question) = question {
            return self
                .question_text_rules
                .iter()
                .find(|(regex, _)| regex.is_match(question))
                .map_or(self.fallback_bet_type, |(_, bet_type)| *bet_type);
        }
        BetType::Unspecified
    }
}

#[derive(Deserialize)]
struct RawRules {
    tag_rules: Vec<RawTagCategoryRule>,
    bet_type_groups: Vec<RawBetTypeGroup>,
}

#[derive(Deserialize)]
struct RawTagCategoryRule {
    tag_id: u32,
    category: String,
}

#[derive(Deserialize)]
struct RawBetTypeGroup {
    categories: Vec<String>,
    tag_bet_type_rules: Vec<RawTagBetTypeRule>,
    sports_market_type_rules: Vec<RawContainsRule>,
    question_text_rules: Vec<RawRegexRule>,
    fallback_bet_type: String,
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
        "SPORTS_PROP" => BetType::SportsProp,
        "TEMP_HIGH" => BetType::TempHigh,
        "TEMP_LOW" => BetType::TempLow,
        "PRECIPITATION" => BetType::Precipitation,
        "STORM" => BetType::Storm,
        "WEATHER_OTHER" => BetType::WeatherOther,
        "PRICE_TARGET" => BetType::PriceTarget,
        "PRICE_THRESHOLD" => BetType::PriceThreshold,
        "UP_DOWN" => BetType::UpDown,
        "CRYPTO_PROP" => BetType::CryptoProp,
        "ELECTION_WINNER" => BetType::ElectionWinner,
        "FED_RATE_DECISION" => BetType::FedRateDecision,
        "TWEET_COUNT" => BetType::TweetCount,
        "POLITICS_PROP" => BetType::PoliticsProp,
        "AWARD_WINNER" => BetType::AwardWinner,
        "MEDIA_METRIC_RANGE" => BetType::MediaMetricRange,
        "CULTURE_PROP" => BetType::CultureProp,
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
    fn category_with_no_bet_type_group_stays_unspecified_despite_matching_text() {
        // Category::Other has no BetTypeGroup in config/category_rules.json
        // at all — an unmapped tag plus a question that would otherwise read
        // as Sports "moneyline" text must not leak a bet_type in from
        // anywhere; there's no group for `classify` to even look in.
        assert_eq!(
            classify(&[999_999], None, Some("Team A vs. Team B")),
            (Category::Other, BetType::Unspecified)
        );
    }

    #[test]
    fn politics_election_text_never_resolves_to_the_sports_groups_outright() {
        // Regression for a real bug caught during review, before this ever
        // shipped: the Sports/Esports group's `^Will\s.+\swin\s(the\s)?.+\?$`
        // OUTRIGHT text rule also matches election-winner questions like this
        // one — when `question_text_rules` was one flat list shared across
        // all categories, this classified as (Politics, Outright), a Sports
        // group (1xxx) value leaking onto a Politics-categorized event. Rules
        // are now scoped per `BetTypeGroup` (see its doc comment) so this
        // must resolve within the Politics group only.
        let (category, bet_type) = classify(
            &[2],
            None,
            Some("Will Sarah Huckabee Sanders win the 2028 Republican presidential nomination?"),
        );
        assert_eq!(category, Category::Politics);
        assert_ne!(bet_type, BetType::Outright);
        assert_eq!(bet_type, BetType::ElectionWinner);
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

    // Crypto/Politics/Culture fixtures below are all real Gamma markets
    // fetched live via `curl https://gamma-api.polymarket.com/markets/<id>`
    // in the session that added this test, same as the sports/weather ones
    // above.

    #[test]
    fn dip_to_price_tag_is_price_target_under_crypto_category() {
        // https://gamma-api.polymarket.com/markets/701502
        // "Will Bitcoin dip to $45,000 by December 31, 2026?", tags include
        // 21 (Crypto) and 102134 (Hit Price).
        assert_eq!(
            classify(&[21, 102134], None, None),
            (Category::Crypto, BetType::PriceTarget)
        );
    }

    #[test]
    fn reach_price_tag_is_also_price_target_regardless_of_direction() {
        // https://gamma-api.polymarket.com/markets/4052446
        // "Will Ethereum reach $3,200 in September?" — same "Hit Price" tag
        // as the dip case above; direction (reach up vs. dip down) doesn't
        // split into separate bet types, same as Sports SPREAD not splitting
        // by which side is favored.
        assert_eq!(
            classify(&[21, 102134], None, None),
            (Category::Crypto, BetType::PriceTarget)
        );
    }

    #[test]
    fn multi_strikes_tag_is_price_threshold_under_crypto_category() {
        // https://gamma-api.polymarket.com/markets/3932599
        // "Will the price of Bitcoin be above $76,000 on September 3?", tags
        // include 21 (Crypto) and 102516 (Multi Strikes).
        assert_eq!(
            classify(&[21, 102516], None, None),
            (Category::Crypto, BetType::PriceThreshold)
        );
    }

    #[test]
    fn up_or_down_tag_is_up_down_under_crypto_category() {
        // https://gamma-api.polymarket.com/markets/4061808
        // "Bitcoin Up or Down on September 3?", tags include 21 (Crypto) and
        // 102127 (Up or Down).
        assert_eq!(
            classify(&[21, 102127], None, None),
            (Category::Crypto, BetType::UpDown)
        );
    }

    #[test]
    fn crypto_market_with_no_matching_signal_falls_back_to_crypto_prop() {
        // https://gamma-api.polymarket.com/markets/920402
        // "Variational FDV above $800M one day after launch?" — tagged
        // Crypto plus subject-specific tags (Pre-Market, Variational, FDV),
        // none of which are bet-type tags.
        assert_eq!(
            classify(
                &[21, 102368, 102802, 139],
                None,
                Some("Variational FDV above $800M one day after launch?")
            ),
            (Category::Crypto, BetType::CryptoProp)
        );
    }

    #[test]
    fn elections_tag_is_election_winner_under_politics_category() {
        // https://gamma-api.polymarket.com/markets/561982
        // "Will Sarah Huckabee Sanders win the 2028 Republican presidential
        // nomination?", tags include 2 (Politics) and 144 (Elections).
        assert_eq!(
            classify(&[2, 144], None, None),
            (Category::Politics, BetType::ElectionWinner)
        );
    }

    #[test]
    fn fed_rates_tag_is_fed_rate_decision_under_politics_category() {
        // https://gamma-api.polymarket.com/markets/2252245
        // "Will the Fed increase interest rates by 25 bps after the
        // September 2026 meeting?", tags include 2 (Politics) and 100196
        // (Fed Rates).
        assert_eq!(
            classify(&[2, 100196], None, None),
            (Category::Politics, BetType::FedRateDecision)
        );
    }

    #[test]
    fn tweet_markets_tag_is_tweet_count_under_politics_category() {
        // https://gamma-api.polymarket.com/markets/3866126
        // "Will Elon Musk post 160-179 tweets from August 28 to September 4,
        // 2026?", tags include 596 (Culture), 2 (Politics), and 972 (Tweet
        // Markets) — Politics wins the Category tie-break (checked before
        // Culture in tag_rules).
        assert_eq!(
            classify(&[2, 596, 972], None, None),
            (Category::Politics, BetType::TweetCount)
        );
    }

    #[test]
    fn politics_market_with_no_matching_signal_falls_back_to_politics_prop() {
        // https://gamma-api.polymarket.com/markets/665374
        // "Will the U.S. invade Iran before 2027?" — tagged Politics plus
        // subject tags (Iran, Trump, Middle East, ...), none bet-type tags.
        assert_eq!(
            classify(
                &[2, 78, 126, 154, 180],
                None,
                Some("Will the U.S. invade Iran before 2027?")
            ),
            (Category::Politics, BetType::PoliticsProp)
        );
    }

    #[test]
    fn be_the_pattern_is_award_winner_under_culture_category() {
        // https://gamma-api.polymarket.com/markets/678416
        // "Will Avengers: Doomsday be the top grossing movie of 2026?" and
        // https://gamma-api.polymarket.com/markets/1130721 "Will Morgan
        // Wallen be the Billboard #1 top artist in 2026?" — both tagged
        // Culture with only subject-specific tags (Movies, Music, ...), no
        // dedicated award-family tag, so this is a text-only match.
        assert_eq!(
            classify(
                &[596],
                None,
                Some("Will Avengers: Doomsday be the top grossing movie of 2026?")
            ),
            (Category::Culture, BetType::AwardWinner)
        );
        assert_eq!(
            classify(
                &[596],
                None,
                Some("Will Morgan Wallen be the Billboard #1 top artist in 2026?")
            ),
            (Category::Culture, BetType::AwardWinner)
        );
    }

    #[test]
    fn metric_range_text_is_media_metric_range_under_culture_category() {
        // https://gamma-api.polymarket.com/markets/3916931 "Will the total
        // domestic gross for Spider-Man: Brand New Day be between 940m and
        // 950m by September 30?" — also text-only, no dedicated tag.
        assert_eq!(
            classify(
                &[596],
                None,
                Some(
                    "Will the total domestic gross for Spider-Man: Brand New Day be between 940m and 950m by September 30?"
                )
            ),
            (Category::Culture, BetType::MediaMetricRange)
        );
    }

    #[test]
    fn culture_market_with_no_matching_signal_falls_back_to_culture_prop() {
        // https://gamma-api.polymarket.com/markets/703258
        // "Will Jesus Christ return before 2027?" — tagged Culture only,
        // question doesn't match the award-winner or metric-range patterns.
        assert_eq!(
            classify(&[596], None, Some("Will Jesus Christ return before 2027?")),
            (Category::Culture, BetType::CultureProp)
        );
    }
}
