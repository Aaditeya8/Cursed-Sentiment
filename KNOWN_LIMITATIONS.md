# Known limitations

This document is part of the build, not separate from it. The Cursed Sentiment dashboard makes claims about how a fandom feels; those claims are only as good as the methodology that produced them. The intent here is to enumerate where the methodology is known to be lossy or wrong, so anyone reading the dashboard can calibrate accordingly.

## Reddit deletes content over time

Reddit removes a substantial fraction of post bodies after they're written — by users, by moderators, by automod. Around chapter 236's release week (September 2023), spot-checks against silver showed roughly 60% of post bodies marked `[removed]` by the time the historical backfill scraped them. About 88% of posts had no body at scrape time, leaving only the title for classification.

We classify the title-only when the body is gone. Titles in this subreddit tend to be reaction-shaped ("Chapter 236 reaction thread", "I'm not okay"), so this is a real signal — but it's a noisier one than full posts would be. The dashboard treats it identically to full-text classification; users should know the underlying corpus is title-heavy for older content.

## The eval set was self-validated

The 55-case synthetic eval in `reference/eval_synthetic.jsonl` was hand-crafted to test specific JJK-fandom idioms (the "this killed me" construction, sarcastic 🙏, the affectionate cursing of Gege Akutami, etc.). Each case was written *while designing the prompt*, by the same person.

This makes the eval excellent for **regression detection** — if a future prompt edit breaks one of the named idioms, the eval surfaces which one. But it has a self-validation hazard: it cannot detect blind spots in the prompt designer's own framing of the problem. Edge cases the designer didn't think of will not appear in the eval set, and the classifier might fail on them silently.

The mitigation, planned but not yet shipped, is a **200-post hand-labeled real eval** drawn from the actual silver corpus after the historical backfill. That set will catch distribution-shift problems the synthetic set misses.

## Multi-character posts get the same sentiment fanned out to each character

A post that says "Gojo great, Sukuna terrible" is classified once with whatever the model concludes about the post as a whole, then attributed to *both* characters at that sentiment. The fact table fans the classification out per character mention without trying to disambiguate which character the sentiment was about.

This introduces noise on multi-character posts. The tradeoff is intentional — per-character sentence-level sentiment would require either a much more elaborate prompt or a separate per-character classification pass, both of which were rejected as v1 scope creep. The bias is documented and the gold-layer aggregations are robust to it on average, but individual character scores on weeks dominated by multi-character posts will be slightly noisier.

## Polarisation is unweighted by classifier confidence

The polarisation index treats every classification equally: a 0.6-confidence positive carries the same weight as a 0.95-confidence positive. This means polarisation can be slightly inflated by uncertain classifications.

Mention weight (the alias-confidence weight from `characters.yaml`) and classifier confidence both exist as columns on the fact tables, but only mention weight is currently used in the polarisation calculation. Folding classifier confidence in is on the v2 list and would be a one-day change.

## Pre-2020 data is sparse

The Reddit subreddit grew rapidly during the anime's first season (Oct 2020). Pre-anime discussion exists but is concentrated on the manga subreddit (r/Jujutsushi) and is much thinner than the post-anime corpus. Time-series charts that go back to 2019 should be read with this in mind — small mention counts mean noisy weekly aggregates, and the polarisation index is suppressed (returns NaN) on weeks with too few opinionated mentions.

## Arctic Shift field-list quirk

Arctic Shift's API documents `permalink` and `is_self` as selectable fields for both posts and comments, but the live API returns HTTP 400 if either is requested. We omit both from the API request and reconstruct `permalink` client-side from subreddit + post id; `is_self` is inferred from selftext presence (~95% accurate).

This is a fixed regression in the bronze layer, covered by tests in `tests/test_arctic_shift.py`. The reconstruction is correct enough for analytics but won't preserve the rare crosspost edge case where `is_self=true` despite no body content.

## The character alias dictionary is hand-curated and incomplete

`reference/characters.yaml` covers ~30 named characters — the protagonist roster plus the major antagonists and supporting cast. Minor characters (single-arc villains, background students at Tokyo Jujutsu High) are not in the registry and any sentiment about them gets dropped on the floor at the gold-layer fact join.

The registry is intentionally hand-curated rather than auto-mined from the data; the alternative (NER + similarity clustering) would produce noisier coverage at the cost of clean per-character analytics. Adding a character to the registry is a YAML edit that takes about a minute, but the maintenance cost over time is real and accepted.

## Two polarisation metrics ship in the gold layer

Both `polarisation_index` (the asymmetry of pos vs neg mentions) and `polarisation_entropy` (Shannon entropy across all four sentiment classes) are computed in `agg_char_week` and `agg_polarisation`. The dashboard shows the simpler index by default. They answer slightly different questions:

- `polarisation_index` ignores mixed and neutral mentions; it asks "of the people with strong opinions, are they evenly split?"
- `polarisation_entropy` includes all four classes; it asks "is the overall reaction uniformly distributed across all sentiment types?"

A character who's mostly mixed (50% mixed, 25% positive, 25% negative) has high entropy but low index. A character who's evenly split (50% positive, 50% negative) has high entropy *and* high index. Both metrics ship; the choice of which to feature on the dashboard is a design decision, not a methodological one.