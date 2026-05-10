/**
 * Typed query layer over the gold parquets.
 *
 * Every gold table has a TypeScript interface and a SQL query constant
 * here. Components import the interface and the SQL together; that
 * keeps the chart's "what data do I want" contract in one place and
 * out of the JSX.
 *
 * Filenames here must match those registered in lib/duckdb.ts initDuckDB.
 * DuckDB resolves bare filenames against its virtual filesystem.
 */

export interface CharacterRow {
  character_id: string;
  display_name: string;
  role: string;
  affiliation: string;
  status: string;
  alias_count: number;
  notes: string;
}

export interface EventRow {
  event_id: string;
  event_date: Date;
  chapter: number | null;
  arc: string;
  title: string;
  description: string;
  spoiler_intensity: string;
  medium: string;
  verified: boolean;
}

export interface CharWeekRow {
  character_id: string;
  display_name?: string;
  year: number;
  week: number;
  week_start: Date;
  mention_count: number;
  mean_sentiment_score: number;
  share_positive: number;
  share_negative: number;
  share_mixed: number;
  share_neutral: number;
  polarisation_index: number;
  polarisation_entropy: number;
}

export interface PolarisationRow {
  character_id: string;
  display_name?: string;
  total_mentions: number;
  mean_sentiment_score: number;
  polarisation_index: number;
  polarisation_entropy: number;
  polarisation_rank: number;
  most_mentioned_rank: number;
}

export interface GegeMomentRow {
  character_id: string;
  display_name?: string;
  week_start: Date;
  sentiment_score: number;
  baseline_mean: number;
  baseline_std: number;
  z_score: number;
  mention_count: number;
  paired_event_id: string | null;
  paired_event_title: string | null;
  paired_event_distance_days: number | null;
}

export const Q_TOP_CHARACTERS = `
  SELECT p.*, c.display_name
  FROM read_parquet('agg_polarisation.parquet') p
  JOIN read_parquet('dim_character.parquet') c
    USING (character_id)
  ORDER BY p.total_mentions DESC
  LIMIT 6
`;

export const Q_HEADLINE_WEEKLY = `
  SELECT w.*, c.display_name
  FROM read_parquet('agg_char_week.parquet') w
  JOIN read_parquet('dim_character.parquet') c
    USING (character_id)
  WHERE c.character_id IN (
    SELECT character_id
    FROM read_parquet('agg_polarisation.parquet')
    ORDER BY total_mentions DESC
    LIMIT 6
  )
  ORDER BY w.week_start
`;

export const Q_EVENTS_TIMELINE = `
  SELECT *
  FROM read_parquet('dim_event.parquet')
  ORDER BY event_date
`;

export const Q_POLARISATION_RANKING = `
  SELECT p.*, c.display_name
  FROM read_parquet('agg_polarisation.parquet') p
  JOIN read_parquet('dim_character.parquet') c
    USING (character_id)
  WHERE p.total_mentions >= 50
  ORDER BY p.polarisation_index DESC NULLS LAST
  LIMIT 10
`;

export const Q_GEGE_MOMENTS = `
  SELECT m.*, c.display_name
  FROM read_parquet('gege_moments.parquet') m
  JOIN read_parquet('dim_character.parquet') c
    USING (character_id)
  ORDER BY m.week_start DESC
  LIMIT 10
`;

export interface CharSummaryRow {
  character_id: string;
  display_name: string;
  category: string;
  mentions: number;
  mean_sentiment_score: number;
  polarisation_index: number;
  positive_summary: string;
  negative_summary: string;
  positive_examples: Array<{
    text: string;
    score: number;
    subreddit: string;
    permalink: string;
  }>;
  negative_examples: Array<{
    text: string;
    score: number;
    subreddit: string;
    permalink: string;
  }>;
  generated_at: Date;
  model: string;
}

export const Q_CHAR_SUMMARIES = `
  SELECT *
  FROM read_parquet('char_summary.parquet')
  ORDER BY category
`;

export const Q_SUBREDDIT_BREAKDOWN = `
  WITH all_facts AS (
    SELECT character_id, subreddit FROM read_parquet('fact_post_sentiment.parquet')
    UNION ALL
    SELECT character_id, subreddit FROM read_parquet('fact_comment_sentiment.parquet')
  ),
  top_chars AS (
    SELECT character_id
    FROM read_parquet('agg_polarisation.parquet')
    ORDER BY total_mentions DESC
    LIMIT 6
  )
  SELECT
    c.display_name,
    f.subreddit,
    COUNT(*) AS mention_count
  FROM all_facts f
  JOIN top_chars tc USING (character_id)
  JOIN read_parquet('dim_character.parquet') c USING (character_id)
  GROUP BY c.display_name, f.subreddit
  ORDER BY c.display_name, f.subreddit
`;

export interface SubredditBreakdownRow {
  display_name: string;
  subreddit: string;
  mention_count: number;
}