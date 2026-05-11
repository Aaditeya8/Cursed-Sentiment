/**
 * The hero — italic Fraunces headline with cursed-red strikethrough on
 * "14,000" and glowing accent on "Gojo Satoru". Big 呪 kanji backdrop.
 * Right-side meta column with version + cron schedule + JJK reference.
 *
 * Server component — all content is static. The dynamic "last refreshed"
 * timestamp lives in the footer's LastUpdated component, not here.
 */
export function Hero() {
  return (
    <section className="hero">
      <div className="hero-kanji" aria-hidden="true">
        呪
      </div>

      <div>
        <div className="hero-eyebrow">
          A Data Pipeline · Five Years · Three Subreddits
        </div>
        <h1>
          What <span className="strike">14,000</span> fans really
          <br />
          feel about <span className="accent">Gojo Satoru</span>
          <br />
          <em>— and everyone else.</em>
        </h1>
        <p className="hero-sub">
          A daily-refreshed sentiment analytics pipeline running over five
          years of <em>r/JuJutsuKaisen</em>, <em>r/Jujutsushi</em>, and{" "}
          <em>r/Jujutsufolk</em> discussion. Character-aware,
          chapter-annotated, and brutally honest about its own limitations.
        </p>
      </div>

      <aside className="hero-meta">
        <strong>v1.0 / Live</strong>
        Pipeline runs
        <br />
        <span style={{ color: "var(--cursed-red)" }}>06:00 UTC daily</span>
        <br />
        <br />
        <strong>Source</strong>
        Arctic Shift
        <br />
        + Llama on Groq
        <br />
        <br />
        <strong>呪術廻戦</strong>
        JJK · 271 chapters
      </aside>
    </section>
  );
}
