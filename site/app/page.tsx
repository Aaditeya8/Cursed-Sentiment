import { GegeMomentsList } from "./components/GegeMomentsList";
import { HeadlineChart } from "./components/HeadlineChart";
import { Hero } from "./components/Hero";
import { HeroStats } from "./components/HeroStats";
import { LastUpdated } from "./components/LastUpdated";
import { Nav } from "./components/Nav";
import { PolarisationTable } from "./components/PolarisationTable";
import { SentimentLandscape } from "./components/SentimentLandscape";
import { SubredditBreakdown } from "./components/SubredditBreakdown";

/**
 * Homepage — new "Cursed Field Manual" structure.
 *
 * Order:
 *   nav
 *   hero (with kanji backdrop + strikethrough + meta column)
 *   stats strip (4 cards: mentions / polarising / warmest / coldest)
 *   01 / 05  How they aged                (HeadlineChart)
 *   02 / 05  Where the conversation lives (SubredditBreakdown)
 *   03 / 05  The landscape                (SentimentLandscape)
 *   04 / 05  Who splits the room          (PolarisationTable)
 *   05 / 05  Gege moments                 (GegeMomentsList)
 *   footer (simple — commit 4 brings the full multi-column kanji-signed one)
 *
 * Note: existing data-viz components still render their own internal
 * headers in this commit. The visual result has the new outer section-head
 * AND the old inner component title side by side. Commit 3 strips the
 * inner titles when each component gets restyled.
 */
export default function HomePage() {
  return (
    <main>
      <Nav />
      <Hero />
      <HeroStats />

      <section id="chart" className="section">
        <div className="section-head">
          <div className="section-head-title">
            <span className="section-num">01 / 05</span>
            <h2>How they aged</h2>
          </div>
          <p className="lede">
            Six-character weekly mean sentiment, five chapter events overlaid.
            The chapter 236 reference line pulses — that&apos;s where the
            floor fell out.
          </p>
        </div>
        <HeadlineChart />
      </section>

      <section id="subreddit" className="section">
        <div className="section-head">
          <div className="section-head-title">
            <span className="section-num">02 / 05</span>
            <h2>Where the conversation lives</h2>
          </div>
          <p className="lede">
            Per-character mention split across the three subreddits.
            r/Jujutsufolk skews meme; r/Jujutsushi skews analytical;
            r/JuJutsuKaisen is the main hub.
          </p>
        </div>
        <SubredditBreakdown />
      </section>

      <section id="landscape" className="section">
        <div className="section-head">
          <div className="section-head-title">
            <span className="section-num">03 / 05</span>
            <h2>The landscape</h2>
          </div>
          <p className="lede">
            Every tracked character placed by warmth, controversy, and volume.
            The upper-right quadrant is where the fandom is loudest and most
            divided.
          </p>
        </div>
        <SentimentLandscape />
      </section>

      <section id="polar" className="section">
        <div className="section-head">
          <div className="section-head-title">
            <span className="section-num">04 / 05</span>
            <h2>Who splits the room</h2>
          </div>
          <p className="lede">
            Polarisation 1.0 = fandom evenly split. Min 50 mentions. The mean
            column shows which way the disagreement leans on average.
          </p>
        </div>
        <PolarisationTable />
      </section>

      <section id="moments" className="section">
        <div className="section-head">
          <div className="section-head-title">
            <span className="section-num">05 / 05</span>
            <h2>Gege moments</h2>
          </div>
          <p className="lede">
            Weeks where weekly sentiment shifted &gt;2σ from baseline, paired
            with the chapter that dropped that week.
          </p>
        </div>
        <GegeMomentsList />
      </section>

      <footer className="mt-24 pt-12 border-t border-smoke/20 font-mono text-xs text-smoke flex flex-wrap justify-between gap-4">
        <div>
          Built by{" "}
          <a
            href="https://aaditeyas.vercel.app"
            className="underline decoration-smoke/40 underline-offset-4 hover:text-bone"
          >
            Aaditeya Sharma
          </a>
          . Source on{" "}
          <a
            href="https://github.com/Aaditeya8/Cursed-Sentiment"
            className="underline decoration-smoke/40 underline-offset-4 hover:text-bone"
          >
            GitHub
          </a>
          .
        </div>
        <LastUpdated />
      </footer>
    </main>
  );
}
