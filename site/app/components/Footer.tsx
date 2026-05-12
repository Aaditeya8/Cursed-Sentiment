import Link from "next/link";

import { LastUpdated } from "./LastUpdated";

/**
 * Magazine colophon. Four columns of metadata + a kanji seal, with
 * publisher signature and refresh time at the bottom. Replaces the
 * temporary single-line footer from commit 2.
 *
 * Server component — only LastUpdated is client (it reads the
 * eval_results.json timestamp via DuckDB-WASM in browser).
 */
export function Footer() {
  return (
    <footer className="site-footer">
      <div className="footer-row">
        <div className="foot-brand">
          <span className="foot-mark" aria-hidden="true">呪</span>
          <p className="foot-blurb">
            <em>Cursed Sentiment</em> is an independent data engineering
            portfolio by <em>Aaditeya Sharma</em>. The pipeline ingests
            Reddit, classifies via Llama 3.1 on Groq, and refreshes daily
            at 06:00 UTC. Built with care over four weekends.
          </p>
        </div>

        <div className="foot-col">
          <h5>The Dashboard</h5>
          <Link href="#chart">Timeline</Link>
          <Link href="#subreddit">Subreddits</Link>
          <Link href="#landscape">Landscape</Link>
          <Link href="#polar">Polarisation</Link>
          <Link href="#moments">Gege Moments</Link>
        </div>

        <div className="foot-col">
          <h5>The Stack</h5>
          <Link href="/methodology">Methodology</Link>
          <a
            href="https://github.com/Aaditeya8/Cursed-Sentiment"
            target="_blank"
            rel="noopener noreferrer"
          >
            Source on GitHub ↗
          </a>
          <a
            href="https://github.com/Aaditeya8/Cursed-Sentiment/blob/main/KNOWN_LIMITATIONS.md"
            target="_blank"
            rel="noopener noreferrer"
          >
            Known limitations ↗
          </a>
          <Link href="#why">Why this exists</Link>
        </div>

        <div className="foot-col">
          <h5>The Author</h5>
          <a
            href="https://aaditeyas.vercel.app"
            target="_blank"
            rel="noopener noreferrer"
          >
            aaditeyas.vercel.app ↗
          </a>
          <a href="mailto:hello@aaditeya.in">hello@aaditeya.in</a>
          <a
            href="https://linkedin.com/in/aaditeyasharma"
            target="_blank"
            rel="noopener noreferrer"
          >
            LinkedIn ↗
          </a>
          <span className="foot-status">
            Exploring data engineering roles.<br />
            Delhi NCR &amp; remote.
          </span>
        </div>
      </div>

      <div className="foot-sig">
        <span className="foot-sig-pub">
          <em>Cursed Sentiment Press</em> · Vol. I · MMXXVI
        </span>
        <span className="foot-sig-refresh">
          pipeline runs 06:00 UTC daily ·{" "}
          <LastUpdated />
        </span>
      </div>
    </footer>
  );
}
