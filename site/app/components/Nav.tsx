import Link from "next/link";

/**
 * Top-of-page nav. Brand on the left with a pulsing cursed-red dot;
 * uppercase mono nav links on the right with red arrow prefixes on hover.
 *
 * Pure CSS animation, no JS state — server component is fine.
 */
export function Nav() {
  return (
    <nav>
      <Link href="/" className="brand">
        <span className="dot" aria-hidden="true" />
        Cursed Sentiment
      </Link>
      <div className="nav-links">
        <Link href="/#chart">Timeline</Link>
        <Link href="/#subreddit">Subreddits</Link>
        <Link href="/#landscape">Landscape</Link>
        <Link href="/#polar">Polarisation</Link>
        <Link href="/#moments">Moments</Link>
        <Link href="/methodology">Methodology</Link>
        <a
          href="https://github.com/Aaditeya8/Cursed-Sentiment"
          target="_blank"
          rel="noopener noreferrer"
        >
          Repo ↗
        </a>
      </div>
    </nav>
  );
}
