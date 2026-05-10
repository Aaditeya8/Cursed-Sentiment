"use client";

import { useEffect, useState } from "react";

interface EvalAxis {
  accuracy: number;
  per_class: Record<
    string,
    { precision: number; recall: number; f1: number; support: number }
  >;
}

interface EvalMiss {
  id: string;
  category: string;
  text: string;
  wrong_axes: string[];
  expected: { sentiment: string; intensity: string; target: string };
  predicted: { sentiment: string; intensity: string; target: string };
}

interface EvalResults {
  eval_set_size: number;
  prompt_version: string;
  model: string;
  ran_at: string;
  sentiment: EvalAxis;
  intensity: EvalAxis;
  target: EvalAxis;
  per_category: Record<string, { total: number; all_correct: number }>;
  misses: EvalMiss[];
}

/**
 * Methodology page: how the pipeline works, why it works that way,
 * and where it's known to fail. Self-aware enough that a hiring
 * manager reads it and trusts the rest of the dashboard.
 */
export default function MethodologyPage() {
  const [evalData, setEvalData] = useState<EvalResults | null>(null);
  const [evalError, setEvalError] = useState<boolean>(false);

  useEffect(() => {
    fetch("/data/eval_results.json")
      .then((r) => (r.ok ? r.json() : Promise.reject()))
      .then(setEvalData)
      .catch(() => setEvalError(true));
  }, []);

  return (
    <main>
      <header className="border-b border-smoke/20 pb-12">
        <div className="font-mono text-xs uppercase tracking-wider text-smoke mb-4">
          Cursed Sentiment / methodology
        </div>
        <h1 className="font-display italic text-headline text-bone max-w-2xl">
          How this dashboard was built — and why you should be skeptical of it.
        </h1>
        <p className="mt-6 text-smoke max-w-2xl leading-relaxed">
          The dashboard&apos;s credibility depends on its methodology being
          legible. This page is the long form. Read it once and decide for
          yourself how much to trust the numbers.
        </p>
        {evalData && (
          <div className="mt-6 font-mono text-xs text-smoke tabular">
            Last classified:{" "}
            <span className="text-bone">
              {new Date(evalData.ran_at).toUTCString()}
            </span>
          </div>
        )}
      </header>

      <Section title="Data collection">
        <p>
          Reddit posts and comments from r/JuJutsuKaisen, r/Jujutsushi, and
          r/Jujutsufolk are pulled from{" "}
          <a
            href="https://arctic-shift.photon-reddit.com/"
            className="text-bone underline decoration-smoke/40 underline-offset-4 hover:decoration-bone"
          >
            Arctic Shift
          </a>
          , a community-run successor to the now-defunct Pushshift. The
          original v1 plan paired Arctic Shift for historical backfill with
          PRAW (the official Python Reddit wrapper) for the daily incremental
          scrape, but in November 2025 Reddit shipped the Responsible Builder
          Policy and closed self-service OAuth approval — new PRAW credentials
          now require manual Reddit review with no committed turnaround. The
          daily cron pulls a 2-day rolling window from Arctic Shift instead;
          silver-layer dedup collapses the 24-hour overlap so a missed run
          doesn&apos;t create gaps. Tradeoff: 12-24 hours of additional
          latency on the freshest data, invisible at the weekly-aggregation
          grain the dashboard ships.
        </p>
        <p>
          Author usernames are SHA-256-hashed with a per-deployment salt
          before they leave the silver layer. The aggregate analytics never
          reference individual users.
        </p>
      </Section>

      <Section title="Character resolution">
        <p>
          A hand-curated alias dictionary maps ~30 canonical character ids to
          their many surface forms — &ldquo;Gojo Satoru,&rdquo;
          &ldquo;Satoru,&rdquo; &ldquo;Gojo-sensei,&rdquo; &ldquo;the honored
          one&rdquo; — with three match modes:
        </p>
        <ul className="list-disc pl-6 space-y-2 text-smoke">
          <li>
            <code className="text-bone">substring</code> for unambiguous full
            names like &ldquo;Higuruma.&rdquo;
          </li>
          <li>
            <code className="text-bone">word_boundary</code> for short tokens
            that need to not match inside other words (&ldquo;Yuji&rdquo;
            should match &ldquo;Yuji&rdquo; but not &ldquo;Yujiro&rdquo;).
          </li>
          <li>
            <code className="text-bone">requires_context</code> for ambiguous
            aliases that only resolve when another character indicator is in
            the same post. &ldquo;The strongest&rdquo; doesn&apos;t fire on
            gym posts.
          </li>
        </ul>
        <p>
          Aliases also carry a confidence weight in [0, 1] that propagates
          all the way to the gold-layer fact tables, so downstream
          aggregations can discount low-confidence mentions. Bare
          &ldquo;Satoru&rdquo; weighs 0.7; full &ldquo;Gojo Satoru&rdquo;
          weighs 1.0.
        </p>
      </Section>

      <Section title="Sentiment classification">
        <p>
          Each post is classified along three axes by Llama 3.1 8B-instant on
          Groq&apos;s free tier. The system prompt names eight specific
          JJK-fandom idioms by hand — the &ldquo;this killed me&rdquo;
          construction, the affectionate cursing of Gege Akutami, the
          sarcastic 🙏 — paired with eight few-shot examples that each teach
          one. A regression test asserts that every named idiom is still
          covered after prompt edits.
        </p>
        <p>
          Results are cached by{" "}
          <code className="text-bone">{`{prompt_version}:{sha256(text)}`}</code>{" "}
          in a JSONL file committed to git, so collaborators don&apos;t pay
          the API bill twice and bumping the prompt version cleanly
          invalidates everything.
        </p>

        <div className="mt-8">
          <h3 className="font-mono text-xs uppercase tracking-wider text-smoke mb-4">
            Synthetic eval results
          </h3>
          {evalError ? (
            <div className="font-mono text-sm text-smoke">
              No eval results have been published yet. Run{" "}
              <code className="text-bone">uv run cursed eval</code>.
            </div>
          ) : evalData ? (
            <>
              <AccuracyBadges data={evalData} />
              <EvalTables data={evalData} />
            </>
          ) : (
            <div className="h-32 bg-smoke/5 animate-pulse" />
          )}
        </div>
      </Section>

      <Section title="The polarisation index">
        <p>
          Polarisation answers a question that mean sentiment can&apos;t:
          how much does the fandom <em>disagree</em> about this character?
          Two characters might both have a mean sentiment of 0.0 — one
          because everyone is neutral, one because half love them and half
          hate them. The polarisation index distinguishes them.
        </p>
        <p>
          The formula is{" "}
          <code className="text-bone">1 − 2·|p − 0.5|</code>, where p is the
          share of opinionated mentions that are positive. 1.0 means a
          perfect 50/50 split (maximum polarisation); 0.0 means one side
          dominates entirely. Mixed and neutral mentions don&apos;t enter
          this calculation.
        </p>
        <p className="text-smoke">
          A second metric — Shannon entropy across all four classes — is
          also computed. Both ship in the gold layer because they answer
          slightly different questions.
        </p>
      </Section>

      <Section title="The Gege moment detector">
        <p>
          For each character with at least 10 mentions in a given week, we
          compute a z-score of that week&apos;s mean sentiment against the
          trailing 12-week baseline. Weeks with |z| &gt; 2 are flagged as
          &ldquo;Gege moments.&rdquo; Each flagged week is paired with the
          closest manga chapter event within ±7 days, if any.
        </p>
        <p>
          The 12-week window is short enough to track shifting baselines
          (the fandom&apos;s mood does drift over years) but long enough to
          be statistically meaningful. The 10-mention floor suppresses
          single-day noise on minor characters.
        </p>
      </Section>

      <Section title="Where the classifier gets it wrong">
        <p className="text-smoke">
          The cases below come straight from the eval run — they are
          classifications the model got wrong on at least one axis. Showing
          them here is deliberate: most portfolios hide their failures, this
          one features them. If you spot a pattern, that&apos;s a v2 prompt
          revision waiting to happen.
        </p>
        {evalData && evalData.misses.length > 0 && (
          <MissesTable misses={evalData.misses.slice(0, 8)} />
        )}
      </Section>

      <Section title="Why you should be skeptical">
        <p>
          A few honest limitations the methodology can&apos;t paper over:
        </p>
        <ul className="list-disc pl-6 space-y-3 text-smoke">
          <li>
            <span className="text-bone">
              The eval set was written by the same person who wrote the
              prompt.
            </span>{" "}
            Self-validation hazard. The synthetic eval guards against
            regressions on named idioms but cannot detect blind spots in
            the prompt designer&apos;s own framing of the problem. A
            hand-labeled real eval is the planned next step.
          </li>
          <li>
            <span className="text-bone">
              Reddit deletes substantial bodies over time.
            </span>{" "}
            Around chapter 236&apos;s release week, ~60% of post bodies
            were marked <code>[removed]</code> by the time we scraped.
            We classify the title-only when the body is gone, which is a
            real signal but a noisier one.
          </li>
          <li>
            <span className="text-bone">
              Multi-character posts get the same sentiment fanned out to
              each character.
            </span>{" "}
            A post saying &ldquo;Gojo great, Sukuna terrible&rdquo; is
            classified once with whatever the model decides about the post
            as a whole, then attributed to both characters at that
            sentiment. This introduces noise on multi-character posts; the
            tradeoff is documented and accepted.
          </li>
          <li>
            <span className="text-bone">
              Polarisation is unweighted by classifier confidence.
            </span>{" "}
            A 0.6-confidence positive carries the same weight as a
            0.95-confidence positive in the index calculation. Folding
            confidence in is on the v2 list.
          </li>
          <li>
            <span className="text-bone">
              The daily window is 2 days wide, not 1.
            </span>{" "}
            Belt-and-suspenders against a missed cron — silver-layer dedup
            collapses the 24-hour overlap. Visible in the cron logs but
            invisible in the dashboard.
          </li>
        </ul>
      </Section>

      <footer className="mt-24 pt-12 border-t border-smoke/20 font-mono text-xs text-smoke">
        <a
          href="/"
          className="underline decoration-smoke/40 underline-offset-4 hover:text-bone"
        >
          ← back to the dashboard
        </a>
      </footer>
    </main>
  );
}

// --- helpers --------------------------------------------------------------

function Section({
  title,
  children,
}: {
  title: string;
  children: React.ReactNode;
}) {
  return (
    <section className="mt-16 max-w-2xl">
      <h2 className="font-display italic text-section text-bone mb-6">
        {title}
      </h2>
      <div className="space-y-4 text-bone leading-relaxed">{children}</div>
    </section>
  );
}

function AccuracyBadges({ data }: { data: EvalResults }) {
  const axes = [
    { name: "sentiment", value: data.sentiment.accuracy },
    { name: "intensity", value: data.intensity.accuracy },
    { name: "target", value: data.target.accuracy },
  ];
  return (
    <div className="grid grid-cols-3 gap-4 mb-8">
      {axes.map((a) => (
        <div
          key={a.name}
          className="border border-smoke/20 px-4 py-3"
        >
          <div className="font-mono text-xs uppercase tracking-wider text-smoke">
            {a.name}
          </div>
          <div className="font-display italic text-2xl text-bone tabular mt-1">
            {(a.value * 100).toFixed(1)}%
          </div>
        </div>
      ))}
    </div>
  );
}

function EvalTables({ data }: { data: EvalResults }) {
  return (
    <div className="space-y-8">
      <div className="font-mono text-xs text-smoke">
        prompt {data.prompt_version} · model {data.model} ·{" "}
        {data.eval_set_size} cases · ran{" "}
        {new Date(data.ran_at).toISOString().slice(0, 10)}
      </div>
      <AxisTable name="sentiment" axis={data.sentiment} />
      <AxisTable name="intensity" axis={data.intensity} />
      <AxisTable name="target" axis={data.target} />
    </div>
  );
}

function AxisTable({ name, axis }: { name: string; axis: EvalAxis }) {
  return (
    <div>
      <div className="font-mono text-xs uppercase tracking-wider text-smoke mb-2">
        {name} · accuracy {(axis.accuracy * 100).toFixed(1)}%
      </div>
      <table className="w-full font-mono text-sm">
        <thead>
          <tr className="border-b border-smoke/20 text-smoke text-xs">
            <th className="text-left py-2 pr-4 font-normal">class</th>
            <th className="text-right py-2 px-4 font-normal">P</th>
            <th className="text-right py-2 px-4 font-normal">R</th>
            <th className="text-right py-2 px-4 font-normal">F1</th>
            <th className="text-right py-2 pl-4 font-normal">n</th>
          </tr>
        </thead>
        <tbody className="text-bone">
          {Object.entries(axis.per_class).map(([cls, m]) => (
            <tr key={cls} className="border-b border-smoke/10">
              <td className="py-2 pr-4">{cls}</td>
              <td className="py-2 px-4 text-right tabular">
                {m.precision.toFixed(2)}
              </td>
              <td className="py-2 px-4 text-right tabular">
                {m.recall.toFixed(2)}
              </td>
              <td className="py-2 px-4 text-right tabular">
                {m.f1.toFixed(2)}
              </td>
              <td className="py-2 pl-4 text-right tabular text-smoke">
                {m.support}
              </td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

function MissesTable({ misses }: { misses: EvalMiss[] }) {
  return (
    <div className="mt-6 space-y-4">
      {misses.map((m) => (
        <div
          key={m.id}
          className="border-l-2 border-gold/50 pl-4 py-1 font-mono text-sm"
        >
          <div className="text-bone leading-relaxed">
            &ldquo;{m.text}&rdquo;
          </div>
          <div className="mt-2 text-xs text-smoke">
            <span className="text-gold">{m.wrong_axes.join(", ")}</span> ·
            expected{" "}
            <span className="text-bone">
              {m.wrong_axes
                .map((axis) => m.expected[axis as keyof typeof m.expected])
                .join("/")}
            </span>
            , got{" "}
            <span className="text-bone">
              {m.wrong_axes
                .map((axis) => m.predicted[axis as keyof typeof m.predicted])
                .join("/")}
            </span>{" "}
            · category {m.category}
          </div>
        </div>
      ))}
    </div>
  );
}
