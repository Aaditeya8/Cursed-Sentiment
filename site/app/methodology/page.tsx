"use client";

import { useEffect, useState } from "react";

import { Footer } from "../components/Footer";
import { Nav } from "../components/Nav";

interface PerClass {
  precision: number;
  recall: number;
  f1: number;
  support: number;
}

interface AxisResult {
  accuracy: number;
  per_class: Record<string, PerClass>;
}

interface EvalResults {
  eval_set_size: number;
  prompt_version: string;
  model: string;
  ran_at: string;
  sentiment: AxisResult;
  intensity: AxisResult;
  target: AxisResult;
}

/**
 * Methodology page — 5 numbered chapters explaining how the dashboard's
 * data is produced. Same design language as the homepage (numbered
 * section heads, italic Fraunces titles, mono ledes), reusing every
 * CSS class from globals.css.
 *
 *   01  The Ingestion       — Arctic Shift, 2-day rolling window
 *   02  The Naming          — alias dictionary, 3 match modes
 *   03  The Classification  — Llama 3.1 prompt + caching strategy
 *   04  The Test            — accuracy badges + per-class table
 *   05  The Misses          — 4 misclassifications + known limitations
 *
 * Eval numbers load from /data/eval_results.json. Everything else is
 * editorial prose with hand-curated examples.
 */
export default function MethodologyPage() {
  const [evalData, setEvalData] = useState<EvalResults | null>(null);

  useEffect(() => {
    fetch("/data/eval_results.json", { cache: "no-store" })
      .then((r) => (r.ok ? r.json() : Promise.reject()))
      .then((data: EvalResults) => setEvalData(data))
      .catch(() => {
        /* silent — eval section just renders without numbers */
      });
  }, []);

  return (
    <div className="methodology-page">
      <main>
        <Nav />

        {/* hero */}
        <section className="method-hero">
          <div className="method-hero-kanji" aria-hidden="true">
            法
          </div>
          <div className="method-hero-eyebrow">
            Methodology · how the data is produced
          </div>
          <h1>
            Every chart has a <em>paper trail</em>.<br />
            This is it.
          </h1>
          <p className="method-hero-sub">
            Inside you&apos;ll find the ingestion pipeline, the alias
            dictionary, the actual <em>Llama prompt</em>, the eval results,
            and the cases where the classifier <em>gets it dramatically
            wrong</em>. Skepticism welcome.
          </p>
          <aside className="method-hero-meta">
            <strong>Vol. I</strong>
            The Ingestion
            <br />
            <span className="red">2026 · 05</span>
            <br />
            <br />
            <strong>Eval set</strong>
            {evalData ? `${evalData.eval_set_size} hand-labelled` : "55 hand-labelled"}
            <br />
            <br />
            <strong>Model</strong>
            llama-3.1-8b-instant
          </aside>
        </section>

        {/* Chapter 01 — Ingestion */}
        <section id="ingestion" className="section">
          <div className="section-head">
            <div className="section-head-title">
              <span className="section-num">01 / 05</span>
              <h2>The Ingestion</h2>
            </div>
            <p className="lede">
              In the autumn of <em>2025</em>, Reddit closed its self-service
              OAuth approvals. Existing wrappers like PRAW lost the ability
              to onboard new applications — a door quietly closed on
              unbusinessed, independent projects.
            </p>
          </div>
          <div className="method-body">
            <div>
              <p>
                The alternative was <strong>Arctic Shift</strong>, a
                community-run successor to the long-vanished Pushshift. It
                hosts mirrors of <em>r/JuJutsuKaisen</em>,{" "}
                <em>r/Jujutsushi</em>, and <em>r/Jujutsufolk</em> back to 2020,
                accessible without authentication.
              </p>
              <p>
                The daily cron now pulls a{" "}
                <strong>2-day rolling window</strong> from Arctic Shift;
                silver-layer deduplication collapses the 24-hour overlap. A
                missed run never creates gaps.
              </p>
            </div>
            <div>
              <div className="method-callout">
                <strong>Trade-off, noted.</strong>
                12–24 hours of latency on the freshest data. Invisible at
                the weekly-aggregation grain the dashboard ships.
              </div>
              <p>
                Author usernames are <em>SHA-256-hashed</em> with a
                per-deployment salt before they leave the silver layer. No
                analytic ever references an individual user. The dataset is
                the fandom, not the fans.
              </p>
            </div>
          </div>
        </section>

        {/* Chapter 02 — Naming */}
        <section id="naming" className="section">
          <div className="section-head">
            <div className="section-head-title">
              <span className="section-num">02 / 05</span>
              <h2>The Naming</h2>
            </div>
            <p className="lede">
              Gojo Satoru is also Satoru, Gojo-sensei, the honored one, the
              strongest, six-eyes, and (in 4chan threads) <em>blindfold
              guy</em>. A character is a cloud of names; the question is
              which cloud each mention belongs to.
            </p>
          </div>
          <div className="method-body">
            <div>
              <p>
                A hand-curated <code>characters.yaml</code> maps roughly{" "}
                <strong>30 canonical IDs</strong> to their many surface
                forms, with three match modes — chosen per alias based on
                how forgiving the matcher can afford to be:
              </p>
              <div className="method-callout">
                <strong>substring</strong>
                Unambiguous full names like <em>&ldquo;Higuruma&rdquo;</em> or{" "}
                <em>&ldquo;Yuta Okkotsu&rdquo;</em>.
              </div>
              <div className="method-callout">
                <strong>word_boundary</strong>
                Short tokens like <em>&ldquo;Yuji&rdquo;</em> — must match{" "}
                <em>Yuji</em> as a whole word, not <em>Yujiro</em>.
              </div>
            </div>
            <div>
              <div className="method-callout">
                <strong>requires_context</strong>
                Ambiguous aliases like <em>&ldquo;the strongest&rdquo;</em>{" "}
                only resolve when another character indicator is present in
                the same post. Gym posts and unrelated power-scaling
                discussions unaffected.
              </div>
              <p>
                Each alias carries a <em>confidence weight</em> in{" "}
                <code>[0, 1]</code> that propagates all the way to
                gold-layer aggregations. Bare <em>&ldquo;Satoru&rdquo;</em>{" "}
                weighs 0.7; full <em>&ldquo;Gojo Satoru&rdquo;</em> weighs
                1.0. The polarisation index uses these weights directly.
              </p>
            </div>
          </div>
        </section>

        {/* Chapter 03 — Classification */}
        <section id="classification" className="section">
          <div className="section-head">
            <div className="section-head-title">
              <span className="section-num">03 / 05</span>
              <h2>The Classification</h2>
            </div>
            <p className="lede">
              A small open-weight model, given the right prompt, can do the
              work of an annotator team. The trick is in the prompt — and
              specifically in naming, by hand, the idioms the model has
              never seen.
            </p>
          </div>
          <div className="method-body">
            <div>
              <p>
                Each Reddit post is classified by{" "}
                <strong>Llama 3.1 8B-Instant</strong> on Groq&apos;s free
                tier, on <em>three axes</em> simultaneously: sentiment
                polarity, engagement intensity, and whether the post is{" "}
                <em>about-the-character</em> or{" "}
                <em>about-an-arc-moment</em>.
              </p>
              <p>
                The system prompt names <em>eight specific JJK-fandom
                idioms</em> by hand — the &ldquo;this killed me&rdquo;
                construction, the affectionate cursing of Gege Akutami, the
                sarcastic 🙏 — each paired with a few-shot example. A
                regression test asserts every named idiom stays covered
                after prompt edits.
              </p>
            </div>
            <div>
              <p>
                Results cache by{" "}
                <code>{`{prompt_version}:sha256(text)`}</code> in a JSONL
                committed to git, so collaborators don&apos;t pay the API
                bill twice and bumping the prompt version cleanly
                invalidates everything downstream.
              </p>
              <p>
                Total cost across the full historical backfill —{" "}
                <em>over 13,000 posts and 6,000 comments</em> — was roughly{" "}
                <strong>$1.40</strong>. The daily cron runs free under
                Groq&apos;s rate limits.
              </p>
            </div>

            <pre className="prompt-block full" data-label="system prompt · v1">
              <span className="com">{`// excerpt — full prompt at pipeline/transform/prompts.py`}</span>
              {`\n`}
              <span className="kw">You</span> are a careful fandom analyst for r/JuJutsuKaisen.
              {`\n\n`}
              <span className="kw">For</span> each post, return JSON with these three axes:
              {`\n`}
              {`  `}<span className="key">sentiment</span>:  <span className="str">&quot;positive&quot; | &quot;negative&quot; | &quot;mixed&quot; | &quot;neutral&quot;</span>
              {`\n`}
              {`  `}<span className="key">intensity</span>:  <span className="str">&quot;low&quot; | &quot;medium&quot; | &quot;high&quot;</span>
              {`\n`}
              {`  `}<span className="key">target</span>:     <span className="str">&quot;character&quot; | &quot;arc_moment&quot; | &quot;meta&quot;</span>
              {`\n\n`}
              <span className="kw">Critical:</span> the JJK community uses certain idioms that look
              {`\n`}
              negative on the surface but signal <span className="str">positive engagement</span>:
              {`\n`}
              {`  · `}<span className="str">&quot;this killed me&quot;</span>     → positive, high intensity
              {`\n`}
              {`  · `}<span className="str">&quot;Gege you genius bastard&quot;</span> → positive, high intensity
              {`\n`}
              {`  · `}<span className="str">&quot;💀💀💀&quot;</span>               → positive, medium intensity
              {`\n`}
              {`  · `}sarcastic <span className="str">🙏</span>             → context-dependent; lean negative
              {`\n\n`}
              <span className="kw">Few-shot examples:</span> ...
            </pre>
          </div>
        </section>

        {/* Chapter 04 — The Test */}
        <section id="test" className="section">
          <div className="section-head">
            <div className="section-head-title">
              <span className="section-num">04 / 05</span>
              <h2>The Test</h2>
            </div>
            <p className="lede">
              A self-written eval set of {evalData?.eval_set_size ?? 55}{" "}
              hand-labelled posts. Self-validation hazard <em>fully
              acknowledged</em>. A hand-labelled third-party eval is the
              planned next step.
            </p>
          </div>
          <div className="method-body">
            <p className="full">
              The eval set was labelled before any prompt iteration, then
              held out. The model classifies each post against the gold
              labels; mismatches are recorded with the exact axis that
              failed. This catches regressions when the prompt changes —
              but <em>cannot detect blind spots in the prompt
              designer&apos;s own framing</em>.
            </p>

            <div className="eval-grid full">
              <div className="eval-card">
                <div className="label">Sentiment</div>
                <div className="value">
                  <em>{formatAccuracy(evalData?.sentiment.accuracy)}</em>%
                </div>
                <div className="sub">
                  {supportLine(evalData?.sentiment, evalData?.eval_set_size)}
                </div>
              </div>
              <div className="eval-card">
                <div className="label">Intensity</div>
                <div className="value">
                  <em>{formatAccuracy(evalData?.intensity.accuracy)}</em>%
                </div>
                <div className="sub">
                  {supportLine(evalData?.intensity, evalData?.eval_set_size)}
                </div>
              </div>
              <div className="eval-card">
                <div className="label">Target</div>
                <div className="value">
                  <em>{formatAccuracy(evalData?.target.accuracy)}</em>%
                </div>
                <div className="sub">
                  {supportLine(evalData?.target, evalData?.eval_set_size)}
                </div>
              </div>
            </div>

            <table className="eval-table full">
              <thead>
                <tr>
                  <th>Class</th>
                  <th className="num">Precision</th>
                  <th className="num">Recall</th>
                  <th className="num">F1</th>
                  <th className="num">Support</th>
                </tr>
              </thead>
              <tbody>
                {renderAxisRows("sentiment", evalData?.sentiment)}
                {renderAxisRows("intensity", evalData?.intensity)}
                {renderAxisRows("target", evalData?.target)}
              </tbody>
            </table>
          </div>
        </section>

        {/* Chapter 05 — The Misses */}
        <section id="misses" className="section">
          <div className="section-head">
            <div className="section-head-title">
              <span className="section-num">05 / 05</span>
              <h2>The Misses</h2>
            </div>
            <p className="lede">
              Most portfolios hide their failures. This one features them.
              If a pattern emerges, that&apos;s a v2 prompt revision
              waiting to happen.
            </p>
          </div>
          <div className="method-body">
            <div className="misses-strip full">
              <div className="miss-card">
                <div className="miss-text">
                  Gege you absolute monster, I haven&apos;t slept in three
                  days.
                </div>
                <div className="miss-meta">
                  <span className="pred">predicted</span> negative · high
                  <br />
                  <span className="expected">expected</span> positive · high
                  <br />
                  <em>category</em> affectionate-curse idiom
                </div>
              </div>
              <div className="miss-card">
                <div className="miss-text">Sukuna won. Cope.</div>
                <div className="miss-meta">
                  <span className="pred">predicted</span> positive · medium
                  <br />
                  <span className="expected">expected</span> negative · medium
                  <br />
                  <em>category</em> in-character power-scaling
                </div>
              </div>
              <div className="miss-card">
                <div className="miss-text">
                  Did Gege just kill the strongest with a household
                  appliance? 🙏
                </div>
                <div className="miss-meta">
                  <span className="pred">predicted</span> positive · high
                  <br />
                  <span className="expected">expected</span> negative · high
                  <br />
                  <em>category</em> sarcastic-🙏 reverse polarity
                </div>
              </div>
              <div className="miss-card">
                <div className="miss-text">
                  Yuta&apos;s character development this arc is just
                  *chef&apos;s kiss*.
                </div>
                <div className="miss-meta">
                  <span className="pred">predicted</span> positive · medium
                  <br />
                  <span className="expected">expected</span> positive · high
                  <br />
                  <em>category</em> intensity underestimated
                </div>
              </div>
            </div>

            <h3
              style={{
                gridColumn: "1 / -1",
                fontFamily: "var(--ff-display)",
                fontStyle: "italic",
                fontWeight: 400,
                fontSize: "1.6rem",
                color: "var(--bone)",
                margin: "2.5rem 0 0.75rem",
                letterSpacing: "-0.015em",
              }}
            >
              Known limitations
            </h3>
            <div className="limits full">
              <div className="limit">
                <h4>
                  The eval set was written by the same person who wrote
                  the prompt.
                </h4>
                <p>
                  Self-validation hazard. The synthetic eval guards against
                  regressions on named idioms but cannot detect blind spots
                  in the prompt designer&apos;s own framing. A hand-labelled
                  third-party eval is the planned next step.
                </p>
              </div>
              <div className="limit">
                <h4>Reddit deletes substantial bodies over time.</h4>
                <p>
                  Around chapter 236&apos;s release week, roughly{" "}
                  <strong>60% of post bodies</strong> were marked{" "}
                  <code>[removed]</code> by the time we scraped. Title-only
                  classifications are real signal but noisier.
                </p>
              </div>
              <div className="limit">
                <h4>Multi-character posts share one sentiment.</h4>
                <p>
                  A post saying &ldquo;Gojo great, Sukuna terrible&rdquo;
                  is classified once on the post as a whole, then
                  attributed to both characters at that sentiment. Noise on
                  multi-character posts is documented and accepted.
                </p>
              </div>
              <div className="limit">
                <h4>
                  Polarisation is unweighted by classifier confidence.
                </h4>
                <p>
                  A 0.6-confidence positive carries the same weight in the
                  index as a 0.95-confidence positive.
                  Confidence-weighting is on the v2 list — needs care to
                  avoid distorting the headline numbers.
                </p>
              </div>
              <div className="limit">
                <h4>The daily window is 2 days, not 1.</h4>
                <p>
                  Belt-and-suspenders against a missed cron — silver-layer
                  dedup collapses the 24-hour overlap. Visible in the cron
                  logs but invisible in the dashboard.
                </p>
              </div>
              <div className="limit">
                <h4>This is fan sentiment, not viewer sentiment.</h4>
                <p>
                  Reddit fans are a self-selected subset of the JJK
                  audience. Casual viewers don&apos;t post weekly chapter
                  reactions. The numbers are{" "}
                  <em>directionally interesting</em>, not population
                  estimates.
                </p>
              </div>
            </div>
          </div>
        </section>

        <Footer />
      </main>
    </div>
  );
}

// ─── helpers ───────────────────────────────────────────────────────

function formatAccuracy(acc: number | undefined): string {
  if (acc === undefined) return "—";
  return (acc * 100).toFixed(1);
}

function supportLine(
  axis: AxisResult | undefined,
  total: number | undefined,
): string {
  if (!axis || !total) return "—";
  const correct = Math.round(axis.accuracy * total);
  return `${correct} / ${total} correct`;
}

function renderAxisRows(name: string, axis: AxisResult | undefined) {
  return (
    <>
      <tr>
        <td className="axis" colSpan={5}>
          {name}
        </td>
      </tr>
      {axis ? (
        Object.entries(axis.per_class).map(([cls, m]) => (
          <tr key={`${name}-${cls}`}>
            <td>{cls}</td>
            <td className="num">{m.precision.toFixed(2)}</td>
            <td className="num">{m.recall.toFixed(2)}</td>
            <td className="num">{m.f1.toFixed(2)}</td>
            <td className="num dim">{m.support}</td>
          </tr>
        ))
      ) : (
        <tr>
          <td className="dim" colSpan={5}>
            (loading)
          </td>
        </tr>
      )}
    </>
  );
}
