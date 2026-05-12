"use client";

import { useEffect, useRef } from "react";

/**
 * Pipeline scroller — v3, exact DOM-manipulation port of the v4 mockup.
 *
 * Why this version instead of v2:
 *   v2 used React useState for activeIdx + percent. React's render
 *   timing during a high-frequency scroll handler combined with
 *   SSR/CSR hydration can leave the component stuck on the wrong
 *   active stage. The mockup uses classList.toggle directly on DOM
 *   nodes (no re-render) so updates land synchronously each frame.
 *
 *   This version mirrors the mockup exactly: refs to stage and text
 *   nodes, classList.toggle for .active/.show, textContent for the
 *   progress readout. No React state changes during scroll.
 */

const STAGES = [
  { kanji: "集", num: "01", name: "Ingest",   desc: "Arctic Shift" },
  { kanji: "浄", num: "02", name: "Clean",    desc: "Bronze → Silver" },
  { kanji: "名", num: "03", name: "Resolve",  desc: "Alias dictionary" },
  { kanji: "呪", num: "04", name: "Classify", desc: "Llama 3.1 · 8B" },
  { kanji: "図", num: "05", name: "Publish",  desc: "DuckDB-WASM" },
];

const STAGE_NAMES_LC = ["ingest", "clean", "resolve", "classify", "publish"];

export function PipelineScroller() {
  const sectionRef = useRef<HTMLElement>(null);
  const stageRefs = useRef<Array<HTMLDivElement | null>>([]);
  const textRefs = useRef<Array<HTMLSpanElement | null>>([]);
  const pctRef = useRef<HTMLSpanElement>(null);
  const stageNameRef = useRef<HTMLSpanElement>(null);

  useEffect(() => {
    const section = sectionRef.current;
    if (!section) return;

    // explicit initial state: first stage active, first detail visible
    stageRefs.current.forEach((s, i) => {
      if (s) s.classList.toggle("active", i === 0);
    });
    textRefs.current.forEach((t, i) => {
      if (t) t.classList.toggle("show", i === 0);
    });
    section.style.setProperty("--track-width", "0%");
    section.style.setProperty("--glow-x", "10%");

    // reduced motion → final state, no scroll wiring
    const mqReduced = window.matchMedia("(prefers-reduced-motion: reduce)");
    if (mqReduced.matches) {
      stageRefs.current.forEach((s) => s?.classList.add("active"));
      textRefs.current.forEach((t) => t?.classList.add("show"));
      section.style.setProperty("--track-width", "80%");
      section.style.setProperty("--glow-x", "90%");
      return;
    }

    let active = false;
    let ticking = false;

    const update = () => {
      const rect = section.getBoundingClientRect();
      const total = Math.max(1, section.offsetHeight - window.innerHeight);
      const scrolled = -rect.top;
      const progress = Math.max(0, Math.min(1, scrolled / total));

      // fill spans from left:10% to left:90% via width:0%–80%
      const fillWidth = progress * 80;
      section.style.setProperty("--track-width", fillWidth + "%");
      section.style.setProperty("--glow-x", 10 + fillWidth + "%");

      // stage idx with -0.04 lookahead so each lights as the fill arrives
      const idx =
        progress >= 0.96 ? 4 :
        progress >= 0.71 ? 3 :
        progress >= 0.46 ? 2 :
        progress >= 0.21 ? 1 : 0;

      stageRefs.current.forEach((s, i) => {
        if (s) s.classList.toggle("active", i === idx);
      });
      textRefs.current.forEach((t, i) => {
        if (t) t.classList.toggle("show", i === idx);
      });

      if (pctRef.current) {
        pctRef.current.textContent = String(Math.round(progress * 100)).padStart(2, "0");
      }
      if (stageNameRef.current) {
        stageNameRef.current.textContent = STAGE_NAMES_LC[idx];
      }

      ticking = false;
    };

    // run scroll handler only when section is near the viewport
    const io = new IntersectionObserver(
      ([entry]) => {
        active = entry.isIntersecting;
        if (active) requestAnimationFrame(update);
      },
      { rootMargin: "50% 0px 50% 0px" },
    );
    io.observe(section);

    const onScroll = () => {
      if (!active || ticking) return;
      ticking = true;
      requestAnimationFrame(update);
    };

    window.addEventListener("scroll", onScroll, { passive: true });
    window.addEventListener("resize", onScroll, { passive: true });

    // initial render after mount so refs are populated and CSS vars set
    update();

    return () => {
      window.removeEventListener("scroll", onScroll);
      window.removeEventListener("resize", onScroll);
      io.disconnect();
    };
  }, []);

  return (
    <section
      id="pipeline"
      className="pipeline-section"
      ref={sectionRef}
      aria-label="Pipeline visualisation"
    >
      <div className="pipeline-pin">
        <div className="pipeline-inner">
          <div className="pipeline-head">
            <h2>
              <span className="jp">領域</span>
              <em>How a post becomes a chart.</em>
            </h2>
            <p className="lede">
              Five stages.
              <br />
              Daily cron.
              <br />
              Scroll to follow ↓
            </p>
          </div>

          <div className="pipeline-track">
            <div className="pipeline-stages">
              {STAGES.map((stage, i) => (
                <div
                  key={i}
                  className="pipeline-stage"
                  data-idx={i}
                  ref={(el) => { stageRefs.current[i] = el; }}
                >
                  <span className="stage-num">{stage.num}</span>
                  <div className="stage-node">{stage.kanji}</div>
                  <div className="stage-name">{stage.name}</div>
                  <div className="stage-desc">{stage.desc}</div>
                </div>
              ))}
            </div>
          </div>

          <div className="pipeline-detail">
            <span className="text" ref={(el) => { textRefs.current[0] = el; }}>
              Five years of fan reaction begins here.{" "}
              <strong>~13,400 posts</strong> pulled from <em>r/JuJutsuKaisen</em>,{" "}
              <em>r/Jujutsushi</em>, and <em>r/Jujutsufolk</em> via a daily{" "}
              <code>2-day rolling window</code> on Arctic Shift. Bronze layer is
              append-only parquet, partitioned by ingestion date.
            </span>
            <span className="text" ref={(el) => { textRefs.current[1] = el; }}>
              Cleaned, deduplicated, normalised. Bots filtered.{" "}
              <code>AutoModerator</code> gone. Markdown stripped. Usernames{" "}
              <strong>SHA-256-hashed</strong> with a per-deployment salt before
              they leave the silver layer.
            </span>
            <span className="text" ref={(el) => { textRefs.current[2] = el; }}>
              A hand-curated alias dictionary (<code>characters.yaml</code>) maps{" "}
              <em>&ldquo;Gojo&rdquo;</em>, <em>&ldquo;Satoru&rdquo;</em>,{" "}
              <em>&ldquo;the strongest&rdquo;</em>, and <em>六眼</em> to one
              canonical character. Three match modes for ambiguity:{" "}
              <strong>substring · word_boundary · contextual</strong>.
            </span>
            <span className="text" ref={(el) => { textRefs.current[3] = el; }}>
              Each post gets <strong>three scores</strong> from Llama 3.1 on Groq:
              sentiment polarity, engagement intensity, and whether it&apos;s about
              the character or the arc. Few-shot prompt covers JJK-specific
              idioms. Cached aggressively — <code>80 / 82 / 82%</code> on the
              holdout eval.
            </span>
            <span className="text" ref={(el) => { textRefs.current[4] = el; }}>
              Aggregated into the gold layer. Static parquet files committed to
              the repo and queried in your browser via <code>DuckDB-WASM</code>.
              <strong> The dashboard you&apos;re scrolling through</strong>{" "}
              right now. Daily cron at <code>06:00 UTC</code>, ~3 minutes
              end-to-end.
            </span>
          </div>
        </div>

        <div className="pipeline-progress">
          <span ref={pctRef}>00</span>% ·{" "}
          <span ref={stageNameRef}>ingest</span>
        </div>
      </div>
    </section>
  );
}
