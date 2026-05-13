/**
 * The one personal section on the dashboard.
 *
 * Sits between the data sections and the footer. Visually anchored
 * by a massive 因 kanji backdrop ("cause" / "reason") at 0.045 opacity.
 * Single-column prose styled to feel like a letter, with a red
 * signature line beneath.
 *
 * Server component — pure prose, no client state.
 */
export function WhyThisExists() {
  return (
    <section id="why" className="why-section">
      <div className="why-kanji" aria-hidden="true">
        因
      </div>

      <div className="why-mark">— Why this exists</div>

      <h2 className="why-headline">
        A data engineering portfolio,<br />
        dressed as <em>fan analytics</em>.
      </h2>

      <div className="why-body">
        <p>
          In my day job at <em>Suzlon Energy</em>, I move data between Power BI,
          Azure Data Factory, Databricks, Snowflake, and SAP. Real systems,
          production scale — all of it under my employer&apos;s name, on my
          employer&apos;s roadmap.
        </p>
        <p>
          Then <em>Chapter 236</em> dropped. Gojo died. And r/JuJutsuKaisen lost
          its mind for a week — half the sub was eulogizing him, half was
          screaming at Gege, the memes were unhinged. I wanted to know: was
          the fandom really splitting? Could the shift be <em>measured</em>?
        </p>
        <p>
          This dashboard is the answer. Five years of fan discussion,
          character-aware sentiment, daily-refreshed, honest about where it
          gets things wrong. It&apos;s also the first piece of work in my
          portfolio that&apos;s <em>clearly mine</em> — not a deliverable, not
          a ticket. Just a system I wanted to exist.
        </p>
        <p>
          If you&apos;re hiring data engineers — yes, this is also an
          application. <em>Same thinking, different domain.</em>
        </p>
      </div>

      <div className="why-signature">
        <span className="dash" aria-hidden="true">—</span>
        <span className="name">Aaditeya Sharma</span>
        <span className="loc">Delhi NCR · May 2026</span>
      </div>
    </section>
  );
}
