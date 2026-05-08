import { GegeMomentsList } from "./components/GegeMomentsList";
import { HeadlineChart } from "./components/HeadlineChart";
import { HeroStats } from "./components/HeroStats";
import { PolarisationTable } from "./components/PolarisationTable";

export default function HomePage() {
  return (
    <main>
      <header className="border-b border-smoke/20 pb-12">
        <div className="font-mono text-xs uppercase tracking-wider text-smoke mb-4">
          Cursed Sentiment / volume 1
        </div>
        <h1 className="font-display italic text-headline text-bone max-w-2xl">
          What 280,000 fans really feel about Gojo Satoru.
        </h1>
        <p className="mt-6 text-smoke max-w-2xl leading-relaxed">
          A character-aware sentiment analytics pipeline over five years of r/JuJutsuKaisen discussion. Every chart on this page is recomputed daily from a fresh Reddit pull. Methodology, eval results, and known limitations are <a href="/methodology" className="text-bone underline decoration-smoke/40 underline-offset-4 hover:decoration-bone">on the methodology page</a>.
        </p>
      </header>

      <HeroStats />
      <HeadlineChart />
      <PolarisationTable />
      <GegeMomentsList />

      <footer className="mt-24 pt-12 border-t border-smoke/20 font-mono text-xs text-smoke">
        Built by <a href="https://aaditeyas.vercel.app" className="underline decoration-smoke/40 underline-offset-4 hover:text-bone">Aaditeya Sharma</a>. Source on <a href="https://github.com/Aaditeya8/Cursed-Sentiment" className="underline decoration-smoke/40 underline-offset-4 hover:text-bone">GitHub</a>.
      </footer>
    </main>
  );
}