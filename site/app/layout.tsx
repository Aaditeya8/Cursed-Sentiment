import type { Metadata } from "next";
import "./globals.css";

export const metadata: Metadata = {
  title: "Cursed Sentiment — A JJK Fan-Sentiment Pipeline",
  description:
    "A daily-refreshed sentiment analytics pipeline running over five years of r/JuJutsuKaisen, r/Jujutsushi, and r/Jujutsufolk discussion. Character-aware, chapter-annotated, and brutally honest about its own limitations.",
  authors: [{ name: "Aaditeya Sharma", url: "https://aaditeyas.vercel.app" }],
  openGraph: {
    title: "Cursed Sentiment",
    description:
      "What 14,000 fans really feel about Gojo Satoru — and everyone else. Five years of JJK Reddit discussion, classified by Llama, charted in your browser.",
    type: "website",
    url: "https://cursed-sentiment.vercel.app",
  },
};

export default function RootLayout({
  children,
}: {
  children: React.ReactNode;
}) {
  return (
    <html lang="en">
      <head>
        <link rel="preconnect" href="https://fonts.googleapis.com" />
        <link rel="preconnect" href="https://fonts.gstatic.com" crossOrigin="anonymous" />
        <link
          href="https://fonts.googleapis.com/css2?family=Fraunces:ital,opsz,wght@0,9..144,300..900;1,9..144,300..900&family=JetBrains+Mono:wght@300;400;500;700&family=Manrope:wght@300;400;500;700&family=Noto+Serif+JP:wght@400;700;900&display=swap"
          rel="stylesheet"
        />
      </head>
      <body>
        <div className="atmosphere" aria-hidden="true" />
        <div className="grain" aria-hidden="true" />
        <div className="sutra" aria-hidden="true">
          <div className="sutra-text">
            無下限呪術廻戦虚式茈領域展開伏魔御厨子無量空処呪霊術式反転両面宿儺特級呪物呪言虎杖悠仁五条悟伏黒恵釘崎野薔薇乙骨憂太禪院真希狗巻棘パンダ夜蛾正道家入硝子七海建人
          </div>
        </div>
        {children}
      </body>
    </html>
  );
}
