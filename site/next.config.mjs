/** @type {import('next').NextConfig} */
const nextConfig = {
  // Static export — produces a deployable bundle of HTML/JS/CSS only,
  // no server runtime needed. Vercel hosts the static files; the
  // dashboard's "backend" is DuckDB-WASM running in the user's browser.
  output: "export",

  // DuckDB-WASM ships its compiled .wasm and worker .js as static
  // assets. Next.js needs to know to serve them as raw files.
  webpack: (config) => {
    config.resolve.fallback = { ...config.resolve.fallback, fs: false };
    return config;
  },

  // Trailing slashes match Vercel's static-host conventions.
  trailingSlash: true,

  // Static export doesn't optimize images; treat them as opaque.
  images: { unoptimized: true },
};

export default nextConfig;