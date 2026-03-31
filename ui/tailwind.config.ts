import type { Config } from "tailwindcss";

const config: Config = {
  content: ["./index.html", "./src/**/*.{js,ts,jsx,tsx}"],
  theme: {
    extend: {
      colors: {
        // Dark theme matching original app
        bg: {
          primary: "#1e1e1e",
          secondary: "#252526",
          tertiary: "#2d2d30",
          hover: "#37373d",
        },
        accent: {
          blue: "#007acc",
          green: "#4ec9b0",
          orange: "#ce9178",
          yellow: "#dcdcaa",
          red: "#f44747",
        },
        text: {
          primary: "#d4d4d4",
          secondary: "#808080",
          muted: "#6a6a6a",
        },
        border: {
          DEFAULT: "#3e3e42",
          focus: "#007acc",
        },
      },
    },
  },
  plugins: [],
};

export default config;
