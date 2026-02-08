/** @type {import('tailwindcss').Config} */
export default {
    content: [
      "./index.html",
      "./src/**/*.{js,ts,jsx,tsx}",
    ],
    theme: {
      extend: {
        colors: {
          fintech: {
            primary: '#0f172a',   // Slate 900 (Backgrounds)
            secondary: '#1e293b', // Slate 800 (Cards)
            accent: '#3b82f6',    // Blue 500 (Actions)
            success: '#10b981',   // Emerald 500 (Good Data)
            warning: '#f59e0b',   // Amber 500 (Data Issues)
            danger: '#ef4444',    // Red 500 (Critical Errors)
            text: '#f8fafc',      // Slate 50 (Main Text)
            muted: '#94a3b8',     // Slate 400 (Labels)
          }
        }
      },
    },
    plugins: [],
  }