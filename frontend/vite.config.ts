import { defineConfig } from 'vite';
import react from '@vitejs/plugin-react';

// Dev: Vite serves the app on :5180 and forwards everything server-side to the backend on :3000.
// changeOrigin stays false so the backend sees the browser's own Host, which its CSRF Origin check compares against.
const backend = process.env['GATE_BACKEND'] ?? 'http://localhost:3000';
const proxy = Object.fromEntries(['/api', '/events', '/login', '/logout', '/health'].map(path => [path, { target: backend, changeOrigin: false }]));

export default defineConfig({
  root: import.meta.dirname,
  plugins: [react()],
  // 5173 is often taken by other Vite apps; strictPort fails loudly instead of silently moving to another port.
  server: { port: 5180, strictPort: true, proxy },
  build: { outDir: 'dist', emptyOutDir: true },
});
