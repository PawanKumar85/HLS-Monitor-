import { defineConfig } from '@playwright/test';

/**
 * Browser smoke test: `npm run test:e2e`. Needs MongoDB and Redis (as in .env) and a built dashboard (npm run web:build).
 * Runs its own server on port 3200 against the gate_test_e2e database and the "e2e" Redis namespace; uses installed Chrome.
 */
const env = { PORT: '3200', HOST: '127.0.0.1', MONGO_DATABASE: 'gate_test_e2e', HLS_PROBE_NAMESPACE: 'e2e', STATUS_FEED_NAMESPACE: 'e2e', HLS_MONITORING_ENABLED: 'false' };
Object.assign(process.env, env); // globalSetup and the tests see the same database

export default defineConfig({
  testDir: 'tests/e2e',
  timeout: 30_000,
  workers: 1,
  globalSetup: './tests/e2e/global-setup.ts',
  globalTeardown: './tests/e2e/global-teardown.ts',
  use: { baseURL: 'http://127.0.0.1:3200', channel: 'chrome', headless: true },
  webServer: { command: 'npx tsx src/index.ts', url: 'http://127.0.0.1:3200/health', env, reuseExistingServer: false, timeout: 60_000 },
});
