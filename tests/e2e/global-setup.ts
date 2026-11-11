import { randomBytes } from 'node:crypto';
import { execFileSync } from 'node:child_process';

/** A fresh user in the throwaway e2e database (via the user CLI); the password lives only in this run's environment. */
export default async function globalSetup() {
  if (process.env['MONGO_DATABASE'] !== 'gate_test_e2e') throw new Error('e2e must run against gate_test_e2e');
  const password = randomBytes(18).toString('base64url');
  execFileSync('npm', ['run', '--silent', 'user', '--', 'add', 'e2e-user'], { env: { ...process.env, GATE_PASSWORD: password }, stdio: 'ignore' });
  process.env['E2E_PASSWORD'] = password;
}
