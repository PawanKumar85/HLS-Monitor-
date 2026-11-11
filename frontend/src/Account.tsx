import { InstallApp } from './InstallApp';
import { ConfigurationTransfer } from './ConfigurationTransfer';
import { ResetDatabase } from './ResetDatabase';
import { useEffect, useState, type FormEvent } from 'react';
import { api, AuthError, type User } from './api';
import { toast } from './toast';
import { disablePush, enablePush, pushState, type PushState } from './lib/push';

export function Account({ user, onUpdated }: { user: User | null; onUpdated: (user: User) => void }) {
  const [username, setUsername] = useState(user?.username ?? '');
  const [currentPassword, setCurrentPassword] = useState('');
  const [newPassword, setNewPassword] = useState('');
  const [confirmation, setConfirmation] = useState('');
  const [showPasswords, setShowPasswords] = useState(false);
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState('');
  const [success, setSuccess] = useState('');
  useEffect(() => { setUsername(user?.username ?? ''); }, [user?.username]);
  const normalized = username.trim().toLowerCase();
  const changed = !!user && (normalized !== user.username || !!newPassword);
  async function submit(event: FormEvent) {
    event.preventDefault(); setError(''); setSuccess('');
    if (!user || !changed || busy) return;
    if (!/^[a-z0-9][a-z0-9._-]{2,63}$/.test(normalized)) { setError('Username must be 3–64 letters, digits, dots, dashes or underscores, starting with a letter or digit.'); return; }
    if (!currentPassword) { setError('Enter your current password to confirm these changes.'); return; }
    if (newPassword && newPassword.length < 12) { setError('New password must contain at least 12 characters.'); return; }
    if (newPassword !== confirmation) { setError('New password and confirmation do not match.'); return; }
    setBusy(true);
    try {
      const updated = await api<User>('/api/me', { method: 'PATCH', body: JSON.stringify({ currentPassword, ...(normalized !== user.username ? { username: normalized } : {}), ...(newPassword ? { newPassword } : {}) }) });
      const passwordChanged = !!newPassword;
      onUpdated(updated); setCurrentPassword(''); setNewPassword(''); setConfirmation('');
      setSuccess(passwordChanged ? 'Password updated. Other sessions must sign in again.' : 'Username updated. Use your new username next time you sign in.');
    } catch (e) { if (!(e instanceof AuthError)) setError((e as Error).message); }
    finally { setBusy(false); }
  }
  return <section className="account-page" aria-labelledby="account-heading">
    <div className="account-page-head"><p className="hc-eyebrow">Personal / account settings</p><h2 id="account-heading">Account</h2><p>Update your sign-in details. Changes apply only to your account.</p></div>
    {!user ? <div className="panel msg"><p>Loading your account…</p></div> : <div className="account-layout">
      <aside className="panel account-identity" aria-label="Signed-in account"><span className="account-avatar" aria-hidden="true">{user.username.slice(0, 2).toUpperCase()}</span><h3>{user.username}</h3><span className="state state--online">Signed in</span><p>Confirm your current password when changing your username or password.</p></aside>
      <div className="account-main">
      <form className="panel account-form" onSubmit={e => void submit(e)}>
        <fieldset disabled={busy}><legend className="visually-hidden">Update your account</legend>
          <div className="account-section"><p className="hc-eyebrow">01 / Profile</p><h3>Username</h3><div className="field"><label htmlFor="account-username">Username</label><input className="input" id="account-username" name="username" autoComplete="username" spellCheck={false} required minLength={3} maxLength={64} value={username} onChange={e => { setUsername(e.target.value); setSuccess(''); }} aria-describedby="account-username-help" /><p className="help" id="account-username-help">3–64 characters: letters, digits, dots, dashes or underscores. Saved in lowercase.</p></div></div>
          <div className="account-section"><p className="hc-eyebrow">02 / Security</p><h3>Password</h3><p className="account-description">Leave the new password fields empty to keep your current password.</p><div className="account-password-fields">
            <div className="field"><label htmlFor="account-new-password">New password</label><input className="input" id="account-new-password" name="newPassword" type={showPasswords ? 'text' : 'password'} autoComplete="new-password" minLength={12} maxLength={1024} value={newPassword} onChange={e => { setNewPassword(e.target.value); setSuccess(''); }} aria-describedby="account-password-help" /><p className="help" id="account-password-help">At least 12 characters. Password changes require other sessions to sign in again.</p></div>
            <div className="field"><label htmlFor="account-confirm-password">Confirm new password</label><input className="input" id="account-confirm-password" name="confirmPassword" type={showPasswords ? 'text' : 'password'} autoComplete="new-password" maxLength={1024} required={!!newPassword} value={confirmation} onChange={e => setConfirmation(e.target.value)} /></div>
          </div></div>
          <div className="account-section"><p className="hc-eyebrow">03 / Confirm changes</p><div className="field"><label htmlFor="account-current-password">Current password</label><input className="input" id="account-current-password" name="currentPassword" type={showPasswords ? 'text' : 'password'} autoComplete="current-password" required maxLength={1024} value={currentPassword} onChange={e => setCurrentPassword(e.target.value)} /></div><label className="account-show-passwords"><input type="checkbox" checked={showPasswords} onChange={e => setShowPasswords(e.target.checked)} /> Show passwords</label></div>
        </fieldset>
        <div className="account-actions">{error && <p className="help help--error" role="alert">{error}</p>}{success && <p className="account-success" role="status">{success}</p>}<div><span className="muted">{changed ? 'You have unsaved changes' : 'Your account details are up to date'}</span><button className="btn btn--primary" type="submit" disabled={busy || !changed}>{busy ? 'Saving…' : 'Save changes'}</button></div></div>
      </form>
      <InstallApp />
      <BackgroundAlerts />
      <ConfigurationTransfer />
      <ResetDatabase />
      </div>
    </div>}
  </section>;
}

const PUSH_TEXT: Record<PushState, string> = {
  unsupported: 'This browser or address cannot receive background alerts. They need HTTPS (or localhost) and a browser with push support.',
  blocked: 'Notifications are blocked for this site. Allow them in the browser’s site settings, then reload this page.',
  off: 'Alerts are not sent to this device.',
  on: 'This device gets alerts even when no tab of this site is open, as long as the browser is running.',
};

/** Turns Web Push on or off for this browser and sends a test alert. */
function BackgroundAlerts() {
  const [state, setState] = useState<PushState | null>(null);
  const [busy, setBusy] = useState(false);
  useEffect(() => { pushState().then(setState, () => setState('off')); }, []);
  async function run(action: () => Promise<PushState>, done: (s: PushState) => string) {
    setBusy(true);
    try { const next = await action(); setState(next); toast(done(next), next === 'blocked'); }
    catch (e) { if (!(e instanceof AuthError)) toast(`Could not change background alerts: ${(e as Error).message}`, true); }
    finally { setBusy(false); }
  }
  async function test() {
    setBusy(true);
    try { const { delivered } = await api<{ delivered: number }>('/api/push/test', { method: 'POST' }); toast(delivered ? `Test alert sent to ${delivered} device${delivered === 1 ? '' : 's'}.` : 'No device of yours has background alerts on.', !delivered); }
    catch (e) { if (!(e instanceof AuthError)) toast(`Could not send a test alert: ${(e as Error).message}`, true); }
    finally { setBusy(false); }
  }
  return <section className="panel account-alerts" aria-labelledby="alerts-heading">
    <div className="account-section">
    <p className="hc-eyebrow">04 / Notifications</p>
    <h3 id="alerts-heading">Background alerts</h3>
    <p className="account-description">A notification on this device for critical incidents: a root cause found, a channel down, a channel on its VOD fallback, or monitoring stopped. Another one follows when it is resolved. Delivery is best effort, so keep the dashboard and webhook as the record.</p>
    </div>
    <div className="account-actions"><div>
    <p className={`account-alerts-status${state === 'blocked' || state === 'unsupported' ? ' help--error' : ''}`} role="status">{state === 'on' && <span className="state state--online">On</span>}{state === 'off' && <span className="state state--stale">Off</span>} {state ? PUSH_TEXT[state] : 'Checking this device…'}</p>
    <div className="row-actions">
      {state === 'on'
        ? <><button className="btn" type="button" disabled={busy} onClick={() => void run(disablePush, () => 'Background alerts turned off on this device.')}>Turn off</button>
            <button className="btn btn--quiet" type="button" disabled={busy} onClick={() => void test()}>Send test alert</button></>
        : <button className="btn btn--primary" type="button" disabled={busy || state === 'unsupported' || state === 'blocked' || state === null}
            onClick={() => void run(enablePush, s => s === 'on' ? 'Background alerts are on for this device.' : s === 'blocked' ? 'Notifications were blocked. Allow them in site settings.' : 'Notifications were not allowed.')}>Turn on background alerts</button>}
    </div>
    </div></div>
  </section>;
}
