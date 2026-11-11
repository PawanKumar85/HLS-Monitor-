import { useRef, useState, type FormEvent } from 'react';
import { api, AuthError } from './api';

export function ResetDatabase() {
  const dialog = useRef<HTMLDialogElement>(null);
  const [password, setPassword] = useState('');
  const [confirmation, setConfirmation] = useState('');
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState('');
  function close() {
    if (busy) return;
    dialog.current?.close(); setPassword(''); setConfirmation(''); setError('');
  }
  async function submit(event: FormEvent) {
    event.preventDefault();
    if (busy || !password || confirmation !== 'RESET') return;
    setBusy(true); setError('');
    try {
      await api('/api/me/reset-database', { method: 'POST', body: JSON.stringify({ currentPassword: password, confirmation }) });
      // Clear only monitoring canvas preferences, never unrelated browser storage.
      try { for (const key of Object.keys(localStorage)) if (/^(pipeline-canvas-|pipeline-channel-links:|workspace-)/.test(key)) localStorage.removeItem(key); } catch { /* optional storage */ }
      window.location.assign('/');
    } catch (e) { if (!(e instanceof AuthError)) setError((e as Error).message); setBusy(false); }
  }
  return <section className="panel account-reset" aria-labelledby="reset-database-heading">
    <div className="account-section"><p className="hc-eyebrow">06 / Data management</p><h3 id="reset-database-heading">Reset database data</h3><p className="account-description">Permanently remove all monitoring channels, streams, probe targets, metrics, reports, incidents and DLQ data for this installation. This affects every user. Login accounts, sessions and notification settings are preserved.</p></div>
    <div className="account-actions"><div><span className="muted">This action cannot be undone.</span><button className="btn account-reset-button" type="button" onClick={() => dialog.current?.showModal()}>Reset database data</button></div></div>
    <dialog ref={dialog} className="reset-dialog" aria-labelledby="reset-dialog-title" aria-describedby="reset-dialog-description" onCancel={event => { event.preventDefault(); close(); }} onClose={() => { if (!busy) { setPassword(''); setConfirmation(''); setError(''); } }}>
      <form onSubmit={event => void submit(event)}>
        <p className="hc-eyebrow">Permanent deletion</p><h2 id="reset-dialog-title">Reset all monitoring data?</h2>
        <p id="reset-dialog-description">All channels, streams, targets, health history, reports, incidents and Redis monitoring state will be deleted. Monitoring resumes with an empty configuration. Accounts and sessions stay available.</p>
        <fieldset disabled={busy}><legend className="visually-hidden">Confirm database reset</legend>
          <div className="field"><label htmlFor="reset-current-password">Current password</label><input id="reset-current-password" type="password" autoComplete="current-password" className="input" value={password} onChange={event => setPassword(event.target.value)} required maxLength={1024} autoFocus /></div>
          <div className="field"><label htmlFor="reset-confirmation">Type RESET to confirm</label><input id="reset-confirmation" className="input" autoComplete="off" spellCheck={false} value={confirmation} onChange={event => setConfirmation(event.target.value)} required pattern="RESET" maxLength={5} /></div>
        </fieldset>
        {error && <p className="help help--error" role="alert">{error}</p>}
        {busy && <p role="status">Pausing workers and resetting monitoring data. Keep this page open…</p>}
        <div className="row-actions"><button type="button" className="btn" disabled={busy} onClick={close}>Cancel</button><button type="submit" className="btn account-reset-button" disabled={busy || !password || confirmation !== 'RESET'}>{busy ? 'Resetting…' : 'Confirm reset'}</button></div>
      </form>
    </dialog>
  </section>;
}
