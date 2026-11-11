import { useRef, useState, type ChangeEvent, type FormEvent } from 'react';
import { api, AuthError } from './api';

type Summary = { channels: number; probes: number; streams: number };
const MAX_BYTES = 10 * 1024 * 1024;
export function ConfigurationTransfer() {
  const fileInput = useRef<HTMLInputElement>(null);
  const dialog = useRef<HTMLDialogElement>(null);
  const [bundle, setBundle] = useState<unknown>(null);
  const [summary, setSummary] = useState<Summary | null>(null);
  const [filename, setFilename] = useState('');
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState('');
  const [password, setPassword] = useState('');
  const [confirmation, setConfirmation] = useState('');
  const [dialogError, setDialogError] = useState('');
  async function download() {
    setBusy(true); setError('');
    try {
      const exported = await api<unknown>('/api/me/configuration/export');
      const url = URL.createObjectURL(new Blob([JSON.stringify(exported, null, 2)], { type: 'application/json' }));
      const link = document.createElement('a'); link.href = url; link.download = `hls-monitor-configuration-${new Date().toISOString().slice(0, 10)}.json`;
      document.body.appendChild(link); link.click(); link.remove(); setTimeout(() => URL.revokeObjectURL(url), 1000);
    } catch (e) { if (!(e instanceof AuthError)) setError((e as Error).message); }
    finally { setBusy(false); }
  }
  async function inspect(event: ChangeEvent<HTMLInputElement>) {
    const file = event.target.files?.[0];
    setBundle(null); setSummary(null); setFilename(''); setError('');
    if (!file) return;
    if (file.size > MAX_BYTES) { setError('Choose a JSON file smaller than 10 MB.'); return; }
    setBusy(true);
    try {
      let candidate: unknown;
      try { candidate = JSON.parse(await file.text()); } catch { throw new Error('This file is not valid JSON.'); }
      const preview = await api<Summary>('/api/me/configuration/preview', { method: 'POST', body: JSON.stringify(candidate) });
      setBundle(candidate); setSummary(preview); setFilename(file.name);
    } catch (e) { if (!(e instanceof AuthError)) setError((e as Error).message); }
    finally { setBusy(false); }
  }
  function close() {
    if (busy) return;
    dialog.current?.close(); setPassword(''); setConfirmation(''); setDialogError('');
  }
  async function submit(event: FormEvent) {
    event.preventDefault();
    if (busy || !summary || !password || confirmation !== 'IMPORT') return;
    setBusy(true); setDialogError('');
    try {
      await api<Summary>('/api/me/configuration/import', { method: 'POST', body: JSON.stringify({ bundle, currentPassword: password, confirmation }) });
      window.location.assign('/workspace');
    } catch (e) { if (!(e instanceof AuthError)) setDialogError((e as Error).message); setBusy(false); }
  }
  return <section className="panel account-transfer" aria-labelledby="configuration-transfer-heading">
    <div className="account-section"><p className="hc-eyebrow">05 / Configuration backup</p><h3 id="configuration-transfer-heading">Import and export</h3>
      <p className="account-description">Back up channels, nodes, signal-path links, probe targets and stream settings as JSON. Accounts, passwords, sessions, metrics, reports and failure history are excluded.</p>
      <p className="help">The file contains full stream URLs, which may include access tokens. Store it privately. Canvas positions saved only in your browser are not included.</p>
      <div className="field"><label htmlFor="configuration-file">Import configuration file</label><input ref={fileInput} id="configuration-file" type="file" accept=".json,application/json" disabled={busy} onChange={event => void inspect(event)} /><p className="help">Up to 10 MB. Import adds new configuration; conflicting channels or streams are rejected before changes begin.</p></div>
      {summary && <p className="account-transfer-preview" role="status"><b>{filename}</b><br />{summary.channels} channels · {summary.probes} probe targets · {summary.streams} streams ready to import.</p>}
      {error && <p className="help help--error" role="alert">{error}</p>}
    </div>
    <div className="account-actions"><div><span className="muted">{busy ? 'Processing configuration…' : 'Export regularly before changing your setup.'}</span><div className="row-actions"><button className="btn" type="button" disabled={busy} onClick={() => void download()}>Export JSON</button><button className="btn btn--primary" type="button" disabled={busy || !summary} onClick={() => dialog.current?.showModal()}>Import configuration</button></div></div></div>
    <dialog ref={dialog} className="reset-dialog transfer-dialog" aria-labelledby="import-dialog-title" onCancel={event => { event.preventDefault(); close(); }} onClose={() => { if (!busy) { setPassword(''); setConfirmation(''); setDialogError(''); } }}>
      <form onSubmit={event => void submit(event)}><p className="hc-eyebrow">Confirm configuration import</p><h2 id="import-dialog-title">Add this configuration?</h2><p>{summary?.channels} channels, {summary?.probes} probe targets and {summary?.streams} streams will be added. Existing configuration stays unchanged. Imported enabled streams will begin monitoring.</p>
        <fieldset disabled={busy}><legend className="visually-hidden">Confirm import</legend><div className="field"><label htmlFor="import-password">Current password</label><input className="input" id="import-password" type="password" autoComplete="current-password" required maxLength={1024} value={password} onChange={event => setPassword(event.target.value)} autoFocus /></div><div className="field"><label htmlFor="import-confirmation">Type IMPORT to confirm</label><input className="input" id="import-confirmation" required pattern="IMPORT" maxLength={6} autoComplete="off" value={confirmation} onChange={event => setConfirmation(event.target.value)} /></div></fieldset>
        {dialogError && <p className="help help--error" role="alert">{dialogError}</p>}
        <div className="row-actions"><button className="btn" type="button" disabled={busy} onClick={close}>Cancel</button><button className="btn btn--primary" type="submit" disabled={busy || !password || confirmation !== 'IMPORT'}>{busy ? 'Importing…' : 'Confirm import'}</button></div>
      </form>
    </dialog>
  </section>;
}
