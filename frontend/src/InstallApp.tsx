import { useState, useSyncExternalStore } from 'react';
import { installPwa, pwaState, subscribePwa } from './lib/pwa';

export function InstallApp({ compact = false }: { compact?: boolean }) {
  const state = useSyncExternalStore(subscribePwa, pwaState);
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState('');
  async function install() {
    setBusy(true); setError('');
    try { await installPwa(); } catch { setError('Use your browser menu to install this app.'); }
    finally { setBusy(false); }
  }
  if (compact) return state === 'ready' ? <button className="btn" type="button" disabled={busy} onClick={() => void install()}>{busy ? 'Opening…' : 'Install app'}</button> : null;
  return <section className="panel account-install" aria-labelledby="install-heading"><div className="account-section"><p className="hc-eyebrow">App / device</p><h3 id="install-heading">Install HLS Monitor</h3><p className="account-description">Open the monitor from your home screen or desktop in its own app window. Your sign-in and background notification settings stay the same.</p>
    {state === 'installed' ? <p role="status" className="account-success">You’re using the installed app.</p> : state === 'ready' ? <button className="btn btn--primary" type="button" disabled={busy} onClick={() => void install()}>{busy ? 'Opening install prompt…' : 'Install app'}</button> : <p className="help">{state === 'unavailable' ? 'Installation requires HTTPS or localhost and a browser with service-worker support.' : 'Use your browser’s Install app option. On iPhone or iPad, open this site in Safari, then Share → Add to Home Screen.'}</p>}
    {error && <p className="help help--error" role="alert">{error}</p>}<p className="help">Live health and reports need a connection. When offline, the installed app shows a reconnect page instead of cached monitoring data.</p>
  </div></section>;
}
