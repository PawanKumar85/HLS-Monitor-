import { useEffect, useState } from 'react';

type Toast = { id: number; text: string; error: boolean };
let nextId = 0;
const listeners = new Set<(t: Toast) => void>();

/** Shows a short message in the corner. Success fades after 8 s, errors after 15 s; both can be dismissed. */
export function toast(text: string, error = false): void {
  const t = { id: ++nextId, text, error };
  listeners.forEach(listen => listen(t));
}

export function Toaster() {
  const [items, setItems] = useState<Toast[]>([]);
  useEffect(() => {
    const timers = new Set<ReturnType<typeof setTimeout>>();
    const add = (t: Toast) => {
      setItems(xs => [...xs.slice(-3), t]); // at most four on screen
      timers.add(setTimeout(() => setItems(xs => xs.filter(x => x.id !== t.id)), t.error ? 15_000 : 8_000));
    };
    listeners.add(add);
    return () => { listeners.delete(add); timers.forEach(clearTimeout); };
  }, []);
  return (
    <div className="toaster" role="status" aria-live="polite">
      {items.map(t => (
        <div key={t.id} className={`toast${t.error ? ' toast--error' : ''}`}>
          <span>{t.text}</span>
          <button type="button" className="toast-x" aria-label="Dismiss message" onClick={() => setItems(xs => xs.filter(x => x.id !== t.id))}>×</button>
        </div>
      ))}
    </div>
  );
}
