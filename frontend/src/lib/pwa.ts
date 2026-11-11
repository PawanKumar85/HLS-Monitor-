interface InstallPrompt extends Event {
  prompt(): Promise<void>;
  userChoice: Promise<{ outcome: 'accepted' | 'dismissed' }>;
}
let deferred: InstallPrompt | undefined;
let installed = false;
let unavailable = false;
let started = false;
const listeners = new Set<() => void>();
const emit = () => { for (const listener of listeners) listener(); };
export const subscribePwa = (listener: () => void) => { listeners.add(listener); return () => { listeners.delete(listener); }; };
export const pwaState = () => installed ? 'installed' : deferred ? 'ready' : unavailable ? 'unavailable' : 'manual';

/** Shares the existing push worker; never requests notification permission on startup. */
export function startPwa() {
  if (started) return;
  started = true;
  const standalone = window.matchMedia('(display-mode: standalone)');
  const updateInstalled = () => { installed = standalone.matches || !!(navigator as Navigator & { standalone?: boolean }).standalone; emit(); };
  updateInstalled(); standalone.addEventListener('change', updateInstalled);
  window.addEventListener('beforeinstallprompt', event => { event.preventDefault(); deferred = event as InstallPrompt; emit(); });
  window.addEventListener('appinstalled', () => { installed = true; deferred = undefined; emit(); });
  if (window.isSecureContext && 'serviceWorker' in navigator) {
    void navigator.serviceWorker.register('/sw.js', { scope: '/' }).catch(() => { unavailable = true; emit(); });
  } else { unavailable = true; emit(); }
}
export async function installPwa() {
  const prompt = deferred;
  if (!prompt) return;
  deferred = undefined; emit();
  await prompt.prompt();
  const result = await prompt.userChoice;
  if (result.outcome === 'accepted') installed = true;
  emit();
}
