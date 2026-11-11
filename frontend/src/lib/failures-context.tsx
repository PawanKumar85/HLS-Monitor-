import { createContext, useContext, useEffect, useState, type ReactNode } from 'react';
import { toLogin } from '../api';
import { useHlsFailures } from './useHlsFailures';

type Failures = ReturnType<typeof useHlsFailures> & { reconnect: () => void };
const FailuresContext = createContext<Failures | null>(null);

/**
 * One failure feed (SSE) for the whole app. Dashboard and DLQ share it, so switching between them neither
 * reconnects nor reloads the newest 100 failures.
 */
export function FailuresProvider({ children }: { children: ReactNode }) {
  const [connection, setConnection] = useState(0);
  const live = useHlsFailures('/events/hls-failures', connection);
  useEffect(() => { if (live.unauthorized) toLogin(); }, [live.unauthorized]);
  return <FailuresContext.Provider value={{ ...live, reconnect: () => setConnection(n => n + 1) }}>{children}</FailuresContext.Provider>;
}

export function useFailures(): Failures {
  const value = useContext(FailuresContext);
  if (!value) throw new Error('useFailures needs <FailuresProvider>');
  return value;
}
