export interface ManagedConnection {
  connect(): Promise<unknown>;
  disconnect(): Promise<void>;
  healthCheck(): Promise<boolean>;
}
