export type QueuebitRoleKind = 'worker' | 'coordinator';
export type QueuebitRoleStatus = 'running' | 'draining' | 'stopped';

export interface QueuebitRoleMetadata {
  [key: string]: string | number | boolean | null | undefined;
}

export interface QueuebitRoleSnapshot {
  role: QueuebitRoleKind;
  domain: string;
  identity: string;
  status: QueuebitRoleStatus;
  lastHeartbeatAt: string;
  heartbeatDeadlineAt: string;
  heartbeatTtlMs: number;
  stale: boolean;
  startedAt?: string;
  stoppedAt?: string;
  drainRequestedAt?: string;
  drainReason?: string;
  metadata?: QueuebitRoleMetadata;
}

export interface QueuebitRoleHeartbeatInput {
  role: QueuebitRoleKind;
  domain: string;
  identity: string;
  status: QueuebitRoleStatus;
  heartbeatTtlMs?: number;
  startedAt?: string;
  stoppedAt?: string;
  metadata?: QueuebitRoleMetadata;
}

export interface QueuebitRoleHeartbeatResult {
  snapshot: QueuebitRoleSnapshot;
  drainRequested: boolean;
}

export interface QueuebitRoleListOptions {
  role: QueuebitRoleKind;
  domain?: string;
  includeStale?: boolean;
  limit?: number;
}

export interface QueuebitRoleListResult {
  role: QueuebitRoleKind;
  domain: string;
  includeStale: boolean;
  now: string;
  items: QueuebitRoleSnapshot[];
}

export interface QueuebitRoleDrainRequest {
  role: QueuebitRoleKind;
  domain?: string;
  identity: string;
  reason?: string;
}

export interface QueuebitRoleUnregisterInput {
  role: QueuebitRoleKind;
  domain: string;
  identity: string;
}

export interface QueuebitRolesApi {
  heartbeat(input: QueuebitRoleHeartbeatInput): Promise<QueuebitRoleHeartbeatResult>;
  get(input: QueuebitRoleUnregisterInput): Promise<QueuebitRoleSnapshot | null>;
  list(options: QueuebitRoleListOptions): Promise<QueuebitRoleListResult>;
  requestDrain(input: QueuebitRoleDrainRequest): Promise<QueuebitRoleSnapshot>;
  unregister(input: QueuebitRoleUnregisterInput): Promise<void>;
}
