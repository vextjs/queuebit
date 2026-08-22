export type QueuebitErrorCode =
  | 'QB_INTERNAL'
  | 'QB_CLI_ARGUMENT_INVALID'
  | 'QB_CLI_COMMAND_UNSUPPORTED'
  | 'QB_CLI_LOADER_FAILED'
  | 'QB_CONFIG_INVALID'
  | 'QB_CONFIG_HANDLER_NOT_REGISTERED'
  | 'QB_CONFIG_SCHEMA_KEYWORD_UNSUPPORTED'
  | 'QB_CANONICAL_INPUT_UNSUPPORTED'
  | 'QB_REDIS_CONNECTION_FAILED'
  | 'QB_REDIS_PREFLIGHT_FAILED'
  | 'QB_REDIS_CLUSTER_UNSUPPORTED'
  | 'QB_REDIS_KEY_INVALID'
  | 'QB_REDIS_SCRIPT_INVALID'
  | 'QB_REDIS_SCRIPT_EXECUTION_FAILED'
  | 'QB_JOB_INVALID'
  | 'QB_JOB_LIMIT_EXCEEDED'
  | 'QB_JOB_DEDUPLICATION_CONFLICT'
  | 'QB_JOB_NOT_FOUND'
  | 'QB_JOB_STATE_CONFLICT'
  | 'QB_BACKPRESSURE_REJECTED'
  | 'QB_BACKPRESSURE_REQUEST_TOO_LARGE'
  | 'QB_WORKER_INVALID'
  | 'QB_WORKER_STATE_CONFLICT'
  | 'QB_WORKER_DRAIN_TIMEOUT'
  | 'QB_VEXT_PLUGIN_INVALID'
  | 'QB_ROLE_INVALID'
  | 'QB_ROLE_NOT_FOUND'
  | 'QB_COORDINATOR_INVALID'
  | 'QB_COORDINATOR_STATE_CONFLICT'
  | 'QB_COORDINATOR_DRAIN_TIMEOUT'
  | 'QB_RUN_INVALID'
  | 'QB_RUN_INPUT_INVALID'
  | 'QB_RUN_NOT_FOUND'
  | 'QB_RUN_DEFINITION_NOT_FOUND'
  | 'QB_RUN_DEDUPLICATION_CONFLICT'
  | 'QB_RUN_STATE_CONFLICT'
  | 'QB_SOURCE_INVALID'
  | 'QB_SOURCE_CURSOR_NOT_ADVANCED'
  | 'QB_DISPATCH_STATE_CONFLICT'
  | 'QB_DISPATCH_LIMIT_EXCEEDED'
  | 'QB_COMPLETION_INVALID'
  | 'QB_COMPLETION_NOT_FOUND'
  | 'QB_COMPLETION_STATE_CONFLICT';

export interface QueuebitErrorOptions {
  code: QueuebitErrorCode;
  message: string;
  details?: unknown;
}

/**
 * Stable public error shape used by SDK calls, CLI JSON output, and later
 * runtime probes. Internal errors should be mapped before they cross exports.
 */
export class QueuebitError extends Error {
  readonly code: QueuebitErrorCode;
  readonly details?: unknown;

  constructor(options: QueuebitErrorOptions) {
    super(options.message);
    this.name = 'QueuebitError';
    this.code = options.code;
    this.details = options.details;
  }
}
