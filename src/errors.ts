/** Base class for all BetterQueue errors. */
export class BetterQueueError extends Error {
  constructor(message: string) {
    super(message);
    this.name = 'BetterQueueError';
  }
}

/** Thrown when a job is dropped due to exceeding max retries. */
export class JobMaxRetriesError extends BetterQueueError {
  constructor(
    public readonly jobId: string,
    public readonly attempts: number,
    public override readonly cause: unknown,
  ) {
    super(`Job ${jobId} failed after ${attempts} attempt(s).`);
    this.name = 'JobMaxRetriesError';
  }
}

/** Thrown when schedule() is called after the limiter has been stopped. */
export class LimiterStoppedError extends BetterQueueError {
  constructor(public readonly queueId: string) {
    super(`Limiter "${queueId}" has been stopped and is no longer accepting jobs.`);
    this.name = 'LimiterStoppedError';
  }
}

/** Thrown when the WorkerPool has not been initialized before creating a limiter. */
export class WorkerPoolNotInitializedError extends BetterQueueError {
  constructor() {
    super(
      'WorkerPool has not been started. Call WorkerPool.create() before creating limiters.',
    );
    this.name = 'WorkerPoolNotInitializedError';
  }
}

/** Thrown when a Redis command fails unexpectedly. */
export class RedisCommandError extends BetterQueueError {
  constructor(command: string, cause: unknown) {
    super(`Redis command "${command}" failed: ${String(cause)}`);
    this.name = 'RedisCommandError';
    this.cause = cause;
  }
}

/**
 * Thrown when a job's closure is evicted from `pendingJobs` without having
 * been executed. This happens when the BRPOP notification was lost (e.g. Redis
 * restart / network blip) and the orphan sweeper detected the stale entry, or
 * when the WorkerPool was stopped while the job was still waiting.
 */
export class OrphanedJobError extends BetterQueueError {
  constructor(public readonly jobId: string) {
    super(
      `Job ${jobId} was evicted from the pending-jobs map without executing. ` +
      `The Redis notification was likely lost. The job may still be enqueued ` +
      `in Redis and will be retried when crash-recovery runs.`,
    );
    this.name = 'OrphanedJobError';
  }
}
