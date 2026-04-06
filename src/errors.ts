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
