namespace NCronJob;

/// <summary>
/// A lease on a dequeued <see cref="JobRun"/>, bound to the queue generation it was taken from.
/// The holder may only complete the run (release capacity, schedule the follow-up run) against the
/// same generation; once the queue moved to a newer generation the lease is stale and its completion
/// must not affect the new generation's slots or next due run.
/// </summary>
/// <param name="Run">The dequeued run.</param>
/// <param name="Generation">The queue generation the run was leased from.</param>
internal readonly record struct JobLease(JobRun Run, long Generation);
