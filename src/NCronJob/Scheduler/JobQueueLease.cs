namespace NCronJob;

/// <summary>
/// Represents the exclusive right of a worker to complete a dequeued queue item.
/// A lease binds a <see cref="JobRun"/> to the <see cref="JobQueue.Generation"/> of its queue at
/// dequeue time and transitions exactly once from <c>Active</c> to <c>Completed</c>:
/// a stale completion can neither release a capacity slot that a newer generation lease is holding
/// nor reorder the next due run of a rescheduled queue.
/// </summary>
internal sealed class JobQueueLease
{
    private const int Active = 0;
    private const int Completed = 1;

    private int state = Active;

    public JobQueueLease(JobRun run, long generation)
    {
        Run = run;
        Generation = generation;
    }

    public JobRun Run { get; }

    public long Generation { get; }

    /// <summary>
    /// Transitions the lease from <c>Active</c> to <c>Completed</c>.
    /// </summary>
    /// <returns><c>true</c> for the first (and only) completion; <c>false</c> when the lease was already completed.</returns>
    public bool TryComplete() => Interlocked.Exchange(ref state, Completed) == Active;
}
