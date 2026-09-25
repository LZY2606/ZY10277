namespace NCronJob;

/// <summary>
/// An atomic, point-in-time view of a queue inside the <see cref="JobQueueManager"/> state machine.
/// The queue reference, its generation and its change signal are captured together under the queue
/// manager lock: every state transition that happens after the snapshot was taken completes
/// <see cref="ChangeSignal"/>, so a worker waiting on it can never miss a wake-up.
/// </summary>
internal sealed class QueueSnapshot(
    string queueName,
    JobQueue queue,
    long generation,
    long version,
    Task changeSignal)
{
    public string QueueName { get; } = queueName;

    public JobQueue Queue { get; } = queue;

    /// <summary>
    /// The generation of the queue at capture time. Only items stamped with this generation may be
    /// leased from <see cref="Queue"/>; after a remove/reschedule the queue moves to a higher
    /// generation and leases of this snapshot are stale.
    /// </summary>
    public long Generation { get; } = generation;

    /// <summary>
    /// The version of the queue state at capture time. Advances on every observable transition.
    /// </summary>
    public long Version { get; } = version;

    /// <summary>
    /// Completes when the queue transitions in any observable way (enqueue, removal, disposal)
    /// after this snapshot was taken.
    /// </summary>
    public Task ChangeSignal { get; } = changeSignal;
}
