using System.Collections.Concurrent;
using System.Collections.Specialized;
using System.Diagnostics.CodeAnalysis;

namespace NCronJob;

/// <summary>
/// Owns the explicit queue state machine that converges enqueue, lease, dequeue, remove/reschedule
/// and the wake-up signal. All transitions happen under <see cref="syncLock"/>; user code
/// (job bodies, progress notifications) always runs outside the lock.
/// <para>
/// Per queue name the lifecycle is:
/// <code>
/// Absent --Enqueue--> Active(g) --Enqueue--> Active(g)     (item stamped with g, version++, signal fires)
/// Active(g) --TryDequeue--> Active(g)                      (item Enqueued -> Leased, lease carries g)
/// Active(g) --RemoveQueue--> Absent                        (signal fires, queued items -> Cancelled)
/// Absent --Enqueue--> Active(g+1)                          (re-add/reschedule starts a new generation)
/// </code>
/// Generations come from a monotonically increasing counter, so a stale lease or completion of an
/// old generation can never be confused with a re-created queue of the same name.
/// </para>
/// </summary>
internal sealed class JobQueueManager : IDisposable
{
    private readonly ConcurrentDictionary<string, QueueState> jobQueues = new();
    private long nextGeneration;
#if NET9_0_OR_GREATER
    private readonly Lock syncLock = new();
#else
    private readonly object syncLock = new();
#endif

    public event NotifyCollectionChangedEventHandler? CollectionChanged;
    public event Action<string>? QueueAdded;

    public bool IsDisposed { get; private set; }

    /// <summary>
    /// Adds the run to its queue, creating the queue if needed.
    /// Lookup and enqueue are atomic with respect to <see cref="RemoveQueue"/>, so a run can never end up in a removed queue.
    /// </summary>
    /// <returns><c>false</c> when <paramref name="canEnqueue"/> rejected the run.</returns>
    public bool Enqueue(
        JobRun run,
        Func<bool>? canEnqueue = null,
        Action<string>? onQueueCreated = null)
    {
        var queueName = run.JobDefinition.JobFullName;
        var isCreating = false;

        lock (syncLock)
        {
            ObjectDisposedException.ThrowIf(IsDisposed, this);

            if (canEnqueue is not null && !canEnqueue())
            {
                return false;
            }

            var state = jobQueues.GetOrAdd(queueName, jt =>
            {
                isCreating = true;
                var queue = new JobQueue(jt);
                queue.CollectionChanged += CallCollectionChanged;
                return new QueueState(queue, NextGenerationUnsafe());
            });

            state.Queue.EnqueueForDirectExecution(run);
            run.Generation = state.Generation;
            AdvanceVersionUnsafe(state);
        }

        if (isCreating)
        {
            onQueueCreated?.Invoke(queueName);
            QueueAdded?.Invoke(queueName);
        }

        return true;
    }

    public void RemoveQueue(string queueName)
    {
        List<JobRun> cancellableRuns;

        lock (syncLock)
        {
            ObjectDisposedException.ThrowIf(IsDisposed, this);

            if (!jobQueues.TryRemove(queueName, out var state))
            {
                return;
            }

            cancellableRuns = state.Queue.Where(j => j.IsCancellable).ToList();

            state.Queue.Clear();
            state.Queue.CollectionChanged -= CallCollectionChanged;
            CompleteSignalUnsafe(state);
        }

        // Progress callbacks run user code, so they must not be invoked while holding the lock.
        foreach (var run in cancellableRuns)
        {
            run.NotifyStateChange(JobStateType.Cancelled);
        }
    }

    public void RemoveRuns(
        IReadOnlyCollection<JobRun> runs,
        IReadOnlyCollection<string>? createdQueueNames = null)
    {
        if (runs.Count == 0 && createdQueueNames is not { Count: > 0 })
        {
            return;
        }

        var runSet = new HashSet<JobRun>(runs, ReferenceEqualityComparer.Instance);
        var createdQueueSet = createdQueueNames is null
            ? []
            : new HashSet<string>(createdQueueNames, StringComparer.Ordinal);

        lock (syncLock)
        {
            if (!IsDisposed)
            {
                foreach (var (queueName, state) in jobQueues.ToArray())
                {
                    var removedRuns = state.Queue.RemoveWhere(runSet.Contains);
                    var removeEmptyCreatedQueue = createdQueueSet.Contains(queueName) && state.Queue.Count == 0;

                    if (removedRuns.Count == 0 && !removeEmptyCreatedQueue)
                    {
                        continue;
                    }

                    if (state.Queue.Count == 0)
                    {
                        jobQueues.TryRemove(queueName, out _);
                        state.Queue.CollectionChanged -= CallCollectionChanged;
                        CompleteSignalUnsafe(state);
                    }
                    else
                    {
                        AdvanceVersionUnsafe(state);
                    }
                }
            }
        }

        foreach (var run in runs.Where(run => run.IsCancellable))
        {
            run.NotifyStateChange(JobStateType.Cancelled);
        }
    }

    public bool TryGetQueue(string queueName, [MaybeNullWhen(false)] out JobQueue jobQueue)
    {
        ObjectDisposedException.ThrowIf(IsDisposed, this);
        if (jobQueues.TryGetValue(queueName, out var state))
        {
            jobQueue = state.Queue;
            return true;
        }

        jobQueue = null;
        return false;
    }

    public IEnumerable<string> GetAllJobQueueNames()
    {
        ObjectDisposedException.ThrowIf(IsDisposed, this);
        return jobQueues.Keys;
    }

    /// <summary>
    /// Captures an atomic snapshot of the given queue: the queue, its generation and the signal
    /// that fires on the next transition. Returns <c>null</c> when the queue does not exist.
    /// </summary>
    public QueueSnapshot? GetQueueSnapshot(string queueName)
    {
        lock (syncLock)
        {
            ObjectDisposedException.ThrowIf(IsDisposed, this);

            return jobQueues.TryGetValue(queueName, out var state)
                ? new QueueSnapshot(queueName, state.Queue, state.Generation, state.Version, state.ChangeSignal.Task)
                : null;
        }
    }

    /// <summary>
    /// Transitions the head of the snapshotted queue from <c>Enqueued</c> to <c>Leased</c>, but only
    /// when the queue is still at the snapshot's generation. A stale snapshot can never lease an item
    /// of a newer generation.
    /// </summary>
    public bool TryDequeue(QueueSnapshot snapshot, JobRun expected, out JobLease lease)
    {
        ArgumentNullException.ThrowIfNull(snapshot);

        lock (syncLock)
        {
            ObjectDisposedException.ThrowIf(IsDisposed, this);

            if (!jobQueues.TryGetValue(snapshot.QueueName, out var state)
                || state.Generation != snapshot.Generation
                || !state.Queue.TryDequeueIf(expected))
            {
                lease = default;
                return false;
            }

            lease = new JobLease(expected, state.Generation);
            return true;
        }
    }

    /// <summary>
    /// Returns <c>true</c> when the queue currently exists and is still at <paramref name="generation"/>.
    /// Used to reject stale completions and stale reschedules of a previous generation.
    /// </summary>
    public bool IsCurrentGeneration(string queueName, long generation) =>
        jobQueues.TryGetValue(queueName, out var state) && state.Generation == generation;

    /// <summary>
    /// The current generation of the given queue, or <c>null</c> when the queue does not exist.
    /// </summary>
    internal long? GetCurrentGeneration(string queueName) =>
        jobQueues.TryGetValue(queueName, out var state) ? state.Generation : null;

    internal async Task WaitUntilEmptyAsync(string queueName, CancellationToken cancellationToken)
    {
        while (true)
        {
            var dequeued = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

            void OnCollectionChanged(object? sender, NotifyCollectionChangedEventArgs args)
            {
                if (sender is JobQueue queue
                    && queue.Name == queueName
                    && args.Action == NotifyCollectionChangedAction.Remove)
                {
                    dequeued.TrySetResult();
                }
            }

            CollectionChanged += OnCollectionChanged;
            try
            {
                Task queueChanged;
                lock (syncLock)
                {
                    ObjectDisposedException.ThrowIf(IsDisposed, this);

                    if (!jobQueues.TryGetValue(queueName, out var state) || state.Queue.Count == 0)
                    {
                        return;
                    }

                    queueChanged = state.ChangeSignal.Task;
                }

                await Task.WhenAny(dequeued.Task, queueChanged)
                    .WaitAsync(cancellationToken)
                    .ConfigureAwait(false);
            }
            finally
            {
                CollectionChanged -= OnCollectionChanged;
            }
        }
    }

    public void Dispose()
    {
        if (IsDisposed)
            return;

        lock (syncLock)
        {
            foreach (var state in jobQueues.Values)
            {
                state.Queue.CollectionChanged -= CallCollectionChanged;
                CompleteSignalUnsafe(state);
            }

            jobQueues.Clear();

            IsDisposed = true;
        }
    }

    private long NextGenerationUnsafe() => ++nextGeneration;

    /// <summary>
    /// Records an observable transition of the queue and completes the previous signal.
    /// Because the swap and the completion happen under <see cref="syncLock"/>, any signal captured
    /// before the transition is guaranteed to complete: no wake-up can be lost.
    /// </summary>
    private static void AdvanceVersionUnsafe(QueueState state)
    {
        var signal = state.ChangeSignal;
        state.Version++;
        state.ChangeSignal = CreateSignal();
        signal.TrySetResult();
    }

    /// <summary>
    /// Completes the signal of a retired queue state without handing out a replacement:
    /// the queue is gone, so every waiter must wake up and re-observe.
    /// </summary>
    private static void CompleteSignalUnsafe(QueueState state) => state.ChangeSignal.TrySetResult();

    private void CallCollectionChanged(object? sender, NotifyCollectionChangedEventArgs e)
    {
        CollectionChanged?.Invoke(sender, e);
    }

    private static TaskCompletionSource CreateSignal() => new(TaskCreationOptions.RunContinuationsAsynchronously);

    /// <summary>
    /// The mutable state of one queue generation. A new instance (with a fresh, higher generation)
    /// is created whenever a queue is re-added after removal, so generations of a re-created queue
    /// never collide with stale leases.
    /// </summary>
    private sealed class QueueState(JobQueue queue, long generation)
    {
        public JobQueue Queue { get; } = queue;

        public long Generation { get; } = generation;

        public long Version { get; set; }

        public TaskCompletionSource ChangeSignal { get; set; } = CreateSignal();
    }
}
