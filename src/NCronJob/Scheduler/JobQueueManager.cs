using System.Collections.Concurrent;
using System.Collections.Specialized;
using System.Diagnostics.CodeAnalysis;

namespace NCronJob;

internal sealed class JobQueueManager : IDisposable
{
    private readonly ConcurrentDictionary<string, JobQueue> jobQueues = new();
    private readonly Dictionary<string, TaskCompletionSource> queueSignals = [];
    private long nextQueueGeneration;
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

            var jobQueue = jobQueues.GetOrAdd(queueName, jt =>
            {
                isCreating = true;
                var queue = new JobQueue(jt, ++nextQueueGeneration);
                queue.CollectionChanged += CallCollectionChanged;
                queueSignals[jt] = CreateSignal();
                return queue;
            });

            run.Generation = jobQueue.Generation;
            jobQueue.EnqueueForDirectExecution(run);
        }

        if (isCreating)
        {
            onQueueCreated?.Invoke(queueName);
            QueueAdded?.Invoke(queueName);
        }

        return true;
    }

    /// <summary>
    /// Atomically dequeues <paramref name="expectedHead"/> when it is still the head of the queue and
    /// returns a lease binding the run to the queue's current generation. Returns <c>null</c> when the
    /// queue was removed or the head changed (remove/reschedule interleaved with the lease).
    /// </summary>
    public JobQueueLease? TryLease(string queueName, JobRun expectedHead)
    {
        lock (syncLock)
        {
            ObjectDisposedException.ThrowIf(IsDisposed, this);

            if (!jobQueues.TryGetValue(queueName, out var jobQueue) || !jobQueue.TryDequeueIf(expectedHead))
            {
                return null;
            }

            return new JobQueueLease(expectedHead, jobQueue.Generation);
        }
    }

    /// <summary>
    /// Enqueues the successor run of a completed cron lease, but only when the queue still exists and is
    /// at the same generation the lease was taken from. A remove/reschedule in between yields a fresh
    /// generation, so a stale completion can never reorder the next due run of a rescheduled queue.
    /// Unlike <see cref="Enqueue"/>, this never recreates a removed queue.
    /// </summary>
    /// <returns><c>false</c> when the generation check or <paramref name="canEnqueue"/> rejected the run.</returns>
    public bool TryEnqueueSuccessor(JobQueueLease lease, JobRun successor, Func<bool>? canEnqueue = null)
    {
        var queueName = successor.JobDefinition.JobFullName;

        lock (syncLock)
        {
            ObjectDisposedException.ThrowIf(IsDisposed, this);

            if (!jobQueues.TryGetValue(queueName, out var jobQueue) || jobQueue.Generation != lease.Generation)
            {
                return false;
            }

            if (canEnqueue is not null && !canEnqueue())
            {
                return false;
            }

            successor.Generation = jobQueue.Generation;
            jobQueue.EnqueueForDirectExecution(successor);
        }

        return true;
    }

    public void RemoveQueue(string queueName)
    {
        List<JobRun> cancellableRuns;

        lock (syncLock)
        {
            ObjectDisposedException.ThrowIf(IsDisposed, this);

            if (!jobQueues.TryRemove(queueName, out var jobQueue))
            {
                return;
            }

            cancellableRuns = jobQueue.Where(j => j.IsCancellable).ToList();

            jobQueue.Clear();
            jobQueue.CollectionChanged -= CallCollectionChanged;

            if (queueSignals.Remove(queueName, out var signal))
            {
                signal.TrySetResult();
            }
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
        var signals = new List<TaskCompletionSource>();

        lock (syncLock)
        {
            if (!IsDisposed)
            {
                foreach (var (queueName, jobQueue) in jobQueues.ToArray())
                {
                    var removedRuns = jobQueue.RemoveWhere(runSet.Contains);
                    var removeEmptyCreatedQueue = createdQueueSet.Contains(queueName) && jobQueue.Count == 0;

                    if (removedRuns.Count == 0 && !removeEmptyCreatedQueue)
                    {
                        continue;
                    }

                    if (jobQueue.Count == 0)
                    {
                        jobQueues.TryRemove(queueName, out _);
                        jobQueue.CollectionChanged -= CallCollectionChanged;

                        if (queueSignals.Remove(queueName, out var removedSignal))
                        {
                            signals.Add(removedSignal);
                        }
                    }
                    else if (queueSignals.TryGetValue(queueName, out var changedSignal))
                    {
                        queueSignals[queueName] = CreateSignal();
                        signals.Add(changedSignal);
                    }
                }
            }
        }

        foreach (var signal in signals)
        {
            signal.TrySetResult();
        }

        foreach (var run in runs.Where(run => run.IsCancellable))
        {
            run.NotifyStateChange(JobStateType.Cancelled);
        }
    }

    public bool TryGetQueue(string queueName, [MaybeNullWhen(false)] out JobQueue jobQueue)
    {
        ObjectDisposedException.ThrowIf(IsDisposed, this);
        return jobQueues.TryGetValue(queueName, out jobQueue);
    }

    public IEnumerable<string> GetAllJobQueueNames()
    {
        ObjectDisposedException.ThrowIf(IsDisposed, this);
        return jobQueues.Keys;
    }

    /// <summary>
    /// Returns a task that completes the next time the given queue changes or is removed.
    /// Obtain it before inspecting the queue so that no change can be missed.
    /// </summary>
    public Task WaitForChangeAsync(string queueName)
    {
        ObjectDisposedException.ThrowIf(IsDisposed, this);
        lock (syncLock)
        {
            return queueSignals.TryGetValue(queueName, out var signal) ? signal.Task : Task.CompletedTask;
        }
    }

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

                    if (!jobQueues.TryGetValue(queueName, out var queue) || queue.Count == 0)
                    {
                        return;
                    }

                    queueChanged = queueSignals[queueName].Task;
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
            foreach (var jobQueue in jobQueues.Values)
            {
                jobQueue.CollectionChanged -= CallCollectionChanged;
            }

            foreach (var signal in queueSignals.Values)
            {
                signal.TrySetResult();
            }

            jobQueues.Clear();
            queueSignals.Clear();

            IsDisposed = true;
        }
    }

    private void SignalJobQueue(string queueName)
    {
        lock (syncLock)
        {
            if (!queueSignals.TryGetValue(queueName, out var signal))
            {
                return;
            }

            queueSignals[queueName] = CreateSignal();
            signal.TrySetResult();
        }
    }

    private void CallCollectionChanged(object? sender, NotifyCollectionChangedEventArgs e)
    {
        if (sender is JobQueue jobQueue && e.Action == NotifyCollectionChangedAction.Add)
        {
            SignalJobQueue(jobQueue.Name);
        }

        CollectionChanged?.Invoke(sender, e);
    }

    private static TaskCompletionSource CreateSignal() => new(TaskCreationOptions.RunContinuationsAsynchronously);
}
