using System.Collections.Concurrent;
using Microsoft.Extensions.Logging;

namespace NCronJob;

internal sealed partial class JobWorker
{
    private readonly JobQueueManager jobQueueManager;
    private readonly JobProcessor jobProcessor;
    private readonly JobRegistry registry;
    private readonly TimeProvider timeProvider;
    private readonly JobExecutionProgressObserver observer;
    private readonly ILogger<JobWorker> logger;
    private readonly ConcurrencySettings concurrencySettings;
    private readonly Dictionary<SlotKey, int> runningJobCounts = [];
    private readonly ConcurrentDictionary<Task, byte> runningJobs = new();
    private int totalRunningJobCount;
    private TaskCompletionSource capacitySignal = CreateSignal();
#if NET9_0_OR_GREATER
    private readonly Lock slotLock = new();
#else
    private readonly object slotLock = new();
#endif

    public JobWorker(
        JobQueueManager jobQueueManager,
        JobProcessor jobProcessor,
        JobRegistry registry,
        TimeProvider timeProvider,
        ConcurrencySettings concurrencySettings,
        JobExecutionProgressObserver observer,
        ILogger<JobWorker> logger)
    {
        this.jobQueueManager = jobQueueManager;
        this.jobProcessor = jobProcessor;
        this.registry = registry;
        this.timeProvider = timeProvider;
        this.observer = observer;
        this.logger = logger;
        this.concurrencySettings = concurrencySettings;
    }

    public async Task WorkerAsync(string queueName, CancellationToken cancellationToken)
    {
        try
        {
            while (!cancellationToken.IsCancellationRequested)
            {
                // The snapshot binds the queue, its generation and its change signal atomically:
                // every transition after the snapshot completes the signal, so no wake-up can be missed.
                var snapshot = jobQueueManager.GetQueueSnapshot(queueName);
                if (snapshot is null)
                {
                    break;
                }

                if (!snapshot.Queue.TryPeek(out var nextJob, out var priority))
                {
                    await snapshot.ChangeSignal.WaitAsync(cancellationToken).ConfigureAwait(false);
                    continue;
                }

                if (priority.NextRunTime > timeProvider.GetUtcNow())
                {
                    await WaitUntilOrChangeAsync(priority.NextRunTime, snapshot.ChangeSignal, cancellationToken).ConfigureAwait(false);
                    continue;
                }

                var capacityChanged = GetCapacitySignal();
                if (!TryReserveSlot(nextJob.JobDefinition, snapshot.Generation))
                {
                    await Task.WhenAny(snapshot.ChangeSignal, capacityChanged).WaitAsync(cancellationToken).ConfigureAwait(false);
                    continue;
                }

                // The lease is only granted when the queue is still at the snapshot's generation,
                // so a concurrent remove/reschedule can never hand out an item of a newer generation.
                if (!jobQueueManager.TryDequeue(snapshot, nextJob, out var lease))
                {
                    ReleaseSlot(nextJob.JobDefinition, snapshot.Generation);
                    continue;
                }

                if (!await nextJob.WaitForActivationAsync().ConfigureAwait(false))
                {
                    nextJob.NotifyStateChange(JobStateType.Cancelled);
                    ReleaseSlot(nextJob.JobDefinition, lease.Generation);
                    continue;
                }

                _ = StartJobProcessingAsync(nextJob, lease.Generation, cancellationToken);

                if (nextJob.TriggerType == TriggerType.Cron)
                {
                    // Only the same generation may schedule the follow-up run: a stale lease must not
                    // reorder the next due run of a newer (removed/rescheduled) generation.
                    ScheduleJob(nextJob.JobDefinition, priority.NextRunTime, expectedGeneration: lease.Generation);
                }
            }
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            LogWorkerCancelled(queueName);
        }
        catch (ObjectDisposedException) when (jobQueueManager.IsDisposed)
        {
            LogJobQueueManagerDisposed();
        }
    }

    /// <summary>
    /// Completes once all jobs started by this worker, including those of already removed queues, have finished.
    /// </summary>
    public Task WaitForRunningJobsAsync() => Task.WhenAll(runningJobs.Keys);

    public async Task InvokeJob(JobRun jobRun, CancellationToken cancellationToken)
    {
        try
        {
            var delay = jobRun.RunAt - timeProvider.GetUtcNow();
            if (delay > TimeSpan.Zero)
            {
                await Task.LongDelaySafe(delay, timeProvider, cancellationToken).ConfigureAwait(false);
            }
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            jobRun.NotifyStateChange(JobStateType.Cancelled);
            return;
        }

        AcquireSlot(jobRun.JobDefinition);
        await StartJobProcessingAsync(jobRun, JobRun.UngeneratedGeneration, cancellationToken).ConfigureAwait(false);
    }

    private Task StartJobProcessingAsync(JobRun jobRun, long generation, CancellationToken cancellationToken)
    {
        Task jobTask;

        // Each run starts from a clean execution context, so it doesn't inherit ambient state (e.g. log scopes) of whoever triggered it.
        using (ExecutionContext.SuppressFlow())
        {
            jobTask = Task.Run(async () =>
            {
                try
                {
                    await jobProcessor.ProcessJobAsync(jobRun, cancellationToken).ConfigureAwait(false);
                }
                finally
                {
                    ReleaseSlot(jobRun.JobDefinition, generation);
                }
            }, CancellationToken.None);
        }

        runningJobs.TryAdd(jobTask, 0);
        jobTask.ContinueWith(
            static (completedTask, state) => ((ConcurrentDictionary<Task, byte>)state!).TryRemove(completedTask, out _),
            runningJobs,
            CancellationToken.None,
            TaskContinuationOptions.ExecuteSynchronously,
            TaskScheduler.Default);

        return jobTask;
    }

    private async Task WaitUntilOrChangeAsync(DateTimeOffset dueTime, Task queueChanged, CancellationToken cancellationToken)
    {
        var delay = dueTime - timeProvider.GetUtcNow();
        if (delay <= TimeSpan.Zero)
        {
            return;
        }

        using var delayCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        var delayTask = Task.LongDelaySafe(delay, timeProvider, delayCts.Token);

        // Time may have advanced between computing the delay and arming the timer, which would make the timer fire late.
        var completedTask = timeProvider.GetUtcNow() >= dueTime
            ? null
            : await Task.WhenAny(delayTask, queueChanged).ConfigureAwait(false);

        if (completedTask != delayTask)
        {
            await delayCts.CancelAsync().ConfigureAwait(false);
        }

        cancellationToken.ThrowIfCancellationRequested();
    }

    private bool TryReserveSlot(JobDefinition jobDefinition, long generation)
    {
        var maxAllowed = jobDefinition.ConcurrencyPolicy?.MaxDegreeOfParallelism ?? 1;

        lock (slotLock)
        {
            // Per-job capacity is scoped to the queue generation: a re-added job starts from zero
            // while runs of retired generations still count against the global limit until they finish.
            var currentCount = GetRunningCountUnsafe(jobDefinition.JobFullName, generation)
                + (generation == JobRun.UngeneratedGeneration
                    ? 0
                    : GetRunningCountUnsafe(jobDefinition.JobFullName, JobRun.UngeneratedGeneration));

            if (currentCount >= maxAllowed || totalRunningJobCount >= concurrencySettings.MaxDegreeOfParallelism)
            {
                return false;
            }

            IncrementSlotUnsafe(jobDefinition.JobFullName, generation);
            return true;
        }
    }

    private void AcquireSlot(JobDefinition jobDefinition)
    {
        lock (slotLock)
        {
            IncrementSlotUnsafe(jobDefinition.JobFullName, JobRun.UngeneratedGeneration);
        }
    }

    private void IncrementSlotUnsafe(string jobFullName, long generation)
    {
        var key = new SlotKey(jobFullName, generation);
        runningJobCounts[key] = GetRunningCountUnsafe(jobFullName, generation) + 1;
        totalRunningJobCount++;
    }

    private int GetRunningCountUnsafe(string jobFullName, long generation) =>
        runningJobCounts.TryGetValue(new SlotKey(jobFullName, generation), out var count) ? count : 0;

    private void ReleaseSlot(JobDefinition jobDefinition, long generation)
    {
        TaskCompletionSource signal;

        lock (slotLock)
        {
            // The release is paired with the lease's generation: a stale completion retires its own
            // generation's bucket and can never free a slot of a newer generation.
            var key = new SlotKey(jobDefinition.JobFullName, generation);
            var remaining = Math.Max(0, GetRunningCountUnsafe(jobDefinition.JobFullName, generation) - 1);
            if (remaining == 0)
            {
                runningJobCounts.Remove(key);
            }
            else
            {
                runningJobCounts[key] = remaining;
            }

            totalRunningJobCount = Math.Max(0, totalRunningJobCount - 1);

            signal = capacitySignal;
            capacitySignal = CreateSignal();
        }

        signal.TrySetResult();
    }

    /// <summary>
    /// A task that completes the next time any slot is released. Obtain it before inspecting the
    /// slot counts so that no release can be missed.
    /// </summary>
    internal Task GetCapacitySignal()
    {
        lock (slotLock)
        {
            return capacitySignal.Task;
        }
    }

    /// <summary>
    /// The number of running jobs of the given queue generation. Diagnostic surface for tests.
    /// </summary>
    internal int GetRunningJobCount(string jobFullName, long generation)
    {
        lock (slotLock)
        {
            return GetRunningCountUnsafe(jobFullName, generation);
        }
    }

    /// <summary>
    /// The number of physically running jobs across all generations. Diagnostic surface for tests.
    /// </summary>
    internal int GetTotalRunningJobCount()
    {
        lock (slotLock)
        {
            return totalRunningJobCount;
        }
    }

    public JobRun? ScheduleJob(
        JobDefinition job,
        DateTimeOffset? lastScheduledRunTime = null,
        Action<JobRun>? onRunCreated = null,
        Action<string>? onQueueCreated = null,
        JobRunActivationGate? activationGate = null,
        long? expectedGeneration = null)
    {
        if (!job.IsEnabled)
        {
            return null;
        }

        var utcNow = timeProvider.GetUtcNow();

        // When rescheduling after a job fires, the timer may have triggered slightly
        // before the scheduled time. Using utcNow directly could return the same cron
        // slot again, causing duplicate execution. Using the later of utcNow and the
        // last scheduled run time guarantees we always advance past the fired slot.
        var baseTime = lastScheduledRunTime > utcNow
            ? lastScheduledRunTime.Value
            : utcNow;
        var nextRunTime = job.GetNextCronOccurrence(baseTime);

        if (!nextRunTime.HasValue)
        {
            return null;
        }

        LogNextJobRun(job.Name, nextRunTime.Value);
        var run = JobRun.Create(
            timeProvider,
            observer.Report,
            job,
            nextRunTime.Value,
            concurrencySettings,
            activationGate);
        onRunCreated?.Invoke(run);
        run.NotifyStateChange(JobStateType.Scheduled);

        // Checked atomically with queue removal, so a job removed concurrently isn't brought back by a
        // pending reschedule. The generation check additionally rejects follow-up runs of a stale lease
        // after a remove/reschedule moved the queue to a newer generation.
        if (!jobQueueManager.Enqueue(
                run,
                () => registry.IsRootJob(job)
                    && (expectedGeneration is not long generation
                        || jobQueueManager.IsCurrentGeneration(job.JobFullName, generation)),
                onQueueCreated))
        {
            run.NotifyStateChange(JobStateType.Cancelled);
        }

        return run;
    }

    public void RemoveJobByName(string jobName)
    {
        RemoveJob(() => registry.RemoveByName(jobName));
    }

    public void RemoveJobByType(Type type)
    {
        RemoveJob(() => registry.RemoveByType(type));
    }

    private void RemoveJob(Func<string?> unregistrator)
    {
        var jobDefinitionFullName = unregistrator();

        if (jobDefinitionFullName is null)
        {
            return;
        }

        jobQueueManager.RemoveQueue(jobDefinitionFullName);
    }

    public void RescheduleJob(JobDefinition jobDefinition)
    {
        ArgumentNullException.ThrowIfNull(jobDefinition);

        jobQueueManager.RemoveQueue(jobDefinition.JobFullName);
        ScheduleJob(jobDefinition);
    }

    private static TaskCompletionSource CreateSignal() => new(TaskCreationOptions.RunContinuationsAsynchronously);

    /// <summary>
    /// Capacity is accounted per queue generation, so a stale completion of an old generation can
    /// never release a slot of a newer generation.
    /// </summary>
    private readonly record struct SlotKey(string JobFullName, long Generation);
}
