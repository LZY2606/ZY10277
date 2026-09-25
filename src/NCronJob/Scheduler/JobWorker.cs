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
    private readonly Dictionary<string, int> runningJobCounts = [];
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
                // The signal must be taken before resolving the queue: if the queue gets replaced in between,
                // the removal completes this signal instead of the worker waiting on the new queue's signal while peeking the old queue.
                var queueChanged = jobQueueManager.WaitForChangeAsync(queueName);

                if (!jobQueueManager.TryGetQueue(queueName, out var jobQueue))
                {
                    break;
                }

                if (!jobQueue.TryPeek(out var nextJob, out var priority))
                {
                    await queueChanged.WaitAsync(cancellationToken).ConfigureAwait(false);
                    continue;
                }

                if (priority.NextRunTime > timeProvider.GetUtcNow())
                {
                    await WaitUntilOrChangeAsync(priority.NextRunTime, queueChanged, cancellationToken).ConfigureAwait(false);
                    continue;
                }

                var capacityChanged = GetCapacitySignal();
                if (!TryReserveSlot(nextJob.JobDefinition))
                {
                    await Task.WhenAny(queueChanged, capacityChanged).WaitAsync(cancellationToken).ConfigureAwait(false);
                    continue;
                }

                // The lease binds the run to the queue generation under the queue manager's lock, so a
                // remove/reschedule interleaving with the dequeue is observed as a failed lease.
                var lease = jobQueueManager.TryLease(queueName, nextJob);
                if (lease is null)
                {
                    ReleaseSlot(nextJob.JobDefinition);
                    continue;
                }

                if (!await nextJob.WaitForActivationAsync().ConfigureAwait(false))
                {
                    nextJob.NotifyStateChange(JobStateType.Cancelled);
                    CompleteLease(lease, nextJob.JobDefinition);
                    continue;
                }

                _ = StartJobProcessingAsync(nextJob, lease, cancellationToken);

                if (nextJob.TriggerType == TriggerType.Cron)
                {
                    // Test seam: allows tests to deterministically interleave remove/reschedule between
                    // the lease and the scheduling of the cron successor. Never set in production.
                    JobLeasedForTesting?.Invoke(nextJob);
                    ScheduleSuccessor(lease, priority.NextRunTime);
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

    // Test seam: invoked on the worker thread after a cron lease was taken and before its successor is
    // scheduled, allowing tests to deterministically interleave remove/reschedule with lease completion.
    internal Action<JobRun>? JobLeasedForTesting { get; set; }

    internal int TotalRunningJobCount
    {
        get
        {
            lock (slotLock)
            {
                return totalRunningJobCount;
            }
        }
    }

    internal int GetRunningJobCount(JobDefinition jobDefinition)
    {
        lock (slotLock)
        {
            runningJobCounts.TryGetValue(jobDefinition.JobFullName, out var currentCount);
            return currentCount;
        }
    }

    /// <summary>
    /// Wakes all workers waiting for capacity. Required when the global concurrency limit is raised at
    /// runtime (e.g. recovered from zero): without a signal, no slot release would ever occur to wake
    /// the waiters, which would be a missed wake-up.
    /// </summary>
    internal void SignalCapacityChanged()
    {
        TaskCompletionSource signal;

        lock (slotLock)
        {
            signal = capacitySignal;
            capacitySignal = CreateSignal();
        }

        signal.TrySetResult();
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
        await StartJobProcessingAsync(jobRun, lease: null, cancellationToken).ConfigureAwait(false);
    }

    private Task StartJobProcessingAsync(JobRun jobRun, JobQueueLease? lease, CancellationToken cancellationToken)
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
                    CompleteLease(lease, jobRun.JobDefinition);
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

    /// <summary>
    /// Completes the lease and releases the capacity slot acquired with it. A lease completes at most
    /// once, so a stale completion can never release a slot that a newer generation lease is holding.
    /// </summary>
    private void CompleteLease(JobQueueLease? lease, JobDefinition jobDefinition)
    {
        if (lease is null || lease.TryComplete())
        {
            ReleaseSlot(jobDefinition);
        }
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

    private bool TryReserveSlot(JobDefinition jobDefinition)
    {
        var maxAllowed = jobDefinition.ConcurrencyPolicy?.MaxDegreeOfParallelism ?? 1;

        lock (slotLock)
        {
            runningJobCounts.TryGetValue(jobDefinition.JobFullName, out var currentCount);

            if (currentCount >= maxAllowed || totalRunningJobCount >= concurrencySettings.MaxDegreeOfParallelism)
            {
                return false;
            }

            IncrementSlotUnsafe(jobDefinition.JobFullName, currentCount);
            return true;
        }
    }

    private void AcquireSlot(JobDefinition jobDefinition)
    {
        lock (slotLock)
        {
            runningJobCounts.TryGetValue(jobDefinition.JobFullName, out var currentCount);
            IncrementSlotUnsafe(jobDefinition.JobFullName, currentCount);
        }
    }

    private void IncrementSlotUnsafe(string jobFullName, int currentCount)
    {
        runningJobCounts[jobFullName] = currentCount + 1;
        totalRunningJobCount++;
    }

    private void ReleaseSlot(JobDefinition jobDefinition)
    {
        TaskCompletionSource signal;

        lock (slotLock)
        {
            runningJobCounts.TryGetValue(jobDefinition.JobFullName, out var currentCount);
            runningJobCounts[jobDefinition.JobFullName] = Math.Max(0, currentCount - 1);
            totalRunningJobCount = Math.Max(0, totalRunningJobCount - 1);

            signal = capacitySignal;
            capacitySignal = CreateSignal();
        }

        signal.TrySetResult();
    }

    private Task GetCapacitySignal()
    {
        lock (slotLock)
        {
            return capacitySignal.Task;
        }
    }

    public JobRun? ScheduleJob(
        JobDefinition job,
        DateTimeOffset? lastScheduledRunTime = null,
        Action<JobRun>? onRunCreated = null,
        Action<string>? onQueueCreated = null,
        JobRunActivationGate? activationGate = null)
    {
        if (!job.IsEnabled)
        {
            return null;
        }

        var run = CreateNextRun(job, lastScheduledRunTime, activationGate);
        if (run is null)
        {
            return null;
        }

        onRunCreated?.Invoke(run);
        run.NotifyStateChange(JobStateType.Scheduled);

        // Checked atomically with queue removal, so a job removed concurrently isn't brought back by a pending reschedule.
        if (!jobQueueManager.Enqueue(
                run,
                () => registry.IsRootJob(job),
                onQueueCreated))
        {
            run.NotifyStateChange(JobStateType.Cancelled);
        }

        return run;
    }

    /// <summary>
    /// Schedules the next occurrence of a cron job whose lease just fired. The successor is only
    /// enqueued when the queue is still at the lease's generation; a remove/reschedule in between
    /// already computed a fresh next due run that a stale completion must not reorder.
    /// </summary>
    private void ScheduleSuccessor(JobQueueLease lease, DateTimeOffset lastScheduledRunTime)
    {
        var job = lease.Run.JobDefinition;
        var run = CreateNextRun(job, lastScheduledRunTime, activationGate: null);
        if (run is null)
        {
            return;
        }

        run.NotifyStateChange(JobStateType.Scheduled);

        if (!jobQueueManager.TryEnqueueSuccessor(lease, run, () => job.IsEnabled && registry.IsRootJob(job)))
        {
            run.NotifyStateChange(JobStateType.Cancelled);
        }
    }

    private JobRun? CreateNextRun(
        JobDefinition job,
        DateTimeOffset? lastScheduledRunTime,
        JobRunActivationGate? activationGate)
    {
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
        return JobRun.Create(
            timeProvider,
            observer.Report,
            job,
            nextRunTime.Value,
            concurrencySettings,
            activationGate);
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
}
