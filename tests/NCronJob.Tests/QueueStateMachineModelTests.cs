using Shouldly;

namespace NCronJob.Tests;

/// <summary>
/// A small reference model for the queue state machine in <see cref="JobQueueManager"/>.
/// Every scripted transition (enqueue, lease, complete, remove/reschedule, signal) is applied to
/// both the real state machine and the model, and both must agree after every single step.
/// The tests are fully deterministic: no threads, no polling, no wall-clock waits.
/// </summary>
public sealed class QueueStateMachineModelTests
{
    private static readonly TimeProvider Time = TimeProvider.System;

    [Fact]
    public void EnqueueAssignsMonotonicGenerationsAcrossRemoveAndReadd()
    {
        using var manager = new JobQueueManager();
        var job = CreateCronJob(nameof(EnqueueAssignsMonotonicGenerationsAcrossRemoveAndReadd));

        var firstRun = CreateRun(job);
        manager.Enqueue(firstRun).ShouldBeTrue();
        firstRun.Generation.ShouldBe(1);

        var secondRun = CreateRun(job);
        manager.Enqueue(secondRun).ShouldBeTrue();
        secondRun.Generation.ShouldBe(1, "same queue generation stamps all its items");

        manager.RemoveQueue(job.JobFullName);

        var thirdRun = CreateRun(job);
        manager.Enqueue(thirdRun).ShouldBeTrue();
        thirdRun.Generation.ShouldBe(2, "re-adding a queue of the same id starts a new generation");

        manager.GetCurrentGeneration(job.JobFullName).ShouldBe(2);
    }

    [Fact]
    public void SnapshotSignalFiresOnEveryTransitionSoNoWakeUpIsLost()
    {
        using var manager = new JobQueueManager();
        var job = CreateCronJob(nameof(SnapshotSignalFiresOnEveryTransitionSoNoWakeUpIsLost));

        manager.GetQueueSnapshot(job.JobFullName).ShouldBeNull("queue does not exist yet");

        manager.Enqueue(CreateRun(job));

        var snapshot = manager.GetQueueSnapshot(job.JobFullName)!;
        snapshot.ShouldNotBeNull();
        snapshot.Generation.ShouldBe(1);
        snapshot.ChangeSignal.IsCompleted.ShouldBeFalse("no transition happened after the snapshot");

        manager.Enqueue(CreateRun(job));

        snapshot.ChangeSignal.IsCompleted.ShouldBeTrue("an enqueue after the snapshot must complete its signal");

        var nextSnapshot = manager.GetQueueSnapshot(job.JobFullName)!;
        nextSnapshot.Version.ShouldBeGreaterThan(snapshot.Version);
        nextSnapshot.ChangeSignal.IsCompleted.ShouldBeFalse();

        manager.RemoveQueue(job.JobFullName);

        nextSnapshot.ChangeSignal.IsCompleted.ShouldBeTrue("a removal after the snapshot must complete its signal");
        manager.GetQueueSnapshot(job.JobFullName).ShouldBeNull();
    }

    [Fact]
    public void StaleSnapshotCannotLeaseItemOfNewerGeneration()
    {
        using var manager = new JobQueueManager();
        var job = CreateCronJob(nameof(StaleSnapshotCannotLeaseItemOfNewerGeneration));

        var oldRun = CreateRun(job);
        manager.Enqueue(oldRun);
        var staleSnapshot = manager.GetQueueSnapshot(job.JobFullName)!;

        manager.RemoveQueue(job.JobFullName);

        var newRun = CreateRun(job);
        manager.Enqueue(newRun);

        manager.TryDequeue(staleSnapshot, newRun, out _).ShouldBeFalse(
            "a lease from a retired generation must never hand out an item of a newer generation");

        var currentSnapshot = manager.GetQueueSnapshot(job.JobFullName)!;
        manager.TryDequeue(currentSnapshot, newRun, out var lease).ShouldBeTrue();
        lease.Run.ShouldBe(newRun);
        lease.Generation.ShouldBe(newRun.Generation);
        lease.Generation.ShouldBe(currentSnapshot.Generation);
    }

    [Fact]
    public void GenerationGuardRejectsStaleRescheduleAndStaleCompletion()
    {
        // Precise interleaving: lease -> remove/reschedule -> stale completion/reschedule attempt.
        // This mirrors the worker's post-dequeue reschedule racing a runtime reschedule.
        using var manager = new JobQueueManager();
        var job = CreateCronJob(nameof(GenerationGuardRejectsStaleRescheduleAndStaleCompletion));

        var leasedRun = CreateRun(job);
        manager.Enqueue(leasedRun);
        var snapshot = manager.GetQueueSnapshot(job.JobFullName)!;
        manager.TryDequeue(snapshot, leasedRun, out var lease).ShouldBeTrue();

        // The runtime reschedules the job: the old queue dies and a new generation is born.
        manager.RemoveQueue(job.JobFullName);
        var rescheduledRun = CreateRun(job);
        manager.Enqueue(rescheduledRun);

        // The stale lease holder now completes and tries to place its follow-up run.
        manager.IsCurrentGeneration(job.JobFullName, lease.Generation).ShouldBeFalse(
            "the lease's generation was retired by the reschedule");
        var staleFollowUp = CreateRun(job);
        manager.Enqueue(staleFollowUp, () => manager.IsCurrentGeneration(job.JobFullName, lease.Generation))
            .ShouldBeFalse("a stale completion must not reorder the next due run of the new generation");

        var current = manager.GetQueueSnapshot(job.JobFullName)!;
        current.Queue.Count.ShouldBe(1);
        current.Queue.TryPeek(out var head, out _).ShouldBeTrue();
        head.ShouldBe(rescheduledRun, "only the run of the current generation may remain queued");

        // The same guard accepts a follow-up of the current generation.
        var currentRun = CreateRun(job);
        manager.Enqueue(currentRun, () => manager.IsCurrentGeneration(job.JobFullName, current.Generation))
            .ShouldBeTrue();
        current.Queue.Count.ShouldBe(2);
    }

    [Fact]
    public async Task MultipleWaitersAreAllWokenByASingleTransition()
    {
        var manager = new JobQueueManager();
        try
        {
        var job = CreateCronJob(nameof(MultipleWaitersAreAllWokenByASingleTransition));

        manager.Enqueue(CreateRun(job));
        var snapshot = manager.GetQueueSnapshot(job.JobFullName)!;

        const int waiterCount = 3;
        var waiters = Enumerable.Range(0, waiterCount)
            .Select(_ => Task.Run(async () => await snapshot.ChangeSignal))
            .ToArray();

        manager.Enqueue(CreateRun(job));

        await Task.WhenAll(waiters).WaitAsync(TimeSpan.FromSeconds(10), TestContext.Current.CancellationToken);
        waiters.ShouldAllBe(waiter => waiter.IsCompletedSuccessfully);

        // Multiple waiters on WaitUntilEmptyAsync must all observe the queue draining as well.
        var emptyWaiters = Enumerable.Range(0, waiterCount)
            .Select(_ => manager.WaitUntilEmptyAsync(job.JobFullName, TestContext.Current.CancellationToken))
            .ToArray();

        var currentSnapshot = manager.GetQueueSnapshot(job.JobFullName)!;
        while (currentSnapshot.Queue.TryPeek(out var head, out _))
        {
            manager.TryDequeue(currentSnapshot, head, out _).ShouldBeTrue();
        }

        await Task.WhenAll(emptyWaiters).WaitAsync(TimeSpan.FromSeconds(10), TestContext.Current.CancellationToken);
        }
        finally
        {
            manager.Dispose();
        }
    }

    [Fact]
    public void TransitionSequenceMatchesReferenceModel()
    {
        using var manager = new JobQueueManager();
        var job = CreateCronJob(nameof(TransitionSequenceMatchesReferenceModel));
        var model = new QueueModel();

        // Each step applies one transition to both the real state machine and the model.
        EnqueueAndVerify(manager, job, model);
        EnqueueAndVerify(manager, job, model);

        // Lease the head.
        var snapshot = manager.GetQueueSnapshot(job.JobFullName)!;
        snapshot.Queue.TryPeek(out var expectedHead, out _).ShouldBeTrue();
        manager.TryDequeue(snapshot, expectedHead, out var lease).ShouldBeTrue();
        model.Items.Dequeue().ShouldBe(expectedHead);
        lease.Generation.ShouldBe(model.Generation!.Value);
        Verify(manager, job, model);

        // Remove/reschedule retires the generation.
        manager.RemoveQueue(job.JobFullName);
        model.Generation = null;
        model.Items.Clear();
        manager.GetQueueSnapshot(job.JobFullName).ShouldBeNull();

        // Re-add: new generation, capacity accounting restarts from zero.
        EnqueueAndVerify(manager, job, model);
        model.Generation.ShouldBe(lease.Generation + 1);

        // The stale lease from the retired generation cannot complete against the new one.
        manager.IsCurrentGeneration(job.JobFullName, lease.Generation).ShouldBeFalse();
        manager.IsCurrentGeneration(job.JobFullName, model.Generation!.Value).ShouldBeTrue();

        // Lease everything of the new generation; the queue drains in enqueue order per due time.
        EnqueueAndVerify(manager, job, model);
        var currentSnapshot = manager.GetQueueSnapshot(job.JobFullName)!;
        while (currentSnapshot.Queue.TryPeek(out var head, out _))
        {
            manager.TryDequeue(currentSnapshot, head, out var drained).ShouldBeTrue();
            drained.Run.ShouldBe(model.Items.Dequeue());
            drained.Generation.ShouldBe(model.Generation.Value);
        }

        model.Items.ShouldBeEmpty();
        currentSnapshot.Queue.Count.ShouldBe(0);
    }

    private static void EnqueueAndVerify(JobQueueManager manager, JobDefinition job, QueueModel model)
    {
        var run = CreateRun(job);
        manager.Enqueue(run).ShouldBeTrue();

        model.Generation ??= run.Generation;
        run.Generation.ShouldBe(model.Generation.Value);
        model.Items.Enqueue(run);

        Verify(manager, job, model);
    }

    private static void Verify(JobQueueManager manager, JobDefinition job, QueueModel model)
    {
        var snapshot = manager.GetQueueSnapshot(job.JobFullName);
        snapshot.ShouldNotBeNull();
        snapshot.Generation.ShouldBe(model.Generation!.Value);
        snapshot.Queue.Count.ShouldBe(model.Items.Count);
        snapshot.Queue.ShouldBe(model.Items, ignoreOrder: true);
    }

    private static JobDefinition CreateCronJob(string name)
    {
        var job = JobDefinition.CreateTyped(name, typeof(DummyJob), null);
        job.UpdateWith(new JobOption { CronExpression = Cron.AtEveryMinute });
        return job;
    }

    private static JobRun CreateRun(JobDefinition job) =>
        JobRun.Create(Time, _ => { }, job, Time.GetUtcNow().AddMinutes(1), new ConcurrencySettings());

    /// <summary>
    /// The reference model: the expected state of one queue inside the state machine.
    /// </summary>
    private sealed class QueueModel
    {
        public long? Generation { get; set; }

        public Queue<JobRun> Items { get; } = new();
    }
}
