using Shouldly;

namespace NCronJob.Tests;

/// <summary>
/// Pins the explicit queue/lease state machine of <see cref="JobQueueManager"/> against a small
/// reference model. All interleavings of enqueue/lease/complete/remove are exercised synchronously,
/// so neither timing nor polling is involved.
/// </summary>
public class SchedulerStateMachineTests
{
    private static readonly DateTimeOffset BaseTime = new(2026, 1, 1, 0, 0, 0, TimeSpan.Zero);

    [Fact]
    public void EnqueueStampsRunWithQueueGenerationAndLeaseBindsIt()
    {
        using var manager = new JobQueueManager();
        var run = CreateRun("Job", BaseTime);
        var name = run.JobDefinition.JobFullName;

        manager.Enqueue(run).ShouldBeTrue();

        run.Generation.ShouldBeGreaterThan(0);
        manager.TryGetQueue(name, out var queue).ShouldBeTrue();
        queue.Generation.ShouldBe(run.Generation);

        var lease = manager.TryLease(name, run);
        lease.ShouldNotBeNull();
        lease.Run.ShouldBe(run);
        lease.Generation.ShouldBe(run.Generation);
    }

    [Fact]
    public void LeaseFailsWhenQueueIsRemovedBetweenPeekAndLease()
    {
        using var manager = new JobQueueManager();
        var run = CreateRun("Job", BaseTime);
        var name = run.JobDefinition.JobFullName;
        manager.Enqueue(run);

        manager.TryGetQueue(name, out var queue).ShouldBeTrue();
        queue.TryPeek(out var head, out _).ShouldBeTrue();

        manager.RemoveQueue(name);

        manager.TryLease(name, head).ShouldBeNull();
    }

    [Fact]
    public void LeaseCompletesExactlyOnce()
    {
        using var manager = new JobQueueManager();
        var run = CreateRun("Job", BaseTime);
        manager.Enqueue(run);

        var lease = manager.TryLease(run.JobDefinition.JobFullName, run);
        lease.ShouldNotBeNull();
        lease.TryComplete().ShouldBeTrue();
        lease.TryComplete().ShouldBeFalse();
    }

    [Fact]
    public void SameGenerationSuccessorIsEnqueuedWithTheLeaseGeneration()
    {
        using var manager = new JobQueueManager();
        var run = CreateRun("Job", BaseTime);
        var name = run.JobDefinition.JobFullName;
        manager.Enqueue(run);
        var lease = manager.TryLease(name, run);
        lease.ShouldNotBeNull();

        var successor = CreateRun("Job", BaseTime.AddMinutes(1));
        manager.TryEnqueueSuccessor(lease, successor).ShouldBeTrue();

        successor.Generation.ShouldBe(lease.Generation);
        manager.TryGetQueue(name, out var queue).ShouldBeTrue();
        queue.Count.ShouldBe(1);
        queue.TryPeek(out var head, out _).ShouldBeTrue();
        head.ShouldBe(successor);
    }

    [Fact]
    public void StaleLeaseCannotEnqueueSuccessorAfterRemoveAndReAdd()
    {
        using var manager = new JobQueueManager();
        var run = CreateRun("Job", BaseTime);
        var name = run.JobDefinition.JobFullName;
        manager.Enqueue(run);
        var lease = manager.TryLease(name, run);
        lease.ShouldNotBeNull();

        manager.RemoveQueue(name);
        var rescheduled = CreateRun("Job", BaseTime.AddMinutes(5));
        manager.Enqueue(rescheduled);
        rescheduled.Generation.ShouldBeGreaterThan(lease.Generation);

        var staleSuccessor = CreateRun("Job", BaseTime.AddMinutes(10));
        manager.TryEnqueueSuccessor(lease, staleSuccessor).ShouldBeFalse();

        manager.TryGetQueue(name, out var queue).ShouldBeTrue();
        queue.Count.ShouldBe(1);
        queue.TryPeek(out var head, out _).ShouldBeTrue();
        head.ShouldBe(rescheduled);
    }

    [Fact]
    public void RemoveRunsKeepsGenerationOfSurvivingQueue()
    {
        using var manager = new JobQueueManager();
        var run1 = CreateRun("Job", BaseTime);
        var run2 = CreateRun("Job", BaseTime.AddMinutes(1));
        var name = run1.JobDefinition.JobFullName;
        manager.Enqueue(run1);
        manager.Enqueue(run2);
        manager.TryGetQueue(name, out var queue).ShouldBeTrue();
        var generation = queue.Generation;

        manager.RemoveRuns([run1]);

        manager.TryGetQueue(name, out var surviving).ShouldBeTrue();
        surviving.Generation.ShouldBe(generation);
        surviving.Count.ShouldBe(1);

        manager.RemoveRuns([run2]);

        manager.TryGetQueue(name, out _).ShouldBeFalse();

        var run3 = CreateRun("Job", BaseTime.AddMinutes(2));
        manager.Enqueue(run3);
        run3.Generation.ShouldBeGreaterThan(generation);
    }

    [Fact]
    public async Task RemoveQueueSignalsAllWaiters()
    {
        using var manager = new JobQueueManager();
        var run = CreateRun("Job", BaseTime);
        var name = run.JobDefinition.JobFullName;
        manager.Enqueue(run);

        var waiter1 = manager.WaitForChangeAsync(name);
        var waiter2 = manager.WaitForChangeAsync(name);
        waiter1.IsCompleted.ShouldBeFalse();
        waiter2.IsCompleted.ShouldBeFalse();

        manager.RemoveQueue(name);

        await Task.WhenAll(waiter1, waiter2).WaitAsync(TestContext.Current.CancellationToken);
    }

    [Fact]
    public async Task EnqueueSignalsWaitersOfTheSameQueue()
    {
        using var manager = new JobQueueManager();
        var run1 = CreateRun("Job", BaseTime);
        var name = run1.JobDefinition.JobFullName;
        manager.Enqueue(run1);

        var waiter = manager.WaitForChangeAsync(name);
        waiter.IsCompleted.ShouldBeFalse();

        manager.Enqueue(CreateRun("Job", BaseTime.AddMinutes(1)));

        await waiter.WaitAsync(TestContext.Current.CancellationToken);
    }

    [Fact]
    public void InterleavedOperationsMatchTheReferenceModel()
    {
        using var manager = new JobQueueManager();
        var model = new ReferenceQueueModel();
        var random = new DeterministicSequence(1281);
        var runCount = 0;
        JobQueueLease? lease = null;
        long? modelLeaseGeneration = null;
        var name = "";

        for (var step = 0; step < 500; step++)
        {
            switch (random.Next(4))
            {
                case 0:
                {
                    var run = CreateRun("Job", BaseTime.AddMinutes(runCount++));
                    name = run.JobDefinition.JobFullName;
                    manager.Enqueue(run).ShouldBeTrue();
                    model.Enqueue(run);
                    run.Generation.ShouldBe(model.Generation);
                    break;
                }
                case 1:
                {
                    var head = TryHead(manager, name);
                    var actualLease = head is null ? null : manager.TryLease(name, head);
                    var modelLease = model.Lease();
                    (actualLease is null).ShouldBe(modelLease is null);
                    if (actualLease is not null && modelLease is not null)
                    {
                        actualLease.Run.ShouldBe(modelLease.Value.Run);
                        actualLease.Generation.ShouldBe(modelLease.Value.Generation);
                        lease = actualLease;
                        modelLeaseGeneration = modelLease.Value.Generation;
                    }

                    break;
                }
                case 2:
                {
                    if (lease is null || modelLeaseGeneration is null)
                    {
                        break;
                    }

                    var successor = CreateRun("Job", BaseTime.AddMinutes(runCount++));
                    var accepted = manager.TryEnqueueSuccessor(lease, successor);
                    accepted.ShouldBe(model.CompleteSuccessor(modelLeaseGeneration.Value, successor));
                    if (accepted)
                    {
                        successor.Generation.ShouldBe(model.Generation);
                    }

                    lease = null;
                    modelLeaseGeneration = null;
                    break;
                }
                case 3:
                {
                    if (name.Length > 0)
                    {
                        manager.RemoveQueue(name);
                        model.Remove();
                    }

                    break;
                }
            }

            AssertAgreement(manager, model, name);
        }
    }

    private static JobRun? TryHead(JobQueueManager manager, string name)
        => name.Length > 0 && manager.TryGetQueue(name, out var queue) && queue.TryPeek(out var head, out _)
            ? head
            : null;

    private static void AssertAgreement(JobQueueManager manager, ReferenceQueueModel model, string name)
    {
        manager.TryGetQueue(name, out var queue).ShouldBe(model.Exists);
        if (!model.Exists)
        {
            return;
        }

        queue.Count.ShouldBe(model.Count);
        queue.Generation.ShouldBe(model.Generation);
    }

    private static JobRun CreateRun(string jobName, DateTimeOffset runAt)
        => JobRun.Create(
            TimeProvider.System,
            _ => { },
            JobDefinition.CreateUntyped(jobName, () => { }),
            runAt);

    /// <summary>
    /// A tiny deterministic generator (xorshift) so the interleaving permutation is reproducible.
    /// </summary>
    private sealed class DeterministicSequence(uint seed)
    {
        private uint state = seed == 0 ? 1u : seed;

        public int Next(int exclusiveUpperBound)
        {
            state ^= state << 13;
            state ^= state >> 17;
            state ^= state << 5;
            return (int)(state % (uint)exclusiveUpperBound);
        }
    }

    /// <summary>
    /// The reference model of the queue state machine: a queue exists once enqueued into, is discarded
    /// by remove, and every (re)creation yields a fresh generation. A lease binds the dequeued run to
    /// the current generation; a successor can only be enqueued against the lease's own generation.
    /// </summary>
    private sealed class ReferenceQueueModel
    {
        private readonly Queue<JobRun> items = new();
        private long counter;

        public bool Exists { get; private set; }
        public long Generation { get; private set; }
        public int Count => items.Count;

        public void Enqueue(JobRun run)
        {
            if (!Exists)
            {
                Exists = true;
                Generation = ++counter;
            }

            items.Enqueue(run);
        }

        public (JobRun Run, long Generation)? Lease()
            => Exists && items.Count > 0 ? (items.Dequeue(), Generation) : null;

        public bool CompleteSuccessor(long leaseGeneration, JobRun successor)
        {
            if (!Exists || leaseGeneration != Generation)
            {
                return false;
            }

            items.Enqueue(successor);
            return true;
        }

        public void Remove()
        {
            Exists = false;
            items.Clear();
        }
    }
}
