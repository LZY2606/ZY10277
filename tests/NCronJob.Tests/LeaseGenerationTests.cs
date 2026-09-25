using Microsoft.Extensions.DependencyInjection;
using Shouldly;

namespace NCronJob.Tests;

/// <summary>
/// Characterization and barrier tests for the lease/generation state machine: remove/reschedule,
/// completion and signal are interleaved precisely through barriers, asserting execution counts,
/// lease generations, exactly-once notification and final idle capacity.
/// </summary>
public class LeaseGenerationTests : JobIntegrationBase
{
    [Fact]
    public async Task RemovingARunningJobDoesNotCancelItAndCapacityReturnsToIdle()
    {
        ServiceCollection.AddSingleton<TestBarrier>();
        ServiceCollection.AddSingleton<GenerationRecorder>();
        ServiceCollection.AddNCronJob(s => s
            .AddJob<BarrierJob>(p => p.WithCronExpression(Cron.AtEveryMinute).WithName("Job"))
            .AddNotificationHandler<BarrierNotificationHandler>());

        await StartNCronJob();
        var barrier = ServiceProvider.GetRequiredService<TestBarrier>();

        FakeTimer.Advance(TimeSpan.FromMinutes(1));
        await WaitForNthOrchestrationState(ExecutionState.Running, 1);
        await barrier.Entered.WaitAsync(CancellationToken);

        var orchestrationId = Events[0].CorrelationId;
        ServiceProvider.GetRequiredService<IRuntimeJobRegistry>().RemoveJob("Job");

        // Characterization: removal cancels queued runs, but a running job is left to finish.
        Events.FilterByOrchestrationId(orchestrationId).ShouldNotContain(e => e.State == ExecutionState.Cancelled);

        barrier.Open();
        await WaitForOrchestrationCompletion(orchestrationId);

        var jobWorker = ServiceProvider.GetRequiredService<JobWorker>();
        await jobWorker.WaitForRunningJobsAsync();
        jobWorker.TotalRunningJobCount.ShouldBe(0);

        // The queue is gone, so no further execution can be scheduled.
        var manager = ServiceProvider.GetRequiredService<JobQueueManager>();
        manager.TryGetQueue(typeof(BarrierJob).FullName!, out _).ShouldBeFalse();

        FakeTimer.Advance(TimeSpan.FromMinutes(5));
        Events.Count(e => e.State == ExecutionState.Running).ShouldBe(1);
        Storage.Entries.Count(e => e == "Notified").ShouldBe(1);
    }

    [Fact]
    public async Task ReAddingSameJobIdAfterRemoveSchedulesAFreshGeneration()
    {
        ServiceCollection.AddSingleton<TestBarrier>();
        ServiceCollection.AddSingleton<GenerationRecorder>();
        ServiceCollection.AddNCronJob(s => s
            .AddJob<BarrierJob>(p => p.WithCronExpression(Cron.AtEveryMinute).WithName("Job")));

        await StartNCronJob();
        var barrier = ServiceProvider.GetRequiredService<TestBarrier>();
        var registry = ServiceProvider.GetRequiredService<IRuntimeJobRegistry>();

        FakeTimer.Advance(TimeSpan.FromMinutes(1));
        await WaitForNthOrchestrationState(ExecutionState.Running, 1);
        await barrier.Entered.WaitAsync(CancellationToken);

        registry.RemoveJob("Job");
        barrier.Open();
        await WaitForNthOrchestrationState(ExecutionState.Completed, 1);

        var registered = registry.TryRegister(
            s => s.AddJob<BarrierJob>(p => p.WithCronExpression(Cron.AtEveryMinute).WithName("Job")),
            out var exception);
        registered.ShouldBeTrue();
        exception.ShouldBeNull();

        FakeTimer.Advance(TimeSpan.FromMinutes(1));
        await WaitForNthOrchestrationState(ExecutionState.Running, 2);

        var jobWorker = ServiceProvider.GetRequiredService<JobWorker>();
        await jobWorker.WaitForRunningJobsAsync();
        jobWorker.TotalRunningJobCount.ShouldBe(0);

        var generations = ServiceProvider.GetRequiredService<GenerationRecorder>().Generations;
        generations.Count.ShouldBe(2);
        generations[0].ShouldBeGreaterThan(0);
        generations[1].ShouldBeGreaterThan(generations[0]);
    }

    [Fact]
    public async Task RescheduleBetweenLeaseAndSuccessorSchedulingKeepsTheNewSchedule()
    {
        ServiceCollection.AddSingleton<GenerationRecorder>();
        ServiceCollection.AddNCronJob(s => s
            .AddJob<GenerationRecordingJob>(p => p.WithCronExpression(Cron.AtEvery2ndMinute).WithName("Job"))
            .AddNotificationHandler<RecordingNotificationHandler>());

        var leaseCount = 0;

        await StartNCronJob();

        var jobWorker = ServiceProvider.GetRequiredService<JobWorker>();
        // Barrier: the reschedule is interleaved on the worker thread itself, exactly between the
        // lease of generation g1 and the scheduling of its successor - no cross-thread rendezvous.
        jobWorker.JobLeasedForTesting = _ =>
        {
            if (Interlocked.Increment(ref leaseCount) == 1)
            {
                ServiceProvider.GetRequiredService<IRuntimeJobRegistry>().UpdateSchedule("Job", "*/3 * * * *");
            }
        };

        // The job fires at 00:02; the worker holds the lease of generation g1.
        FakeTimer.Advance(TimeSpan.FromMinutes(2));
        await WaitForNthOrchestrationState(ExecutionState.Running, 1);

        // The stale successor of generation g1 is rejected and cancelled.
        await WaitForNthOrchestrationState(ExecutionState.Cancelled, 1);

        var manager = ServiceProvider.GetRequiredService<JobQueueManager>();
        manager.TryGetQueue(typeof(GenerationRecordingJob).FullName!, out var queue).ShouldBeTrue();
        queue.Count.ShouldBe(1);
        queue.TryPeek(out var head, out _).ShouldBeTrue();
        head.RunAt.ShouldBe(new DateTimeOffset(2000, 1, 1, 0, 3, 0, TimeSpan.Zero));

        var recorder = ServiceProvider.GetRequiredService<GenerationRecorder>();
        var firstGeneration = recorder.Generations.ShouldHaveSingleItem();
        head.Generation.ShouldBeGreaterThan(firstGeneration);

        // The new schedule fires at 00:03 and 00:06; the stale schedule would have fired at 00:04.
        FakeTimer.Advance(TimeSpan.FromMinutes(1));
        await WaitForNthOrchestrationState(ExecutionState.Running, 2);
        FakeTimer.Advance(TimeSpan.FromMinutes(3));
        await WaitForNthOrchestrationState(ExecutionState.Running, 3);

        await jobWorker.WaitForRunningJobsAsync();

        recorder.Generations.Count.ShouldBe(3);
        recorder.Generations[1].ShouldBe(head.Generation);
        recorder.Generations[2].ShouldBe(head.Generation);
        Events.Count(e => e.State == ExecutionState.Running).ShouldBe(3);
        Storage.Entries.Count(e => e == "Executed").ShouldBe(3);

        var notifications = Storage.Entries.Where(e => e.StartsWith("Notified ", StringComparison.Ordinal)).ToList();
        notifications.Count.ShouldBe(3);
        notifications.Select(e => e["Notified ".Length..]).Distinct().Count().ShouldBe(3);

        jobWorker.TotalRunningJobCount.ShouldBe(0);
    }

    [Fact]
    public async Task CapacityRecoversFromZeroWhenGlobalLimitIsRaised()
    {
        ServiceCollection.AddSingleton(new ConcurrencySettings { MaxDegreeOfParallelism = 0 });
        ServiceCollection.AddNCronJob(s => s
            .AddJob<FirstJob>(p => p.WithCronExpression(Cron.AtEveryMinute).WithName("First")));

        await StartNCronJob();

        // The run becomes due but no capacity is available, so the worker parks on the capacity signal.
        FakeTimer.Advance(TimeSpan.FromMinutes(2));

        var registered = ServiceProvider.GetRequiredService<IRuntimeJobRegistry>().TryRegister(s =>
        {
            s.WithMaxDegreeOfParallelism(1);
            s.AddJob<SecondJob>(p => p.WithCronExpression(Cron.AtEveryMinute).WithName("Second"));
        }, out var exception);
        registered.ShouldBeTrue();
        exception.ShouldBeNull();

        // Raising the limit signals the parked waiter; the queued run starts without any polling.
        await WaitForNthOrchestrationState(ExecutionState.Running, 1);

        FakeTimer.Advance(TimeSpan.FromMinutes(1));
        await WaitForNthOrchestrationState(ExecutionState.Running, 2);

        var jobWorker = ServiceProvider.GetRequiredService<JobWorker>();
        await jobWorker.WaitForRunningJobsAsync();
        jobWorker.TotalRunningJobCount.ShouldBe(0);
        Storage.Entries.ShouldContain("First");
        Storage.Entries.ShouldContain("Second");
    }

    [Fact]
    public async Task MultipleWaitersAreReleasedAsCapacityFrees()
    {
        var gate = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var concurrency = 0;
        var completions = 0;
        var maxObserved = 0;
        var sync = new object();

        async Task Body()
        {
            var current = Interlocked.Increment(ref concurrency);
            lock (sync)
            {
                maxObserved = Math.Max(maxObserved, current);
            }

            await gate.Task;
            Interlocked.Decrement(ref concurrency);
            Interlocked.Increment(ref completions);
        }

        ServiceCollection.AddNCronJob(s =>
        {
            s.WithMaxDegreeOfParallelism(1);
            s.AddJob(() => Body(), Cron.AtEveryMinute, jobName: "J1");
            s.AddJob(() => Body(), Cron.AtEveryMinute, jobName: "J2");
            s.AddJob(() => Body(), Cron.AtEveryMinute, jobName: "J3");
        });

        await StartNCronJob();

        FakeTimer.Advance(TimeSpan.FromMinutes(1));
        await WaitForNthOrchestrationState(ExecutionState.Running, 1);

        // One slot for three due runs: the other two workers wait for capacity.
        gate.SetResult();
        await WaitForNthOrchestrationState(ExecutionState.Running, 3);

        var jobWorker = ServiceProvider.GetRequiredService<JobWorker>();
        await jobWorker.WaitForRunningJobsAsync();

        completions.ShouldBe(3);
        maxObserved.ShouldBe(1);
        jobWorker.TotalRunningJobCount.ShouldBe(0);
    }

    private sealed class TestBarrier
    {
        private readonly TaskCompletionSource entered = new(TaskCreationOptions.RunContinuationsAsynchronously);
        private readonly TaskCompletionSource release = new(TaskCreationOptions.RunContinuationsAsynchronously);

        public Task Entered => entered.Task;
        public Task Release => release.Task;

        public void SignalEntered() => entered.TrySetResult();
        public void Open() => release.TrySetResult();
    }

    private sealed class GenerationRecorder
    {
        private readonly object sync = new();
        private readonly List<long> generations = [];

        public IReadOnlyList<long> Generations
        {
            get
            {
                lock (sync)
                {
                    return [.. generations];
                }
            }
        }

        public void Record(long generation)
        {
            lock (sync)
            {
                generations.Add(generation);
            }
        }
    }

    private sealed class BarrierJob(Storage storage, TestBarrier barrier, GenerationRecorder recorder) : IJob
    {
        public async Task RunAsync(IJobExecutionContext context, CancellationToken token)
        {
            recorder.Record(((JobExecutionContext)context).JobRun.Generation);
            storage.Add("Started");
            barrier.SignalEntered();
            await barrier.Release;
            storage.Add("Finished");
        }
    }

    private sealed class BarrierNotificationHandler(Storage storage) : IJobNotificationHandler<BarrierJob>
    {
        public Task HandleAsync(IJobExecutionContext context, Exception? exception, CancellationToken cancellationToken)
        {
            storage.Add("Notified");
            return Task.CompletedTask;
        }
    }

    private sealed class GenerationRecordingJob(Storage storage, GenerationRecorder recorder) : IJob
    {
        public Task RunAsync(IJobExecutionContext context, CancellationToken token)
        {
            recorder.Record(((JobExecutionContext)context).JobRun.Generation);
            storage.Add("Executed");
            return Task.CompletedTask;
        }
    }

    private sealed class RecordingNotificationHandler(Storage storage) : IJobNotificationHandler<GenerationRecordingJob>
    {
        public Task HandleAsync(IJobExecutionContext context, Exception? exception, CancellationToken cancellationToken)
        {
            storage.Add($"Notified {context.CorrelationId}");
            return Task.CompletedTask;
        }
    }

    private sealed class FirstJob(Storage storage) : IJob
    {
        public Task RunAsync(IJobExecutionContext context, CancellationToken token)
        {
            storage.Add("First");
            return Task.CompletedTask;
        }
    }

    private sealed class SecondJob(Storage storage) : IJob
    {
        public Task RunAsync(IJobExecutionContext context, CancellationToken token)
        {
            storage.Add("Second");
            return Task.CompletedTask;
        }
    }
}
