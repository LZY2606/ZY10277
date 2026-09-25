using System.Collections.Concurrent;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Shouldly;

namespace NCronJob.Tests;

/// <summary>
/// Characterization and barrier tests for the generation-based queue state machine:
/// remove/reschedule of running jobs, re-adding the same job id, capacity recovering from zero
/// and stale completions that must neither release slots of a newer generation nor reorder its
/// next due run. Interleavings are controlled through job-body barriers (gates), never through
/// polling or shortened delays.
/// </summary>
public sealed class SchedulerStateMachineTests : JobIntegrationBase
{
    [Fact]
    public async Task RemovingARunningJobCancelsTheQueuedRunExactlyOnceAndLetsTheRunningJobFinish()
    {
        var gates = new GateRegistry();
        var gate = new RunGate(1);
        gates.Add(gate);
        ServiceCollection.AddSingleton(gates);
        ServiceCollection.AddNCronJob(n => n
            .AddJob<GatedJob>(p => p.WithCronExpression(Cron.AtEveryMinute).WithName("Gated"))
            .AddNotificationHandler<GatedJobNotificationHandler>());

        await StartNCronJob();

        // The first run starts and blocks inside the job body; its follow-up run sits in the queue.
        FakeTimer.Advance(TimeSpan.FromMinutes(1));
        await gate.Started.WaitAsync(CancellationToken);

        var runtimeRegistry = ServiceProvider.GetRequiredService<IRuntimeJobRegistry>();
        runtimeRegistry.RemoveJob("Gated");

        // Cancellation semantics of removing a running job: the queued run is cancelled,
        // the already running job is left alone.
        await WaitForJobState(ExecutionState.Cancelled, name: "Gated");

        gate.Continue();
        await WaitForJobState(ExecutionState.Completed, name: "Gated");
        await WaitForCapacityAsync(() =>
            ServiceProvider.GetRequiredService<JobWorker>().GetTotalRunningJobCount() == 0);

        Storage.Entries.ShouldBe(["started 1", "finished 1", "notified"]);
        Events.Count(e => e.Name == "Gated" && e.State == ExecutionState.Cancelled)
            .ShouldBe(1, "the queued run must be cancelled exactly once");
        Events.Count(e => e.Name == "Gated" && e.State == ExecutionState.Completed).ShouldBe(1);

        var queueManager = ServiceProvider.GetRequiredService<JobQueueManager>();
        queueManager.GetAllJobQueueNames().ShouldBeEmpty();
        var queueWorker = (QueueWorker)ServiceProvider.GetRequiredService<IHostedService>();
        await queueWorker.WaitForWorkerRemovalAsync(typeof(GatedJob).FullName!, CancellationToken);
        queueWorker.GetActiveWorkerQueueNames().ShouldBeEmpty();
    }

    [Fact]
    public async Task ReAddingJobWithSameNameStartsANewGeneration()
    {
        ServiceCollection.AddNCronJob(n => n
            .AddJob<DummyJob>(p => p.WithCronExpression(Cron.AtEveryMinute).WithName("Re")));

        await StartNCronJob();

        var runtimeRegistry = ServiceProvider.GetRequiredService<IRuntimeJobRegistry>();
        var queueManager = ServiceProvider.GetRequiredService<JobQueueManager>();
        var queueName = typeof(DummyJob).FullName!;

        var firstGeneration = queueManager.GetCurrentGeneration(queueName);
        firstGeneration.ShouldNotBeNull();

        FakeTimer.Advance(TimeSpan.FromMinutes(1));
        await WaitForNthOrchestrationState(ExecutionState.Completed, 1);
        Storage.Entries.Count.ShouldBe(1);

        runtimeRegistry.RemoveJob("Re");
        queueManager.GetCurrentGeneration(queueName).ShouldBeNull("the queue died with the job");

        runtimeRegistry.TryRegister(
            builder => builder.AddJob(
                typeof(DummyJob),
                p => p.WithCronExpression(Cron.AtEveryMinute).WithName("Re")),
            out var exception).ShouldBeTrue(exception?.ToString());

        var secondGeneration = queueManager.GetCurrentGeneration(queueName);
        secondGeneration.ShouldNotBeNull();
        secondGeneration.Value.ShouldBeGreaterThan(
            firstGeneration.Value,
            "re-adding the same job id must start a new generation");

        FakeTimer.Advance(TimeSpan.FromMinutes(1));
        await WaitForNthOrchestrationState(ExecutionState.Completed, 2);

        Storage.Entries.Count.ShouldBe(2);
        await WaitForCapacityAsync(() =>
            ServiceProvider.GetRequiredService<JobWorker>().GetTotalRunningJobCount() == 0);
    }

    [Fact]
    public async Task CapacityRecoversFromZeroAndStaleCompletionDoesNotReleaseNewGenerationSlot()
    {
        var gates = new GateRegistry();
        var gate1 = new RunGate(1);
        var gate2 = new RunGate(2);
        var gate3 = new RunGate(3);
        gates.Add(gate1);
        gates.Add(gate2);
        gates.Add(gate3);
        ServiceCollection.AddSingleton(gates);
        ServiceCollection.AddNCronJob(n => n
            .AddJob<GatedJob>(p => p.WithCronExpression(Cron.AtEveryMinute).WithName("Limited"))
            .AddNotificationHandler<GatedJobNotificationHandler>());

        await StartNCronJob();

        var runtimeRegistry = ServiceProvider.GetRequiredService<IRuntimeJobRegistry>();
        var queueManager = ServiceProvider.GetRequiredService<JobQueueManager>();
        var jobWorker = ServiceProvider.GetRequiredService<JobWorker>();
        var queueName = typeof(GatedJob).FullName!;

        // Generation 1: the first run starts and blocks inside the job body, holding the only slot.
        FakeTimer.Advance(TimeSpan.FromMinutes(1));
        await gate1.Started.WaitAsync(CancellationToken);
        var firstGeneration = queueManager.GetCurrentGeneration(queueName)!.Value;
        jobWorker.GetRunningJobCount(queueName, firstGeneration).ShouldBe(1);

        // Remove and re-add the same job id while the first run is still blocked.
        runtimeRegistry.RemoveJob("Limited");
        runtimeRegistry.TryRegister(
            builder => builder.AddJob(
                typeof(GatedJob),
                p => p.WithCronExpression(Cron.AtEveryMinute).WithName("Limited")),
            out var exception).ShouldBeTrue(exception?.ToString());
        var secondGeneration = queueManager.GetCurrentGeneration(queueName)!.Value;
        secondGeneration.ShouldBeGreaterThan(firstGeneration);

        // Capacity recovers from zero in the new generation: the second run starts although
        // the first run still holds its retired generation's slot.
        FakeTimer.Advance(TimeSpan.FromMinutes(1));
        await gate2.Started.WaitAsync(CancellationToken);
        jobWorker.GetRunningJobCount(queueName, secondGeneration).ShouldBe(1);
        jobWorker.GetRunningJobCount(queueName, firstGeneration).ShouldBe(1);
        jobWorker.GetTotalRunningJobCount().ShouldBe(2);

        // The stale completion of the first run retires only its own generation's slot.
        gate1.Continue();
        await WaitForCapacityAsync(() => jobWorker.GetTotalRunningJobCount() == 1);
        jobWorker.GetRunningJobCount(queueName, firstGeneration).ShouldBe(0);
        jobWorker.GetRunningJobCount(queueName, secondGeneration)
            .ShouldBe(1, "a stale completion must not release a slot of the new generation");

        // An instant run joins the current generation and must queue behind the second run.
        ServiceProvider.GetRequiredService<IInstantJobRegistry>()
            .RunScheduledJob("Limited", TimeSpan.Zero, CancellationToken);

        gate2.Continue();
        await gate3.Started.WaitAsync(CancellationToken);
        gate3.Continue();

        await WaitForNthOrchestrationState(ExecutionState.Completed, 3);
        await WaitForCapacityAsync(() => jobWorker.GetTotalRunningJobCount() == 0);

        // The exact execution order proves that the instant run only started after the second
        // run released the only slot of the new generation: the stale completion of the first
        // run did not let it in early.
        var executions = Storage.Entries.Where(entry => entry != "notified").ToArray();
        executions.ShouldBe(["started 1", "started 2", "finished 1", "finished 2", "started 3", "finished 3"]);
        Storage.Entries.Count(entry => entry == "notified")
            .ShouldBe(3, "every executed run notifies exactly once");
        jobWorker.GetRunningJobCount(queueName, firstGeneration).ShouldBe(0);
        jobWorker.GetRunningJobCount(queueName, secondGeneration).ShouldBe(0);
    }

    /// <summary>
    /// Signal-driven wait for a capacity condition: the signal is captured before the condition is
    /// checked, so a release racing the check completes the captured signal and cannot be missed.
    /// </summary>
    private async Task WaitForCapacityAsync(Func<bool> condition)
    {
        var jobWorker = ServiceProvider.GetRequiredService<JobWorker>();
        while (true)
        {
            var capacityChanged = jobWorker.GetCapacitySignal();
            if (condition())
            {
                return;
            }

            await capacityChanged.WaitAsync(CancellationToken);
        }
    }

    [SupportsConcurrency(1)]
    private sealed class GatedJob(Storage storage, GateRegistry gates) : IJob
    {
        public async Task RunAsync(IJobExecutionContext context, CancellationToken token)
        {
            var gate = gates.Next();
            storage.Add($"started {gate.Id}");
            gate.SignalStarted();
            await gate.WaitAsync(token);
            storage.Add($"finished {gate.Id}");
        }
    }

    private sealed class GatedJobNotificationHandler(Storage storage) : IJobNotificationHandler<GatedJob>
    {
        public Task HandleAsync(
            IJobExecutionContext context,
            Exception? exception,
            CancellationToken cancellationToken)
        {
            storage.Add("notified");
            return Task.CompletedTask;
        }
    }

    /// <summary>
    /// Hands out one barrier per job run, in the order the test registered them.
    /// </summary>
    private sealed class GateRegistry
    {
        private readonly ConcurrentQueue<RunGate> pending = new();

        public void Add(RunGate gate) => pending.Enqueue(gate);

        public RunGate Next() =>
            pending.TryDequeue(out var gate)
                ? gate
                : throw new InvalidOperationException("More job runs started than gates were registered.");
    }

    /// <summary>
    /// A barrier for a single job run: the run signals that it started and then waits
    /// until the test lets it continue.
    /// </summary>
    private sealed class RunGate(int id)
    {
        private readonly TaskCompletionSource started = new(TaskCreationOptions.RunContinuationsAsynchronously);
        private readonly TaskCompletionSource continuation = new(TaskCreationOptions.RunContinuationsAsynchronously);

        public int Id { get; } = id;

        public Task Started => started.Task;

        public void SignalStarted() => started.TrySetResult();

        public Task WaitAsync(CancellationToken cancellationToken) => continuation.Task.WaitAsync(cancellationToken);

        public void Continue() => continuation.TrySetResult();
    }
}
