namespace NCronJob;

/// <summary>
/// Represents the internal work queue. This represents all scheduled and running CRON jobs as well as instant jobs.
/// </summary>
internal sealed class JobQueue : ObservablePriorityQueue<JobRun>
{
    public string Name { get; }

    /// <summary>
    /// The generation of this queue instance, assigned by <see cref="JobQueueManager"/> from a monotonic
    /// counter. Removing and re-adding a queue (remove/reschedule) always yields a fresh generation, so
    /// leases taken from a previous instance can be distinguished from the current one.
    /// </summary>
    public long Generation { get; }

    public JobQueue(string name, long generation) : base(new JobQueueTupleComparer())
    {
        Name = name;
        Generation = generation;
    }

    /// <summary>
    /// Adds a job entry to this instance.
    /// </summary>
    /// <param name="job">The job that will be added.</param>
    public void EnqueueForDirectExecution(JobRun job)
    {
        Enqueue(job, (job.RunAt, (int)job.Priority));
    }
}
