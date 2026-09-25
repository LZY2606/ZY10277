namespace NCronJob;

/// <summary>
/// Represents a builder for adding jobs at runtime.
/// </summary>
public interface IRuntimeJobBuilder
{
    /// <summary>
    /// Sets the maximum degree of parallelism for the job execution, i.e. the maximum number of jobs
    /// that are allowed to run in parallel. Raising the limit at runtime wakes up workers that are
    /// waiting for capacity.
    /// </summary>
    /// <param name="maxDegreeOfParallelism">The maximum degree of parallelism.</param>
    /// <returns>Returns a <see cref="IRuntimeJobBuilder"/> that allows further configuration.</returns>
    IRuntimeJobBuilder WithMaxDegreeOfParallelism(int maxDegreeOfParallelism);

    /// <summary>
    /// Adds a job to the service collection that gets executed based on the given cron expression.
    /// If a job with the same configuration is already registered, it will throw an exception.
    /// </summary>
    /// <param name="jobType">The type of the job to be added.</param>
    /// <param name="options">Configures the <see cref="JobOptionBuilder"/>, like the cron expression or parameters that get passed down.</param>
    void AddJob(Type jobType, Action<JobOptionBuilder>? options = null);

    /// <summary>
    /// Adds a job using an asynchronous anonymous delegate to the service collection that gets executed based on the given cron expression.
    /// </summary>
    /// <param name="jobDelegate">The delegate that represents the job to be executed.</param>
    /// <param name="cronExpression">The cron expression that defines when the job should be executed.</param>
    /// <param name="timeZoneInfo">The time zone information that the cron expression should be evaluated against.
    /// If not set the default time zone is UTC.
    /// </param>
    /// <param name="jobName">Sets the job name that can be used to identify and manipulate the job later on.</param>
    void AddJob(Delegate jobDelegate,
        string cronExpression,
        TimeZoneInfo? timeZoneInfo = null,
        string? jobName = null);
}
