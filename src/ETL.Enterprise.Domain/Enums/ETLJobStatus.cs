namespace ETL.Enterprise.Domain.Enums;

/// <summary>
/// Represents the status of an ETL job
/// </summary>
public enum ETLJobStatus
{
    Created,
    Running,
    Completed,
    Failed,
    Cancelled,
    Paused
}

/// <summary>
/// Represents the log level for ETL job logs
/// </summary>
public enum ETLJobLogLevel
{
    Debug,
    Information,
    Warning,
    Error,
    Critical
}

/// <summary>
/// Represents the load strategy for data targets
/// </summary>
public enum LoadStrategy
{
    Insert,
    Update,
    Upsert,
    Delete,
    TruncateAndInsert
}

/// <summary>
/// Represents the type of transformation
/// </summary>
public enum TransformationType
{
    Copy,
    Map,
    Convert,
    Calculate,
    Filter,
    Aggregate,
    Custom
}

/// <summary>
/// Represents the error handling strategy
/// </summary>
public enum ErrorHandlingStrategy
{
    StopOnError,
    ContinueOnError,
    RetryOnError,
    LogAndContinue
}

/// <summary>
/// Represents the type of validation
/// </summary>
public enum ValidationType
{
    NotNull,
    NotEmpty,
    Range,
    Regex,
    Custom,
    ForeignKey,
    Unique
}
