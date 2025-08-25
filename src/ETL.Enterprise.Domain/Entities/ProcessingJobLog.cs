using System.ComponentModel.DataAnnotations;
using ETL.Enterprise.Domain.Enums;

namespace ETL.Enterprise.Domain.Entities;

/// <summary>
/// Log entry for a processing job
/// </summary>
public class ProcessingJobLog
{
    public Guid Id { get; private set; }
    public Guid ProcessingJobId { get; private set; }
    public DateTime Timestamp { get; private set; }
    public ProcessingJobLogLevel Level { get; private set; }
    public string Message { get; private set; } = string.Empty;
    public string? Exception { get; private set; }
    public string? StackTrace { get; private set; }
    public Dictionary<string, string>? Properties { get; private set; }
    
    // Navigation property
    public ProcessingJob ProcessingJob { get; private set; } = null!;
    
    // Private constructor for EF Core
    private ProcessingJobLog() { }
    
    public ProcessingJobLog(Guid processingJobId, string message, ProcessingJobLogLevel level)
    {
        Id = Guid.NewGuid();
        ProcessingJobId = processingJobId;
        Message = message ?? throw new ArgumentNullException(nameof(message));
        Level = level;
        Timestamp = DateTime.UtcNow;
    }
    
    public ProcessingJobLog(Guid processingJobId, string message, ProcessingJobLogLevel level, Exception exception)
        : this(processingJobId, message, level)
    {
        Exception = exception.Message;
        StackTrace = exception.StackTrace;
    }
    
    public ProcessingJobLog(Guid processingJobId, string message, ProcessingJobLogLevel level, Dictionary<string, string> properties)
        : this(processingJobId, message, level)
    {
        Properties = properties;
    }
}
