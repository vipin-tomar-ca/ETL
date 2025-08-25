using ETL.Enterprise.Domain.Enums;

namespace ETL.Enterprise.Domain.Entities;

/// <summary>
/// Represents a log entry for an ETL job execution
/// </summary>
public class ETLJobLog
{
    public Guid Id { get; private set; }
    
    public Guid ETLJobId { get; private set; }
    
    public DateTime Timestamp { get; private set; }
    
    public ETLJobLogLevel Level { get; private set; }
    
    public string Message { get; private set; } = string.Empty;
    
    public string? Exception { get; private set; }
    
    public string? StackTrace { get; private set; }
    
    public Dictionary<string, string>? Properties { get; private set; }
    
    // Private constructor for EF Core
    private ETLJobLog() { }
    
    public ETLJobLog(Guid etlJobId, string message, ETLJobLogLevel level)
    {
        Id = Guid.NewGuid();
        ETLJobId = etlJobId;
        Message = message ?? throw new ArgumentNullException(nameof(message));
        Level = level;
        Timestamp = DateTime.UtcNow;
    }
    
    public ETLJobLog(Guid etlJobId, string message, ETLJobLogLevel level, Exception exception)
        : this(etlJobId, message, level)
    {
        Exception = exception.Message;
        StackTrace = exception.StackTrace;
    }
    
    public ETLJobLog(Guid etlJobId, string message, ETLJobLogLevel level, Dictionary<string, string> properties)
        : this(etlJobId, message, level)
    {
        Properties = properties;
    }
    
    public void AddProperty(string key, string value)
    {
        Properties ??= new Dictionary<string, string>();
        Properties[key] = value;
    }
}
