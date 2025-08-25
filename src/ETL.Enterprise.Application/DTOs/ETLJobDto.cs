using ETL.Enterprise.Domain.Enums;
using ETL.Enterprise.Application.DTOs;

namespace ETL.Enterprise.Application.DTOs;

/// <summary>
/// Data Transfer Object for ETL Job
/// </summary>
public class ETLJobDto
{
    public Guid Id { get; set; }
    public string Name { get; set; } = string.Empty;
    public string Description { get; set; } = string.Empty;
    public ETLJobStatus Status { get; set; }
    public DateTime CreatedAt { get; set; }
    public DateTime? StartedAt { get; set; }
    public DateTime? CompletedAt { get; set; }
    public int RecordsProcessed { get; set; }
    public int RecordsFailed { get; set; }
    public string? ErrorMessage { get; set; }
    public ETLJobConfigurationDto Configuration { get; set; } = new();
    public List<ETLJobLogDto> Logs { get; set; } = new();
}

/// <summary>
/// Data Transfer Object for ETL Job Configuration
/// </summary>
public class ETLJobConfigurationDto
{
    public Guid Id { get; set; }
    public DataSourceConfigurationDto Source { get; set; } = new();
    public DataTargetConfigurationDto Target { get; set; } = new();
    public TransformationConfigurationDto? Transformation { get; set; }
    public SchedulingConfigurationDto? Scheduling { get; set; }
    public ErrorHandlingConfigurationDto ErrorHandling { get; set; } = new();
    public PerformanceConfigurationDto Performance { get; set; } = new();
    public ETLEngineConfigurationDto Engine { get; set; } = new();
}

/// <summary>
/// Data Transfer Object for Data Source Configuration
/// </summary>
public class DataSourceConfigurationDto
{
    public string ConnectionString { get; set; } = string.Empty;
    public string Query { get; set; } = string.Empty;
    public string Provider { get; set; } = string.Empty;
    public Dictionary<string, string> Parameters { get; set; } = new();
    public int CommandTimeout { get; set; } = 30;
    public bool EnableRetry { get; set; } = true;
    public int MaxRetryAttempts { get; set; } = 3;
}

/// <summary>
/// Data Transfer Object for Data Target Configuration
/// </summary>
public class DataTargetConfigurationDto
{
    public string ConnectionString { get; set; } = string.Empty;
    public string TableName { get; set; } = string.Empty;
    public string Provider { get; set; } = string.Empty;
    public LoadStrategy LoadStrategy { get; set; } = LoadStrategy.Insert;
    public Dictionary<string, string> ColumnMappings { get; set; } = new();
    public int BatchSize { get; set; } = 1000;
    public bool EnableBulkCopy { get; set; } = true;
}

/// <summary>
/// Data Transfer Object for Transformation Configuration
/// </summary>
public class TransformationConfigurationDto
{
    public List<TransformationRuleDto> Rules { get; set; } = new();
    public DataValidationConfigurationDto? Validation { get; set; }
    public DataCleansingConfigurationDto? Cleansing { get; set; }
}

/// <summary>
/// Data Transfer Object for Transformation Rule
/// </summary>
public class TransformationRuleDto
{
    public string Name { get; set; } = string.Empty;
    public string SourceColumn { get; set; } = string.Empty;
    public string TargetColumn { get; set; } = string.Empty;
    public TransformationType Type { get; set; }
    public string? Expression { get; set; }
    public Dictionary<string, string> Parameters { get; set; } = new();
}

/// <summary>
/// Data Transfer Object for Scheduling Configuration
/// </summary>
public class SchedulingConfigurationDto
{
    public bool IsEnabled { get; set; } = false;
    public string CronExpression { get; set; } = string.Empty;
    public string? TimeZoneId { get; set; }
    public DateTime? StartDate { get; set; }
    public DateTime? EndDate { get; set; }
    public int MaxConcurrentExecutions { get; set; } = 1;
}

/// <summary>
/// Data Transfer Object for Error Handling Configuration
/// </summary>
public class ErrorHandlingConfigurationDto
{
    public ErrorHandlingStrategy Strategy { get; set; } = ErrorHandlingStrategy.ContinueOnError;
    public int MaxErrors { get; set; } = 100;
    public bool LogErrors { get; set; } = true;
    public string? ErrorTable { get; set; }
    public bool SendNotifications { get; set; } = false;
    public List<string> NotificationEmails { get; set; } = new();
}

/// <summary>
/// Data Transfer Object for Performance Configuration
/// </summary>
public class PerformanceConfigurationDto
{
    public int MaxDegreeOfParallelism { get; set; } = Environment.ProcessorCount;
    public int BufferSize { get; set; } = 8192;
    public bool EnableCompression { get; set; } = false;
    public int MemoryLimitMB { get; set; } = 1024;
    public bool EnableProfiling { get; set; } = false;
}

/// <summary>
/// Data Transfer Object for Data Validation Configuration
/// </summary>
public class DataValidationConfigurationDto
{
    public List<ValidationRuleDto> Rules { get; set; } = new();
    public bool StopOnFirstError { get; set; } = false;
}

/// <summary>
/// Data Transfer Object for Validation Rule
/// </summary>
public class ValidationRuleDto
{
    public string Column { get; set; } = string.Empty;
    public ValidationType Type { get; set; }
    public string? Expression { get; set; }
    public string? ErrorMessage { get; set; }
}

/// <summary>
/// Data Transfer Object for Data Cleansing Configuration
/// </summary>
public class DataCleansingConfigurationDto
{
    public bool RemoveNulls { get; set; } = false;
    public bool TrimStrings { get; set; } = true;
    public bool RemoveDuplicates { get; set; } = false;
    public Dictionary<string, string> DefaultValues { get; set; } = new();
}

/// <summary>
/// Data Transfer Object for ETL Job Log
/// </summary>
public class ETLJobLogDto
{
    public Guid Id { get; set; }
    public Guid ETLJobId { get; set; }
    public DateTime Timestamp { get; set; }
    public ETLJobLogLevel Level { get; set; }
    public string Message { get; set; } = string.Empty;
    public string? Exception { get; set; }
    public string? StackTrace { get; set; }
    public Dictionary<string, string>? Properties { get; set; }
}
