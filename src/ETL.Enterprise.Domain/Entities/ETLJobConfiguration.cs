using System.ComponentModel.DataAnnotations;
using ETL.Enterprise.Domain.Enums;

namespace ETL.Enterprise.Domain.Entities;

/// <summary>
/// Configuration for an ETL job including source, target, and transformation settings
/// </summary>
public class ETLJobConfiguration
{
    public Guid Id { get; private set; }
    
    [Required]
    public DataSourceConfiguration Source { get; private set; } = new();
    
    [Required]
    public DataTargetConfiguration Target { get; private set; } = new();
    
    public TransformationConfiguration? Transformation { get; private set; }
    
    public SchedulingConfiguration? Scheduling { get; private set; }
    
    public ErrorHandlingConfiguration ErrorHandling { get; private set; } = new();
    
    public PerformanceConfiguration Performance { get; private set; } = new();
    
    public ETLEngineConfiguration Engine { get; private set; } = new();
    
    // Public parameterless constructor
    public ETLJobConfiguration() 
    {
        Id = Guid.NewGuid();
    }
    
    public ETLJobConfiguration(
        DataSourceConfiguration source, 
        DataTargetConfiguration target,
        TransformationConfiguration? transformation = null,
        SchedulingConfiguration? scheduling = null,
        ETLEngineConfiguration? engine = null)
    {
        Id = Guid.NewGuid();
        Source = source ?? throw new ArgumentNullException(nameof(source));
        Target = target ?? throw new ArgumentNullException(nameof(target));
        Transformation = transformation;
        Scheduling = scheduling;
        Engine = engine ?? new ETLEngineConfiguration();
    }
    
    public void UpdateTransformation(TransformationConfiguration transformation)
    {
        Transformation = transformation ?? throw new ArgumentNullException(nameof(transformation));
    }
    
    public void UpdateScheduling(SchedulingConfiguration scheduling)
    {
        Scheduling = scheduling ?? throw new ArgumentNullException(nameof(scheduling));
    }
    
    public void UpdateErrorHandling(ErrorHandlingConfiguration errorHandling)
    {
        ErrorHandling = errorHandling ?? throw new ArgumentNullException(nameof(errorHandling));
    }
    
    public void UpdatePerformance(PerformanceConfiguration performance)
    {
        Performance = performance ?? throw new ArgumentNullException(nameof(performance));
    }
    
    public void UpdateEngine(ETLEngineConfiguration engine)
    {
        Engine = engine ?? throw new ArgumentNullException(nameof(engine));
    }
}

public class DataSourceConfiguration
{
    public string ConnectionString { get; set; } = string.Empty;
    public string Query { get; set; } = string.Empty;
    public string Provider { get; set; } = string.Empty; // SQL Server, Oracle, etc.
    public Dictionary<string, string> Parameters { get; set; } = new();
    public int CommandTimeout { get; set; } = 30;
    public bool EnableRetry { get; set; } = true;
    public int MaxRetryAttempts { get; set; } = 3;
}

public class DataTargetConfiguration
{
    public string ConnectionString { get; set; } = string.Empty;
    public string TableName { get; set; } = string.Empty;
    public string Provider { get; set; } = string.Empty;
    public LoadStrategy LoadStrategy { get; set; } = LoadStrategy.Insert;
    public Dictionary<string, string> ColumnMappings { get; set; } = new();
    public int BatchSize { get; set; } = 1000;
    public bool EnableBulkCopy { get; set; } = true;
}

public class TransformationConfiguration
{
    public List<TransformationRule> Rules { get; set; } = new();
    public DataValidationConfiguration? Validation { get; set; }
    public DataCleansingConfiguration? Cleansing { get; set; }
}

public class TransformationRule
{
    public string Name { get; set; } = string.Empty;
    public string SourceColumn { get; set; } = string.Empty;
    public string TargetColumn { get; set; } = string.Empty;
    public TransformationType Type { get; set; }
    public string? Expression { get; set; }
    public Dictionary<string, string> Parameters { get; set; } = new();
}

public class SchedulingConfiguration
{
    public bool IsEnabled { get; set; } = false;
    public string CronExpression { get; set; } = string.Empty;
    public TimeZoneInfo? TimeZone { get; set; }
    public DateTime? StartDate { get; set; }
    public DateTime? EndDate { get; set; }
    public int MaxConcurrentExecutions { get; set; } = 1;
}

public class ErrorHandlingConfiguration
{
    public ErrorHandlingStrategy Strategy { get; set; } = ErrorHandlingStrategy.ContinueOnError;
    public int MaxErrors { get; set; } = 100;
    public bool LogErrors { get; set; } = true;
    public string? ErrorTable { get; set; }
    public bool SendNotifications { get; set; } = false;
    public List<string> NotificationEmails { get; set; } = new();
}

public class PerformanceConfiguration
{
    public int MaxDegreeOfParallelism { get; set; } = Environment.ProcessorCount;
    public int BufferSize { get; set; } = 8192;
    public bool EnableCompression { get; set; } = false;
    public int MemoryLimitMB { get; set; } = 1024;
    public bool EnableProfiling { get; set; } = false;
}

public class DataValidationConfiguration
{
    public List<ValidationRule> Rules { get; set; } = new();
    public bool StopOnFirstError { get; set; } = false;
}

public class ValidationRule
{
    public string Column { get; set; } = string.Empty;
    public ValidationType Type { get; set; }
    public string? Expression { get; set; }
    public string? ErrorMessage { get; set; }
}

public class DataCleansingConfiguration
{
    public bool RemoveNulls { get; set; } = false;
    public bool TrimStrings { get; set; } = true;
    public bool RemoveDuplicates { get; set; } = false;
    public Dictionary<string, string> DefaultValues { get; set; } = new();
}
