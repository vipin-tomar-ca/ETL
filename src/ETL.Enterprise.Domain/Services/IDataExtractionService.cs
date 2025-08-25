using ETL.Enterprise.Domain.Entities;
using ETL.Enterprise.Domain.Enums;

namespace ETL.Enterprise.Domain.Services;

/// <summary>
/// Service interface for data extraction operations
/// </summary>
public interface IDataExtractionService
{
    /// <summary>
    /// Extracts data from the source based on configuration
    /// </summary>
    Task<DataExtractionResult> ExtractAsync(
        DataSourceConfiguration configuration, 
        IProgress<ExtractionProgress>? progress = null,
        CancellationToken cancellationToken = default);
    
    /// <summary>
    /// Tests the connection to the data source
    /// </summary>
    Task<ConnectionTestResult> TestConnectionAsync(DataSourceConfiguration configuration, CancellationToken cancellationToken = default);
    
    /// <summary>
    /// Gets the schema information from the data source
    /// </summary>
    Task<SchemaInfo> GetSchemaAsync(DataSourceConfiguration configuration, CancellationToken cancellationToken = default);
}

/// <summary>
/// Result of data extraction operation
/// </summary>
public class DataExtractionResult
{
    public bool IsSuccess { get; set; }
    public IEnumerable<IDictionary<string, object>> Data { get; set; } = new List<IDictionary<string, object>>();
    public int TotalRecords { get; set; }
    public TimeSpan Duration { get; set; }
    public string? ErrorMessage { get; set; }
    public Exception? Exception { get; set; }
    
    public static DataExtractionResult Success(IEnumerable<IDictionary<string, object>> data, int totalRecords, TimeSpan duration) =>
        new() { IsSuccess = true, Data = data, TotalRecords = totalRecords, Duration = duration };
    
    public static DataExtractionResult Failure(string errorMessage, Exception? exception = null) =>
        new() { IsSuccess = false, ErrorMessage = errorMessage, Exception = exception };
}

/// <summary>
/// Progress information for data extraction
/// </summary>
public class ExtractionProgress
{
    public int RecordsProcessed { get; set; }
    public int TotalRecords { get; set; }
    public double PercentageComplete { get; set; }
    public TimeSpan ElapsedTime { get; set; }
    public TimeSpan EstimatedTimeRemaining { get; set; }
}

/// <summary>
/// Result of connection test
/// </summary>
public class ConnectionTestResult
{
    public bool IsSuccess { get; set; }
    public TimeSpan ResponseTime { get; set; }
    public string? ErrorMessage { get; set; }
    public Exception? Exception { get; set; }
}

/// <summary>
/// Schema information from data source
/// </summary>
public class SchemaInfo
{
    public List<ColumnInfo> Columns { get; set; } = new();
    public int TotalRows { get; set; }
    public long TotalSize { get; set; }
}

/// <summary>
/// Information about a database column
/// </summary>
public class ColumnInfo
{
    public string Name { get; set; } = string.Empty;
    public string DataType { get; set; } = string.Empty;
    public bool IsNullable { get; set; }
    public int? MaxLength { get; set; }
    public int? Precision { get; set; }
    public int? Scale { get; set; }
}
