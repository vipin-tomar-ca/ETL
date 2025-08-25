using ETL.Enterprise.Domain.Entities;
using ETL.Enterprise.Domain.Enums;

namespace ETL.Enterprise.Domain.Repositories;

/// <summary>
/// Repository interface for ETL job log operations
/// </summary>
public interface IETLJobLogRepository
{
    /// <summary>
    /// Gets logs for a specific ETL job
    /// </summary>
    Task<IEnumerable<ETLJobLog>> GetByJobIdAsync(Guid jobId, CancellationToken cancellationToken = default);
    
    /// <summary>
    /// Gets logs for a specific ETL job with level filtering
    /// </summary>
    Task<IEnumerable<ETLJobLog>> GetByJobIdAndLevelAsync(
        Guid jobId, 
        ETLJobLogLevel level, 
        CancellationToken cancellationToken = default);
    
    /// <summary>
    /// Gets logs for a specific ETL job within a date range
    /// </summary>
    Task<IEnumerable<ETLJobLog>> GetByJobIdAndDateRangeAsync(
        Guid jobId, 
        DateTime fromDate, 
        DateTime toDate, 
        CancellationToken cancellationToken = default);
    
    /// <summary>
    /// Adds a new log entry
    /// </summary>
    Task<ETLJobLog> AddAsync(ETLJobLog log, CancellationToken cancellationToken = default);
    
    /// <summary>
    /// Adds multiple log entries
    /// </summary>
    Task AddRangeAsync(IEnumerable<ETLJobLog> logs, CancellationToken cancellationToken = default);
    
    /// <summary>
    /// Deletes logs older than the specified date
    /// </summary>
    Task DeleteOldLogsAsync(DateTime cutoffDate, CancellationToken cancellationToken = default);
    
    /// <summary>
    /// Gets the count of logs by level for a specific job
    /// </summary>
    Task<int> GetCountByLevelAsync(Guid jobId, ETLJobLogLevel level, CancellationToken cancellationToken = default);
}
