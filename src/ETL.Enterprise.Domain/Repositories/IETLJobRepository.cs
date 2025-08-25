using ETL.Enterprise.Domain.Entities;
using ETL.Enterprise.Domain.Enums;

namespace ETL.Enterprise.Domain.Repositories;

/// <summary>
/// Repository interface for ETL job operations
/// </summary>
public interface IETLJobRepository
{
    /// <summary>
    /// Gets an ETL job by its ID
    /// </summary>
    Task<ETLJob?> GetByIdAsync(Guid id, CancellationToken cancellationToken = default);
    
    /// <summary>
    /// Gets all ETL jobs with optional filtering
    /// </summary>
    Task<IEnumerable<ETLJob>> GetAllAsync(
        ETLJobStatus? status = null,
        DateTime? fromDate = null,
        DateTime? toDate = null,
        CancellationToken cancellationToken = default);
    
    /// <summary>
    /// Gets ETL jobs by status
    /// </summary>
    Task<IEnumerable<ETLJob>> GetByStatusAsync(ETLJobStatus status, CancellationToken cancellationToken = default);
    
    /// <summary>
    /// Gets ETL jobs by name pattern
    /// </summary>
    Task<IEnumerable<ETLJob>> GetByNameAsync(string namePattern, CancellationToken cancellationToken = default);
    
    /// <summary>
    /// Adds a new ETL job
    /// </summary>
    Task<ETLJob> AddAsync(ETLJob etlJob, CancellationToken cancellationToken = default);
    
    /// <summary>
    /// Updates an existing ETL job
    /// </summary>
    Task<ETLJob> UpdateAsync(ETLJob etlJob, CancellationToken cancellationToken = default);
    
    /// <summary>
    /// Deletes an ETL job
    /// </summary>
    Task DeleteAsync(Guid id, CancellationToken cancellationToken = default);
    
    /// <summary>
    /// Checks if an ETL job exists
    /// </summary>
    Task<bool> ExistsAsync(Guid id, CancellationToken cancellationToken = default);
    
    /// <summary>
    /// Gets the count of ETL jobs by status
    /// </summary>
    Task<int> GetCountByStatusAsync(ETLJobStatus status, CancellationToken cancellationToken = default);
    
    /// <summary>
    /// Gets ETL jobs that need to be executed based on scheduling
    /// </summary>
    Task<IEnumerable<ETLJob>> GetScheduledJobsAsync(CancellationToken cancellationToken = default);
    
    /// <summary>
    /// Gets ETL jobs with filtering and pagination
    /// </summary>
    Task<(IEnumerable<ETLJob> Jobs, int TotalCount)> GetJobsAsync(
        ETLJobFilterCriteria filterCriteria, 
        CancellationToken cancellationToken = default);
}

/// <summary>
/// Filter criteria for ETL jobs
/// </summary>
public class ETLJobFilterCriteria
{
    public ETLJobStatus? Status { get; set; }
    public string? NamePattern { get; set; }
    public DateTime? FromDate { get; set; }
    public DateTime? ToDate { get; set; }
    public int Page { get; set; } = 1;
    public int PageSize { get; set; } = 20;
}
