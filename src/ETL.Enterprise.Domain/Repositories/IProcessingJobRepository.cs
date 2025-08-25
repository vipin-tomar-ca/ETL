using ETL.Enterprise.Domain.Entities;
using ETL.Enterprise.Domain.Enums;

namespace ETL.Enterprise.Domain.Repositories;

/// <summary>
/// Repository interface for processing jobs
/// </summary>
public interface IProcessingJobRepository
{
    /// <summary>
    /// Gets a processing job by ID
    /// </summary>
    Task<ProcessingJob?> GetByIdAsync(Guid id, CancellationToken cancellationToken = default);
    
    /// <summary>
    /// Gets processing jobs by tenant and status
    /// </summary>
    Task<IEnumerable<ProcessingJob>> GetByTenantAndStatusAsync(
        Guid tenantId, 
        ProcessingJobStatus status, 
        CancellationToken cancellationToken = default);
    
    /// <summary>
    /// Gets all processing jobs for a tenant
    /// </summary>
    Task<IEnumerable<ProcessingJob>> GetByTenantAsync(
        Guid tenantId, 
        CancellationToken cancellationToken = default);
    
    /// <summary>
    /// Adds a new processing job
    /// </summary>
    Task AddAsync(ProcessingJob processingJob, CancellationToken cancellationToken = default);
    
    /// <summary>
    /// Updates an existing processing job
    /// </summary>
    Task UpdateAsync(ProcessingJob processingJob, CancellationToken cancellationToken = default);
    
    /// <summary>
    /// Deletes a processing job
    /// </summary>
    Task DeleteAsync(Guid id, CancellationToken cancellationToken = default);
    
    /// <summary>
    /// Saves changes to the underlying data store
    /// </summary>
    Task SaveChangesAsync(CancellationToken cancellationToken = default);
    
    /// <summary>
    /// Gets processing jobs that are running or pending
    /// </summary>
    Task<IEnumerable<ProcessingJob>> GetActiveJobsAsync(CancellationToken cancellationToken = default);
    
    /// <summary>
    /// Gets processing jobs with filtering
    /// </summary>
    Task<(IEnumerable<ProcessingJob> Jobs, int TotalCount)> GetJobsAsync(
        ProcessingJobFilterCriteria filterCriteria,
        CancellationToken cancellationToken = default);
}

/// <summary>
/// Filter criteria for processing jobs
/// </summary>
public class ProcessingJobFilterCriteria
{
    public Guid? TenantId { get; set; }
    public ProcessingJobStatus? Status { get; set; }
    public ProcessingJobType? JobType { get; set; }
    public string? NamePattern { get; set; }
    public DateTime? FromDate { get; set; }
    public DateTime? ToDate { get; set; }
    public int Page { get; set; } = 1;
    public int PageSize { get; set; } = 20;
}
