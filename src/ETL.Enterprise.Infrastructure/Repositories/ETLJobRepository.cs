using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Logging;
using ETL.Enterprise.Domain.Entities;
using ETL.Enterprise.Domain.Enums;
using ETL.Enterprise.Domain.Repositories;
using ETL.Enterprise.Infrastructure.Data;

namespace ETL.Enterprise.Infrastructure.Repositories;

/// <summary>
/// Repository implementation for ETL job operations
/// </summary>
public class ETLJobRepository : IETLJobRepository
{
    private readonly ETLDbContext _context;
    private readonly ILogger<ETLJobRepository> _logger;
    
    public ETLJobRepository(ETLDbContext context, ILogger<ETLJobRepository> logger)
    {
        _context = context ?? throw new ArgumentNullException(nameof(context));
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));
    }
    
    public async Task<ETLJob?> GetByIdAsync(Guid id, CancellationToken cancellationToken = default)
    {
        try
        {
            _logger.LogDebug("Getting ETL job by ID: {JobId}", id);
            
            return await _context.ETLJobs
                .Include(j => j.Logs.OrderByDescending(l => l.Timestamp).Take(100))
                .FirstOrDefaultAsync(j => j.Id == id, cancellationToken);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error getting ETL job by ID: {JobId}", id);
            throw;
        }
    }
    
    public async Task<IEnumerable<ETLJob>> GetAllAsync(
        ETLJobStatus? status = null,
        DateTime? fromDate = null,
        DateTime? toDate = null,
        CancellationToken cancellationToken = default)
    {
        try
        {
            _logger.LogDebug("Getting all ETL jobs with filters - Status: {Status}, FromDate: {FromDate}, ToDate: {ToDate}", 
                status, fromDate, toDate);
            
            var query = _context.ETLJobs.AsQueryable();
            
            if (status.HasValue)
                query = query.Where(j => j.Status == status.Value);
                
            if (fromDate.HasValue)
                query = query.Where(j => j.CreatedAt >= fromDate.Value);
                
            if (toDate.HasValue)
                query = query.Where(j => j.CreatedAt <= toDate.Value);
                
            return await query
                .OrderByDescending(j => j.CreatedAt)
                .ToListAsync(cancellationToken);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error getting all ETL jobs");
            throw;
        }
    }
    
    public async Task<IEnumerable<ETLJob>> GetByStatusAsync(ETLJobStatus status, CancellationToken cancellationToken = default)
    {
        try
        {
            _logger.LogDebug("Getting ETL jobs by status: {Status}", status);
            
            return await _context.ETLJobs
                .Where(j => j.Status == status)
                .OrderByDescending(j => j.CreatedAt)
                .ToListAsync(cancellationToken);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error getting ETL jobs by status: {Status}", status);
            throw;
        }
    }
    
    public async Task<IEnumerable<ETLJob>> GetByNameAsync(string namePattern, CancellationToken cancellationToken = default)
    {
        try
        {
            _logger.LogDebug("Getting ETL jobs by name pattern: {NamePattern}", namePattern);
            
            return await _context.ETLJobs
                .Where(j => j.Name.Contains(namePattern))
                .OrderByDescending(j => j.CreatedAt)
                .ToListAsync(cancellationToken);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error getting ETL jobs by name pattern: {NamePattern}", namePattern);
            throw;
        }
    }
    
    public async Task<ETLJob> AddAsync(ETLJob etlJob, CancellationToken cancellationToken = default)
    {
        try
        {
            _logger.LogInformation("Adding new ETL job: {JobName}", etlJob.Name);
            
            _context.ETLJobs.Add(etlJob);
            await _context.SaveChangesAsync(cancellationToken);
            
            _logger.LogInformation("Successfully added ETL job: {JobId}", etlJob.Id);
            return etlJob;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error adding ETL job: {JobName}", etlJob.Name);
            throw;
        }
    }
    
    public async Task<ETLJob> UpdateAsync(ETLJob etlJob, CancellationToken cancellationToken = default)
    {
        try
        {
            _logger.LogDebug("Updating ETL job: {JobId}", etlJob.Id);
            
            _context.ETLJobs.Update(etlJob);
            await _context.SaveChangesAsync(cancellationToken);
            
            _logger.LogDebug("Successfully updated ETL job: {JobId}", etlJob.Id);
            return etlJob;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error updating ETL job: {JobId}", etlJob.Id);
            throw;
        }
    }
    
    public async Task DeleteAsync(Guid id, CancellationToken cancellationToken = default)
    {
        try
        {
            _logger.LogInformation("Deleting ETL job: {JobId}", id);
            
            var etlJob = await _context.ETLJobs.FindAsync(new object[] { id }, cancellationToken);
            if (etlJob != null)
            {
                _context.ETLJobs.Remove(etlJob);
                await _context.SaveChangesAsync(cancellationToken);
                _logger.LogInformation("Successfully deleted ETL job: {JobId}", id);
            }
            else
            {
                _logger.LogWarning("ETL job not found for deletion: {JobId}", id);
            }
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error deleting ETL job: {JobId}", id);
            throw;
        }
    }
    
    public async Task<bool> ExistsAsync(Guid id, CancellationToken cancellationToken = default)
    {
        try
        {
            return await _context.ETLJobs.AnyAsync(j => j.Id == id, cancellationToken);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error checking if ETL job exists: {JobId}", id);
            throw;
        }
    }
    
    public async Task<int> GetCountByStatusAsync(ETLJobStatus status, CancellationToken cancellationToken = default)
    {
        try
        {
            return await _context.ETLJobs.CountAsync(j => j.Status == status, cancellationToken);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error getting count of ETL jobs by status: {Status}", status);
            throw;
        }
    }
    
    public async Task<IEnumerable<ETLJob>> GetScheduledJobsAsync(CancellationToken cancellationToken = default)
    {
        try
        {
            _logger.LogDebug("Getting scheduled ETL jobs");
            
            return await _context.ETLJobs
                .Where(j => j.Configuration.Scheduling != null && 
                           j.Configuration.Scheduling.IsEnabled &&
                           j.Status == ETLJobStatus.Created)
                .OrderBy(j => j.CreatedAt)
                .ToListAsync(cancellationToken);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error getting scheduled ETL jobs");
            throw;
        }
    }
    
    public async Task<(IEnumerable<ETLJob> Jobs, int TotalCount)> GetJobsAsync(
        ETLJobFilterCriteria filterCriteria, 
        CancellationToken cancellationToken = default)
    {
        try
        {
            _logger.LogDebug("Getting ETL jobs with filter criteria: {FilterCriteria}", 
                System.Text.Json.JsonSerializer.Serialize(filterCriteria));
            
            var query = _context.ETLJobs.AsQueryable();
            
            // Apply filters
            if (filterCriteria.Status.HasValue)
                query = query.Where(j => j.Status == filterCriteria.Status.Value);
                
            if (!string.IsNullOrWhiteSpace(filterCriteria.NamePattern))
                query = query.Where(j => j.Name.Contains(filterCriteria.NamePattern));
                
            if (filterCriteria.FromDate.HasValue)
                query = query.Where(j => j.CreatedAt >= filterCriteria.FromDate.Value);
                
            if (filterCriteria.ToDate.HasValue)
                query = query.Where(j => j.CreatedAt <= filterCriteria.ToDate.Value);
            
            // Get total count
            var totalCount = await query.CountAsync(cancellationToken);
            
            // Apply pagination
            var jobs = await query
                .OrderByDescending(j => j.CreatedAt)
                .Skip((filterCriteria.Page - 1) * filterCriteria.PageSize)
                .Take(filterCriteria.PageSize)
                .ToListAsync(cancellationToken);
            
            _logger.LogDebug("Retrieved {Count} ETL jobs out of {TotalCount}", jobs.Count, totalCount);
            return (jobs, totalCount);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error getting ETL jobs with filter criteria");
            throw;
        }
    }
}
