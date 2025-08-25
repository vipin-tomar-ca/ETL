using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Logging;
using ETL.Enterprise.Domain.Entities;
using ETL.Enterprise.Domain.Enums;
using ETL.Enterprise.Domain.Repositories;
using ETL.Enterprise.Infrastructure.Data;

namespace ETL.Enterprise.Infrastructure.Repositories;

/// <summary>
/// Repository implementation for ETL job log operations
/// </summary>
public class ETLJobLogRepository : IETLJobLogRepository
{
    private readonly ETLDbContext _context;
    private readonly ILogger<ETLJobLogRepository> _logger;
    
    public ETLJobLogRepository(ETLDbContext context, ILogger<ETLJobLogRepository> logger)
    {
        _context = context ?? throw new ArgumentNullException(nameof(context));
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));
    }
    
    public async Task<IEnumerable<ETLJobLog>> GetByJobIdAsync(Guid jobId, CancellationToken cancellationToken = default)
    {
        try
        {
            _logger.LogDebug("Getting logs for ETL job: {JobId}", jobId);
            
            return await _context.ETLJobLogs
                .Where(l => l.ETLJobId == jobId)
                .OrderByDescending(l => l.Timestamp)
                .ToListAsync(cancellationToken);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error getting logs for ETL job: {JobId}", jobId);
            throw;
        }
    }
    
    public async Task<IEnumerable<ETLJobLog>> GetByJobIdAndLevelAsync(
        Guid jobId, 
        ETLJobLogLevel level, 
        CancellationToken cancellationToken = default)
    {
        try
        {
            _logger.LogDebug("Getting logs for ETL job {JobId} with level {Level}", jobId, level);
            
            return await _context.ETLJobLogs
                .Where(l => l.ETLJobId == jobId && l.Level == level)
                .OrderByDescending(l => l.Timestamp)
                .ToListAsync(cancellationToken);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error getting logs for ETL job {JobId} with level {Level}", jobId, level);
            throw;
        }
    }
    
    public async Task<IEnumerable<ETLJobLog>> GetByJobIdAndDateRangeAsync(
        Guid jobId, 
        DateTime fromDate, 
        DateTime toDate, 
        CancellationToken cancellationToken = default)
    {
        try
        {
            _logger.LogDebug("Getting logs for ETL job {JobId} from {FromDate} to {ToDate}", jobId, fromDate, toDate);
            
            return await _context.ETLJobLogs
                .Where(l => l.ETLJobId == jobId && l.Timestamp >= fromDate && l.Timestamp <= toDate)
                .OrderByDescending(l => l.Timestamp)
                .ToListAsync(cancellationToken);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error getting logs for ETL job {JobId} in date range", jobId);
            throw;
        }
    }
    
    public async Task<ETLJobLog> AddAsync(ETLJobLog log, CancellationToken cancellationToken = default)
    {
        try
        {
            _logger.LogDebug("Adding log entry for ETL job: {JobId}", log.ETLJobId);
            
            _context.ETLJobLogs.Add(log);
            await _context.SaveChangesAsync(cancellationToken);
            
            _logger.LogDebug("Successfully added log entry: {LogId}", log.Id);
            return log;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error adding log entry for ETL job: {JobId}", log.ETLJobId);
            throw;
        }
    }
    
    public async Task AddRangeAsync(IEnumerable<ETLJobLog> logs, CancellationToken cancellationToken = default)
    {
        try
        {
            _logger.LogDebug("Adding {Count} log entries", logs.Count());
            
            _context.ETLJobLogs.AddRange(logs);
            await _context.SaveChangesAsync(cancellationToken);
            
            _logger.LogDebug("Successfully added {Count} log entries", logs.Count());
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error adding log entries");
            throw;
        }
    }
    
    public async Task DeleteOldLogsAsync(DateTime cutoffDate, CancellationToken cancellationToken = default)
    {
        try
        {
            _logger.LogInformation("Deleting logs older than {CutoffDate}", cutoffDate);
            
            var oldLogs = await _context.ETLJobLogs
                .Where(l => l.Timestamp < cutoffDate)
                .ToListAsync(cancellationToken);
            
            if (oldLogs.Any())
            {
                _context.ETLJobLogs.RemoveRange(oldLogs);
                await _context.SaveChangesAsync(cancellationToken);
                _logger.LogInformation("Successfully deleted {Count} old log entries", oldLogs.Count);
            }
            else
            {
                _logger.LogDebug("No old logs found to delete");
            }
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error deleting old logs");
            throw;
        }
    }
    
    public async Task<int> GetCountByLevelAsync(Guid jobId, ETLJobLogLevel level, CancellationToken cancellationToken = default)
    {
        try
        {
            return await _context.ETLJobLogs
                .CountAsync(l => l.ETLJobId == jobId && l.Level == level, cancellationToken);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error getting log count for ETL job {JobId} with level {Level}", jobId, level);
            throw;
        }
    }
}
