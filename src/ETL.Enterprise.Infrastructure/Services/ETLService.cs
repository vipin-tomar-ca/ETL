using Microsoft.Extensions.Logging;
using ETL.Enterprise.Domain.Entities;
using ETL.Enterprise.Domain.Enums;
using ETL.Enterprise.Domain.Services;
using ETL.Enterprise.Domain.Repositories;

namespace ETL.Enterprise.Infrastructure.Services;

/// <summary>
/// Main ETL service implementation that orchestrates ETL operations using different engines
/// </summary>
public class ETLService : IETLService
{
    private readonly ILogger<ETLService> _logger;
    private readonly IETLJobRepository _etlJobRepository;
    private readonly IETLJobLogRepository _etlJobLogRepository;
    private readonly IETLEngineFactory _etlEngineFactory;

    public ETLService(
        ILogger<ETLService> logger,
        IETLJobRepository etlJobRepository,
        IETLJobLogRepository etlJobLogRepository,
        IETLEngineFactory etlEngineFactory)
    {
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));
        _etlJobRepository = etlJobRepository ?? throw new ArgumentNullException(nameof(etlJobRepository));
        _etlJobLogRepository = etlJobLogRepository ?? throw new ArgumentNullException(nameof(etlJobLogRepository));
        _etlEngineFactory = etlEngineFactory ?? throw new ArgumentNullException(nameof(etlEngineFactory));
    }

    public async Task<ETLJob> CreateAndStartJobAsync(
        string name, 
        string description, 
        ETLJobConfiguration configuration, 
        CancellationToken cancellationToken = default)
    {
        try
        {
            _logger.LogInformation("Creating and starting ETL job: {JobName}", name);

            // Auto-select engine if configured
            if (configuration.Engine.AutoSelectEngine)
            {
                var recommendedEngine = _etlEngineFactory.GetRecommendedEngine(configuration);
                configuration.Engine.EngineType = recommendedEngine;
                _logger.LogInformation("Auto-selected ETL engine: {EngineType}", recommendedEngine);
            }

            // Validate configuration with selected engine
            var engine = _etlEngineFactory.CreateEngine(configuration.Engine.EngineType);
            var validationResult = await engine.ValidateConfigurationAsync(configuration, cancellationToken);

            if (!validationResult.IsValid)
            {
                var errorMessage = $"Configuration validation failed: {string.Join(", ", validationResult.Errors)}";
                _logger.LogError(errorMessage);
                throw new InvalidOperationException(errorMessage);
            }

            // Create ETL job
            var etlJob = new ETLJob(name, description, configuration);
            etlJob.AddLog($"ETL job created with engine: {engine.EngineName} v{engine.EngineVersion}", ETLJobLogLevel.Information);

            // Save job to repository
            etlJob = await _etlJobRepository.AddAsync(etlJob, cancellationToken);

            // Start execution
            await ExecuteJobAsync(etlJob.Id, cancellationToken);

            return etlJob;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error creating and starting ETL job: {JobName}", name);
            throw;
        }
    }

    public async Task<ETLJob> ExecuteJobAsync(Guid jobId, CancellationToken cancellationToken = default)
    {
        try
        {
            _logger.LogInformation("Executing ETL job: {JobId}", jobId);

            // Get job from repository
            var etlJob = await _etlJobRepository.GetByIdAsync(jobId, cancellationToken);
            if (etlJob == null)
            {
                throw new ArgumentException($"ETL job not found: {jobId}");
            }

            // Create engine instance
            var engine = _etlEngineFactory.CreateEngine(etlJob.Configuration.Engine.EngineType);
            
            etlJob.AddLog($"Starting execution with {engine.EngineName}", ETLJobLogLevel.Information);

            // Execute with progress reporting
            var progress = new Progress<ETLProgress>(p => ReportProgress(etlJob, p));
            var executionResult = await engine.ExecuteAsync(etlJob, progress, cancellationToken);

            // Update job status
            if (executionResult.IsSuccess)
            {
                etlJob.AddLog($"Job completed successfully. Processed: {executionResult.RecordsProcessed}, Failed: {executionResult.RecordsFailed}", ETLJobLogLevel.Information);
            }
            else
            {
                etlJob.AddLog($"Job failed: {executionResult.ErrorMessage}", ETLJobLogLevel.Error);
            }

            // Save updated job
            etlJob = await _etlJobRepository.UpdateAsync(etlJob, cancellationToken);

            return etlJob;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error executing ETL job: {JobId}", jobId);
            throw;
        }
    }

    public async Task<ETLJob> CancelJobAsync(Guid jobId, CancellationToken cancellationToken = default)
    {
        try
        {
            _logger.LogInformation("Cancelling ETL job: {JobId}", jobId);

            var etlJob = await _etlJobRepository.GetByIdAsync(jobId, cancellationToken);
            if (etlJob == null)
            {
                throw new ArgumentException($"ETL job not found: {jobId}");
            }

            var engine = _etlEngineFactory.CreateEngine(etlJob.Configuration.Engine.EngineType);
            var cancelled = await engine.CancelAsync(jobId, cancellationToken);

            if (cancelled)
            {
                etlJob.AddLog("Job cancelled by user", ETLJobLogLevel.Warning);
                etlJob = await _etlJobRepository.UpdateAsync(etlJob, cancellationToken);
            }

            return etlJob;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error cancelling ETL job: {JobId}", jobId);
            throw;
        }
    }

    public async Task<ETLJob> PauseJobAsync(Guid jobId, CancellationToken cancellationToken = default)
    {
        try
        {
            _logger.LogInformation("Pausing ETL job: {JobId}", jobId);

            var etlJob = await _etlJobRepository.GetByIdAsync(jobId, cancellationToken);
            if (etlJob == null)
            {
                throw new ArgumentException($"ETL job not found: {jobId}");
            }

            // Note: Not all engines support pausing, this is a placeholder
            etlJob.AddLog("Job pause requested (not supported by all engines)", ETLJobLogLevel.Warning);
            etlJob = await _etlJobRepository.UpdateAsync(etlJob, cancellationToken);

            return etlJob;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error pausing ETL job: {JobId}", jobId);
            throw;
        }
    }

    public async Task<ETLJob> ResumeJobAsync(Guid jobId, CancellationToken cancellationToken = default)
    {
        try
        {
            _logger.LogInformation("Resuming ETL job: {JobId}", jobId);

            var etlJob = await _etlJobRepository.GetByIdAsync(jobId, cancellationToken);
            if (etlJob == null)
            {
                throw new ArgumentException($"ETL job not found: {jobId}");
            }

            // Note: Not all engines support resuming, this is a placeholder
            etlJob.AddLog("Job resume requested (not supported by all engines)", ETLJobLogLevel.Warning);
            etlJob = await _etlJobRepository.UpdateAsync(etlJob, cancellationToken);

            return etlJob;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error resuming ETL job: {JobId}", jobId);
            throw;
        }
    }

    public async Task<ETLJob> GetJobStatusAsync(Guid jobId, CancellationToken cancellationToken = default)
    {
        try
        {
            var etlJob = await _etlJobRepository.GetByIdAsync(jobId, cancellationToken);
            if (etlJob == null)
            {
                throw new ArgumentException($"ETL job not found: {jobId}");
            }

            // Get real-time status from engine if job is running
            if (etlJob.Status == ETLJobStatus.Running)
            {
                var engine = _etlEngineFactory.CreateEngine(etlJob.Configuration.Engine.EngineType);
                var engineStatus = await engine.GetStatusAsync(jobId, cancellationToken);
                
                if (engineStatus != etlJob.Status)
                {
                    etlJob.AddLog($"Status updated from engine: {engineStatus}", ETLJobLogLevel.Debug);
                    etlJob = await _etlJobRepository.UpdateAsync(etlJob, cancellationToken);
                }
            }

            return etlJob;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error getting job status: {JobId}", jobId);
            throw;
        }
    }

    public async Task<ValidationResult> ValidateConfigurationAsync(ETLJobConfiguration configuration, CancellationToken cancellationToken = default)
    {
        try
        {
            _logger.LogDebug("Validating ETL configuration");

            var errors = new List<string>();
            var warnings = new List<string>();

            // Basic configuration validation
            if (string.IsNullOrEmpty(configuration.Source.ConnectionString))
                errors.Add("Source connection string is required");

            if (string.IsNullOrEmpty(configuration.Target.ConnectionString))
                errors.Add("Target connection string is required");

            if (string.IsNullOrEmpty(configuration.Source.Query))
                errors.Add("Source query is required");

            if (string.IsNullOrEmpty(configuration.Target.TableName))
                errors.Add("Target table name is required");

            // Engine-specific validation
            if (configuration.Engine.AutoSelectEngine)
            {
                var recommendedEngine = _etlEngineFactory.GetRecommendedEngine(configuration);
                configuration.Engine.EngineType = recommendedEngine;
                warnings.Add($"Auto-selected engine: {recommendedEngine}");
            }

            if (_etlEngineFactory.IsEngineAvailable(configuration.Engine.EngineType))
            {
                var engine = _etlEngineFactory.CreateEngine(configuration.Engine.EngineType);
                var engineValidation = await engine.ValidateConfigurationAsync(configuration, cancellationToken);
                
                errors.AddRange(engineValidation.Errors);
                warnings.AddRange(engineValidation.Warnings);
            }
            else
            {
                errors.Add($"ETL engine '{configuration.Engine.EngineType}' is not available");
            }

            return errors.Count == 0 ? ValidationResult.Success() : ValidationResult.Failure(errors.ToArray());
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error validating configuration");
            return ValidationResult.Failure($"Configuration validation failed: {ex.Message}");
        }
    }

    private void ReportProgress(ETLJob etlJob, ETLProgress progress)
    {
        try
        {
            etlJob.UpdateProgress(progress.RecordsProcessed, progress.RecordsFailed);
            
            var logMessage = $"Progress: {progress.PercentageComplete:F1}% complete, " +
                           $"Processed: {progress.RecordsProcessed}, " +
                           $"Failed: {progress.RecordsFailed}, " +
                           $"Elapsed: {progress.ElapsedTime:mm\\:ss}, " +
                           $"ETA: {progress.EstimatedTimeRemaining:mm\\:ss}";

            etlJob.AddLog(logMessage, ETLJobLogLevel.Debug);

            _logger.LogDebug("ETL Progress - Job: {JobId}, Step: {Step}, Progress: {Progress}%", 
                progress.JobId, progress.CurrentStep, progress.PercentageComplete);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error reporting progress for job: {JobId}", progress.JobId);
        }
    }
}
