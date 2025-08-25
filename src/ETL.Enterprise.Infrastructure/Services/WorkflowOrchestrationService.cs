using Microsoft.Extensions.Logging;
using ETL.Enterprise.Domain.Entities;
using ETL.Enterprise.Domain.Enums;
using ETL.Enterprise.Domain.Services;
using ETL.Enterprise.Domain.Repositories;

namespace ETL.Enterprise.Infrastructure.Services;

/// <summary>
/// Implementation of the workflow orchestration service
/// </summary>
public class WorkflowOrchestrationService : IWorkflowOrchestrationService
{
    private readonly ILogger<WorkflowOrchestrationService> _logger;
    private readonly IETLService _etlService;
    private readonly IFileProcessingService _fileProcessingService;
    private readonly ISFTPService _sftpService;
    private readonly IUserManagementService _userManagementService;
    private readonly IProcessingJobRepository _processingJobRepository;
    private readonly ITenantRepository _tenantRepository;

    public WorkflowOrchestrationService(
        ILogger<WorkflowOrchestrationService> logger,
        IETLService etlService,
        IFileProcessingService fileProcessingService,
        ISFTPService sftpService,
        IUserManagementService userManagementService,
        IProcessingJobRepository processingJobRepository,
        ITenantRepository tenantRepository)
    {
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));
        _etlService = etlService ?? throw new ArgumentNullException(nameof(etlService));
        _fileProcessingService = fileProcessingService ?? throw new ArgumentNullException(nameof(fileProcessingService));
        _sftpService = sftpService ?? throw new ArgumentNullException(nameof(sftpService));
        _userManagementService = userManagementService ?? throw new ArgumentNullException(nameof(userManagementService));
        _processingJobRepository = processingJobRepository ?? throw new ArgumentNullException(nameof(processingJobRepository));
        _tenantRepository = tenantRepository ?? throw new ArgumentNullException(nameof(tenantRepository));
    }

    public async Task<WorkflowOrchestrationResult> StartWorkflowAsync(
        Guid tenantId,
        ProcessingJobType jobType,
        ProcessingJobConfiguration configuration,
        CancellationToken cancellationToken = default)
    {
        try
        {
            _logger.LogInformation("Starting workflow for tenant {TenantId}, job type: {JobType}", tenantId, jobType);

            // Get tenant
            var tenant = await _tenantRepository.GetByIdAsync(tenantId, cancellationToken);
            if (tenant == null)
            {
                return WorkflowOrchestrationResult.Failure($"Tenant {tenantId} not found");
            }

            // Create processing job
            var processingJob = new ProcessingJob(tenantId, $"Workflow_{jobType}", $"Workflow for {jobType}", jobType, configuration);

            // Add workflow steps based on job type
            await AddWorkflowStepsAsync(processingJob, jobType, configuration, cancellationToken);

            // Save processing job
            await _processingJobRepository.AddAsync(processingJob, cancellationToken);
            await _processingJobRepository.SaveChangesAsync(cancellationToken);

            // Start the workflow
            processingJob.Start();
            await _processingJobRepository.SaveChangesAsync(cancellationToken);

            // Execute the first step
            await ExecuteNextStepAsync(processingJob, cancellationToken);

            return WorkflowOrchestrationResult.Success(processingJob);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error starting workflow for tenant {TenantId}", tenantId);
            return WorkflowOrchestrationResult.Failure($"Failed to start workflow: {ex.Message}");
        }
    }

    public async Task<WorkflowStepResult> ExecuteStepAsync(
        Guid processingJobId,
        ProcessingStepType stepType,
        ProcessingStepConfiguration configuration,
        CancellationToken cancellationToken = default)
    {
        try
        {
            _logger.LogInformation("Executing step {StepType} for job {JobId}", stepType, processingJobId);

            var processingJob = await _processingJobRepository.GetByIdAsync(processingJobId, cancellationToken);
            if (processingJob == null)
            {
                return WorkflowStepResult.Failure($"Processing job {processingJobId} not found");
            }

            var step = processingJob.Steps.FirstOrDefault(s => s.StepType == stepType);
            if (step == null)
            {
                return WorkflowStepResult.Failure($"Step {stepType} not found in job {processingJobId}");
            }

            step.Start();
            await _processingJobRepository.SaveChangesAsync(cancellationToken);

            var startTime = DateTime.UtcNow;
            var recordsProcessed = 0;
            var recordsFailed = 0;

            try
            {
                // Execute the step based on its type
                var result = await ExecuteStepByTypeAsync(step, processingJob, cancellationToken);
                
                recordsProcessed = result.RecordsProcessed;
                recordsFailed = result.RecordsFailed;

                step.Complete(recordsProcessed, recordsFailed);
                await _processingJobRepository.SaveChangesAsync(cancellationToken);

                // Check if this was the last step
                if (IsWorkflowComplete(processingJob))
                {
                    processingJob.Complete(recordsProcessed, recordsFailed);
                    await _processingJobRepository.SaveChangesAsync(cancellationToken);
                }
                else
                {
                    // Execute next step
                    await ExecuteNextStepAsync(processingJob, cancellationToken);
                }

                var duration = DateTime.UtcNow - startTime;
                return WorkflowStepResult.Success(step, recordsProcessed, recordsFailed, duration);
            }
            catch (Exception ex)
            {
                step.Fail(ex.Message);
                await _processingJobRepository.SaveChangesAsync(cancellationToken);
                return WorkflowStepResult.Failure($"Step execution failed: {ex.Message}");
            }
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error executing step {StepType} for job {JobId}", stepType, processingJobId);
            return WorkflowStepResult.Failure($"Failed to execute step: {ex.Message}");
        }
    }

    public async Task<WorkflowProgress> GetWorkflowProgressAsync(
        Guid processingJobId,
        CancellationToken cancellationToken = default)
    {
        try
        {
            var processingJob = await _processingJobRepository.GetByIdAsync(processingJobId, cancellationToken);
            if (processingJob == null)
            {
                throw new ArgumentException($"Processing job {processingJobId} not found");
            }

            var totalSteps = processingJob.Steps.Count;
            var completedSteps = processingJob.Steps.Count(s => s.Status == ProcessingStepStatus.Completed);
            var failedSteps = processingJob.Steps.Count(s => s.Status == ProcessingStepStatus.Failed);
            var pendingSteps = processingJob.Steps.Count(s => s.Status == ProcessingStepStatus.Pending);

            var progressPercentage = totalSteps > 0 ? (double)completedSteps / totalSteps * 100 : 0;
            var elapsedTime = processingJob.StartedAt.HasValue ? DateTime.UtcNow - processingJob.StartedAt.Value : TimeSpan.Zero;
            var currentStep = processingJob.Steps.FirstOrDefault(s => s.Status == ProcessingStepStatus.Running)?.Name ?? "None";

            var totalRecordsProcessed = processingJob.Steps.Sum(s => s.RecordsProcessed);
            var totalRecordsFailed = processingJob.Steps.Sum(s => s.RecordsFailed);

            return new WorkflowProgress
            {
                ProcessingJobId = processingJobId,
                Status = processingJob.Status,
                TotalSteps = totalSteps,
                CompletedSteps = completedSteps,
                FailedSteps = failedSteps,
                PendingSteps = pendingSteps,
                ProgressPercentage = progressPercentage,
                ElapsedTime = elapsedTime,
                EstimatedTimeRemaining = TimeSpan.Zero, // Calculate based on progress
                CurrentStep = currentStep,
                Steps = processingJob.Steps.ToList(),
                TotalRecordsProcessed = totalRecordsProcessed,
                TotalRecordsFailed = totalRecordsFailed
            };
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error getting workflow progress for job {JobId}", processingJobId);
            throw;
        }
    }

    public async Task<WorkflowOrchestrationResult> TriggerChildTasksAsync(
        Guid tenantId,
        CancellationToken cancellationToken = default)
    {
        try
        {
            _logger.LogInformation("Triggering child tasks for tenant {TenantId}", tenantId);

            // Get completed ingestion jobs
            var completedJobs = await PollCompletedJobsAsync(tenantId, ProcessingJobStatus.Completed, cancellationToken);

            foreach (var completedJob in completedJobs)
            {
                if (completedJob.JobType == ProcessingJobType.InitialLoad || completedJob.JobType == ProcessingJobType.DeltaLoad)
                {
                    // Trigger file generation
                    await StartFileGenerationWorkflowAsync(tenantId, completedJob, cancellationToken);

                    // Trigger SFTP upload if configured
                    await StartSFTPUploadWorkflowAsync(tenantId, completedJob, cancellationToken);
                }
            }

            return WorkflowOrchestrationResult.Success(null!);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error triggering child tasks for tenant {TenantId}", tenantId);
            return WorkflowOrchestrationResult.Failure($"Failed to trigger child tasks: {ex.Message}");
        }
    }

    public async Task<List<ProcessingJob>> PollCompletedJobsAsync(
        Guid tenantId,
        ProcessingJobStatus status,
        CancellationToken cancellationToken = default)
    {
        try
        {
            var jobs = await _processingJobRepository.GetByTenantAndStatusAsync(tenantId, status, cancellationToken);
            return jobs.ToList();
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error polling completed jobs for tenant {TenantId}", tenantId);
            return new List<ProcessingJob>();
        }
    }

    public async Task<bool> CancelWorkflowAsync(
        Guid processingJobId,
        CancellationToken cancellationToken = default)
    {
        try
        {
            var processingJob = await _processingJobRepository.GetByIdAsync(processingJobId, cancellationToken);
            if (processingJob == null)
                return false;

            processingJob.Cancel();
            await _processingJobRepository.SaveChangesAsync(cancellationToken);
            return true;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error cancelling workflow {JobId}", processingJobId);
            return false;
        }
    }

    public async Task<bool> PauseWorkflowAsync(
        Guid processingJobId,
        CancellationToken cancellationToken = default)
    {
        try
        {
            var processingJob = await _processingJobRepository.GetByIdAsync(processingJobId, cancellationToken);
            if (processingJob == null)
                return false;

            processingJob.Pause();
            await _processingJobRepository.SaveChangesAsync(cancellationToken);
            return true;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error pausing workflow {JobId}", processingJobId);
            return false;
        }
    }

    public async Task<bool> ResumeWorkflowAsync(
        Guid processingJobId,
        CancellationToken cancellationToken = default)
    {
        try
        {
            var processingJob = await _processingJobRepository.GetByIdAsync(processingJobId, cancellationToken);
            if (processingJob == null)
                return false;

            processingJob.Resume();
            await _processingJobRepository.SaveChangesAsync(cancellationToken);

            // Execute next step
            await ExecuteNextStepAsync(processingJob, cancellationToken);
            return true;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error resuming workflow {JobId}", processingJobId);
            return false;
        }
    }

    #region Private Methods

    private async Task AddWorkflowStepsAsync(
        ProcessingJob processingJob,
        ProcessingJobType jobType,
        ProcessingJobConfiguration configuration,
        CancellationToken cancellationToken)
    {
        var stepOrder = 1;

        switch (jobType)
        {
            case ProcessingJobType.InitialLoad:
            case ProcessingJobType.DeltaLoad:
                // SSIS Processing Step
                processingJob.AddStep(new ProcessingJobStep(
                    processingJob.Id,
                    "SSIS Data Processing",
                    "Execute SSIS package for data ingestion",
                    ProcessingStepType.SSISPackageExecution,
                    stepOrder++,
                    new ProcessingStepConfiguration
                    {
                        Parameters = new Dictionary<string, string>
                        {
                            ["packagePath"] = configuration.DataSource.Parameters.GetValueOrDefault("packagePath", ""),
                            ["connectionString"] = configuration.DataSource.ConnectionString
                        }
                    }));

                // Data Validation Step
                processingJob.AddStep(new ProcessingJobStep(
                    processingJob.Id,
                    "Data Validation",
                    "Validate processed data",
                    ProcessingStepType.DataValidation,
                    stepOrder++));

                break;

            case ProcessingJobType.DataExport:
                // File Generation Step
                processingJob.AddStep(new ProcessingJobStep(
                    processingJob.Id,
                    "File Generation",
                    "Generate files from database data",
                    ProcessingStepType.FileGeneration,
                    stepOrder++,
                    new ProcessingStepConfiguration
                    {
                        Parameters = new Dictionary<string, string>
                        {
                            ["query"] = configuration.DataSource.Query,
                            ["outputFormat"] = configuration.FileExport.Format.ToString()
                        }
                    }));

                // File Compression Step
                if (configuration.FileExport.EnableCompression)
                {
                    processingJob.AddStep(new ProcessingJobStep(
                        processingJob.Id,
                        "File Compression",
                        "Compress generated files",
                        ProcessingStepType.FileCompression,
                        stepOrder++));
                }

                break;

            case ProcessingJobType.SFTPUpload:
                // SFTP Upload Step
                processingJob.AddStep(new ProcessingJobStep(
                    processingJob.Id,
                    "SFTP Upload",
                    "Upload files to SFTP server",
                    ProcessingStepType.SFTPUpload,
                    stepOrder++,
                    new ProcessingStepConfiguration
                    {
                        Parameters = new Dictionary<string, string>
                        {
                            ["localPath"] = configuration.FileExport.OutputDirectory,
                            ["remotePath"] = configuration.SFTP.RemoteDirectory
                        }
                    }));

                break;
        }
    }

    private async Task<StepExecutionResult> ExecuteStepByTypeAsync(
        ProcessingJobStep step,
        ProcessingJob processingJob,
        CancellationToken cancellationToken)
    {
        switch (step.StepType)
        {
            case ProcessingStepType.SSISPackageExecution:
                return await ExecuteSSISStepAsync(step, processingJob, cancellationToken);

            case ProcessingStepType.FileGeneration:
                return await ExecuteFileGenerationStepAsync(step, processingJob, cancellationToken);

            case ProcessingStepType.FileCompression:
                return await ExecuteFileCompressionStepAsync(step, processingJob, cancellationToken);

            case ProcessingStepType.SFTPUpload:
                return await ExecuteSFTPUploadStepAsync(step, processingJob, cancellationToken);

            case ProcessingStepType.DataValidation:
                return await ExecuteDataValidationStepAsync(step, processingJob, cancellationToken);

            default:
                throw new NotSupportedException($"Step type {step.StepType} is not supported");
        }
    }

    private async Task<StepExecutionResult> ExecuteSSISStepAsync(
        ProcessingJobStep step,
        ProcessingJob processingJob,
        CancellationToken cancellationToken)
    {
        _logger.LogInformation("Executing SSIS step for job {JobId}", processingJob.Id);

        // Create ETL job configuration for SSIS
        var etlConfiguration = new ETLJobConfiguration(
            processingJob.Configuration.DataSource,
            processingJob.Configuration.DataTarget,
            processingJob.Configuration.Transformation,
            processingJob.Configuration.Scheduling,
            new ETLEngineConfiguration
            {
                EngineType = ETLEngineType.SSIS,
                EngineParameters = step.Configuration.Parameters
            });

        var etlJob = new ETLJob(step.Name, step.Description, etlConfiguration);

        // Execute ETL job
        var result = await _etlService.ExecuteJobAsync(etlJob.Id, cancellationToken);

        return new StepExecutionResult
        {
            RecordsProcessed = result.RecordsProcessed,
            RecordsFailed = result.RecordsFailed
        };
    }

    private async Task<StepExecutionResult> ExecuteFileGenerationStepAsync(
        ProcessingJobStep step,
        ProcessingJob processingJob,
        CancellationToken cancellationToken)
    {
        _logger.LogInformation("Executing file generation step for job {JobId}", processingJob.Id);

        var query = step.Configuration.Parameters.GetValueOrDefault("query", "");
        var result = await _fileProcessingService.GenerateFileAsync(
            processingJob.TenantId,
            query,
            processingJob.Configuration.FileExport,
            cancellationToken);

        return new StepExecutionResult
        {
            RecordsProcessed = result.RecordCount,
            RecordsFailed = 0
        };
    }

    private async Task<StepExecutionResult> ExecuteFileCompressionStepAsync(
        ProcessingJobStep step,
        ProcessingJob processingJob,
        CancellationToken cancellationToken)
    {
        _logger.LogInformation("Executing file compression step for job {JobId}", processingJob.Id);

        var sourceDirectory = processingJob.Configuration.FileExport.OutputDirectory;
        var outputFilePath = Path.Combine(sourceDirectory, $"compressed_{DateTime.UtcNow:yyyyMMdd_HHmmss}.zip");

        var result = await _fileProcessingService.CompressFilesAsync(
            sourceDirectory,
            outputFilePath,
            true, // Delete source files
            cancellationToken);

        return new StepExecutionResult
        {
            RecordsProcessed = 1, // One compressed file
            RecordsFailed = 0
        };
    }

    private async Task<StepExecutionResult> ExecuteSFTPUploadStepAsync(
        ProcessingJobStep step,
        ProcessingJob processingJob,
        CancellationToken cancellationToken)
    {
        _logger.LogInformation("Executing SFTP upload step for job {JobId}", processingJob.Id);

        var localPath = step.Configuration.Parameters.GetValueOrDefault("localPath", "");
        var remotePath = step.Configuration.Parameters.GetValueOrDefault("remotePath", "");

        // Get all files in the local directory
        var files = Directory.GetFiles(localPath, "*.*", SearchOption.TopDirectoryOnly);
        var recordsProcessed = 0;
        var recordsFailed = 0;

        foreach (var file in files)
        {
            try
            {
                var fileName = Path.GetFileName(file);
                var remoteFilePath = Path.Combine(remotePath, fileName).Replace('\\', '/');

                var result = await _sftpService.UploadFileAsync(
                    file,
                    remoteFilePath,
                    processingJob.Configuration.SFTP,
                    cancellationToken);

                if (result.IsSuccess)
                    recordsProcessed++;
                else
                    recordsFailed++;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error uploading file {File}", file);
                recordsFailed++;
            }
        }

        return new StepExecutionResult
        {
            RecordsProcessed = recordsProcessed,
            RecordsFailed = recordsFailed
        };
    }

    private async Task<StepExecutionResult> ExecuteDataValidationStepAsync(
        ProcessingJobStep step,
        ProcessingJob processingJob,
        CancellationToken cancellationToken)
    {
        _logger.LogInformation("Executing data validation step for job {JobId}", processingJob.Id);

        // Simulate data validation
        await Task.Delay(1000, cancellationToken);

        return new StepExecutionResult
        {
            RecordsProcessed = 1000,
            RecordsFailed = 0
        };
    }

    private async Task ExecuteNextStepAsync(ProcessingJob processingJob, CancellationToken cancellationToken)
    {
        var nextStep = processingJob.Steps
            .Where(s => s.Status == ProcessingStepStatus.Pending)
            .OrderBy(s => s.Order)
            .FirstOrDefault();

        if (nextStep != null)
        {
            await ExecuteStepAsync(processingJob.Id, nextStep.StepType, nextStep.Configuration, cancellationToken);
        }
    }

    private bool IsWorkflowComplete(ProcessingJob processingJob)
    {
        return processingJob.Steps.All(s => s.Status == ProcessingStepStatus.Completed || s.Status == ProcessingStepStatus.Skipped);
    }

    private async Task StartFileGenerationWorkflowAsync(
        Guid tenantId,
        ProcessingJob completedJob,
        CancellationToken cancellationToken)
    {
        var configuration = new ProcessingJobConfiguration
        {
            DataSource = completedJob.Configuration.DataSource,
            DataTarget = completedJob.Configuration.DataTarget,
            FileExport = completedJob.Configuration.FileExport
        };

        await StartWorkflowAsync(tenantId, ProcessingJobType.DataExport, configuration, cancellationToken);
    }

    private async Task StartSFTPUploadWorkflowAsync(
        Guid tenantId,
        ProcessingJob completedJob,
        CancellationToken cancellationToken)
    {
        var configuration = new ProcessingJobConfiguration
        {
            FileExport = completedJob.Configuration.FileExport,
            SFTP = completedJob.Configuration.SFTP
        };

        await StartWorkflowAsync(tenantId, ProcessingJobType.SFTPUpload, configuration, cancellationToken);
    }

    #endregion

    #region Supporting Classes

    private class StepExecutionResult
    {
        public int RecordsProcessed { get; set; }
        public int RecordsFailed { get; set; }
    }

    #endregion
}
