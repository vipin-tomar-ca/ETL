using MediatR;
using AutoMapper;
using Microsoft.Extensions.Logging;
using ETL.Enterprise.Application.DTOs;
using ETL.Enterprise.Domain.Services;
using ETL.Enterprise.Domain.Repositories;

namespace ETL.Enterprise.Application.UseCases.Commands;

/// <summary>
/// Handler for executing ETL jobs
/// </summary>
public class ExecuteETLJobCommandHandler : IRequestHandler<ExecuteETLJobCommand, ExecuteETLJobResult>
{
    private readonly ILogger<ExecuteETLJobCommandHandler> _logger;
    private readonly IETLJobRepository _etlJobRepository;
    private readonly IETLService _etlService;
    private readonly IMapper _mapper;

    public ExecuteETLJobCommandHandler(
        ILogger<ExecuteETLJobCommandHandler> logger,
        IETLJobRepository etlJobRepository,
        IETLService etlService,
        IMapper mapper)
    {
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));
        _etlJobRepository = etlJobRepository ?? throw new ArgumentNullException(nameof(etlJobRepository));
        _etlService = etlService ?? throw new ArgumentNullException(nameof(etlService));
        _mapper = mapper ?? throw new ArgumentNullException(nameof(mapper));
    }

    public async Task<ExecuteETLJobResult> Handle(ExecuteETLJobCommand request, CancellationToken cancellationToken)
    {
        try
        {
            _logger.LogInformation("Executing ETL job: {JobId}", request.JobId);

            // Get the job from repository
            var etlJob = await _etlJobRepository.GetByIdAsync(request.JobId, cancellationToken);
            if (etlJob == null)
            {
                _logger.LogWarning("ETL job not found: {JobId}", request.JobId);
                return ExecuteETLJobResult.Failure($"ETL job not found: {request.JobId}");
            }

            var startTime = DateTime.UtcNow;

            // Execute the job
            etlJob = await _etlService.ExecuteJobAsync(request.JobId, cancellationToken);

            var executionTime = DateTime.UtcNow - startTime;

            // Map to DTO
            var etlJobDto = _mapper.Map<ETLJobDto>(etlJob);

            // Determine if job is completed
            var isCompleted = etlJob.Status == Domain.Enums.ETLJobStatus.Completed || 
                             etlJob.Status == Domain.Enums.ETLJobStatus.Failed;

            _logger.LogInformation("ETL job execution completed: {JobId}, Status: {Status}, Duration: {Duration}", 
                request.JobId, etlJob.Status, executionTime);

            return ExecuteETLJobResult.Success(etlJobDto, isCompleted, executionTime);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error executing ETL job: {JobId}", request.JobId);
            return ExecuteETLJobResult.Failure($"Failed to execute ETL job: {ex.Message}");
        }
    }
}
