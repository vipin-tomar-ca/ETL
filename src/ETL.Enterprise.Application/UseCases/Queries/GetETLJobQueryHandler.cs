using MediatR;
using AutoMapper;
using Microsoft.Extensions.Logging;
using ETL.Enterprise.Application.DTOs;
using ETL.Enterprise.Domain.Repositories;

namespace ETL.Enterprise.Application.UseCases.Queries;

/// <summary>
/// Handler for getting a specific ETL job
/// </summary>
public class GetETLJobQueryHandler : IRequestHandler<GetETLJobQuery, GetETLJobResult>
{
    private readonly ILogger<GetETLJobQueryHandler> _logger;
    private readonly IETLJobRepository _etlJobRepository;
    private readonly IMapper _mapper;

    public GetETLJobQueryHandler(
        ILogger<GetETLJobQueryHandler> logger,
        IETLJobRepository etlJobRepository,
        IMapper mapper)
    {
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));
        _etlJobRepository = etlJobRepository ?? throw new ArgumentNullException(nameof(etlJobRepository));
        _mapper = mapper ?? throw new ArgumentNullException(nameof(mapper));
    }

    public async Task<GetETLJobResult> Handle(GetETLJobQuery request, CancellationToken cancellationToken)
    {
        try
        {
            _logger.LogInformation("Getting ETL job: {JobId}", request.JobId);

            var etlJob = await _etlJobRepository.GetByIdAsync(request.JobId, cancellationToken);
            if (etlJob == null)
            {
                _logger.LogWarning("ETL job not found: {JobId}", request.JobId);
                return GetETLJobResult.Failure($"ETL job not found: {request.JobId}");
            }

            // Map to DTO
            var etlJobDto = _mapper.Map<ETLJobDto>(etlJob);

            // Limit logs if requested
            if (request.MaxLogs.HasValue && etlJobDto.Logs.Count > request.MaxLogs.Value)
            {
                etlJobDto.Logs = etlJobDto.Logs.Take(request.MaxLogs.Value).ToList();
            }

            _logger.LogInformation("Successfully retrieved ETL job: {JobId}", request.JobId);
            return GetETLJobResult.Success(etlJobDto);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error getting ETL job: {JobId}", request.JobId);
            return GetETLJobResult.Failure($"Failed to get ETL job: {ex.Message}");
        }
    }
}
