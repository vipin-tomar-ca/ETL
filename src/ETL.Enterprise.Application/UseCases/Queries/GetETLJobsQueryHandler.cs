using MediatR;
using AutoMapper;
using Microsoft.Extensions.Logging;
using ETL.Enterprise.Application.DTOs;
using ETL.Enterprise.Domain.Repositories;
using ETL.Enterprise.Domain.Enums;

namespace ETL.Enterprise.Application.UseCases.Queries;

/// <summary>
/// Handler for getting multiple ETL jobs with filtering and pagination
/// </summary>
public class GetETLJobsQueryHandler : IRequestHandler<GetETLJobsQuery, GetETLJobsResult>
{
    private readonly ILogger<GetETLJobsQueryHandler> _logger;
    private readonly IETLJobRepository _etlJobRepository;
    private readonly IMapper _mapper;

    public GetETLJobsQueryHandler(
        ILogger<GetETLJobsQueryHandler> logger,
        IETLJobRepository etlJobRepository,
        IMapper mapper)
    {
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));
        _etlJobRepository = etlJobRepository ?? throw new ArgumentNullException(nameof(etlJobRepository));
        _mapper = mapper ?? throw new ArgumentNullException(nameof(mapper));
    }

    public async Task<GetETLJobsResult> Handle(GetETLJobsQuery request, CancellationToken cancellationToken)
    {
        try
        {
            _logger.LogInformation("Getting ETL jobs with filters: Status={Status}, NamePattern={NamePattern}, Page={Page}, PageSize={PageSize}", 
                request.Status, request.NamePattern, request.Page, request.PageSize);

            // Build filter criteria
            var filterCriteria = new ETL.Enterprise.Domain.Repositories.ETLJobFilterCriteria
            {
                Status = request.Status,
                NamePattern = request.NamePattern,
                FromDate = request.FromDate,
                ToDate = request.ToDate,
                Page = request.Page,
                PageSize = request.PageSize
            };

            // Get jobs from repository
            var (jobs, totalCount) = await _etlJobRepository.GetJobsAsync(filterCriteria, cancellationToken);

            // Map to DTOs
            var jobDtos = _mapper.Map<List<ETLJobDto>>(jobs);

            _logger.LogInformation("Successfully retrieved {Count} ETL jobs out of {TotalCount}", jobDtos.Count, totalCount);
            return GetETLJobsResult.Success(jobDtos, totalCount);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error getting ETL jobs");
            return GetETLJobsResult.Failure($"Failed to get ETL jobs: {ex.Message}");
        }
    }
}


