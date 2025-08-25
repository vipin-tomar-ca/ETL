using MediatR;
using AutoMapper;
using Microsoft.Extensions.Logging;
using ETL.Enterprise.Application.DTOs;
using ETL.Enterprise.Domain.Entities;
using ETL.Enterprise.Domain.Services;
using ETL.Enterprise.Domain.Repositories;

namespace ETL.Enterprise.Application.UseCases.Commands;

/// <summary>
/// Handler for creating ETL jobs
/// </summary>
public class CreateETLJobCommandHandler : IRequestHandler<CreateETLJobCommand, CreateETLJobResult>
{
    private readonly ILogger<CreateETLJobCommandHandler> _logger;
    private readonly IETLJobRepository _etlJobRepository;
    private readonly IETLService _etlService;
    private readonly IMapper _mapper;

    public CreateETLJobCommandHandler(
        ILogger<CreateETLJobCommandHandler> logger,
        IETLJobRepository etlJobRepository,
        IETLService etlService,
        IMapper mapper)
    {
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));
        _etlJobRepository = etlJobRepository ?? throw new ArgumentNullException(nameof(etlJobRepository));
        _etlService = etlService ?? throw new ArgumentNullException(nameof(etlService));
        _mapper = mapper ?? throw new ArgumentNullException(nameof(mapper));
    }

    public async Task<CreateETLJobResult> Handle(CreateETLJobCommand request, CancellationToken cancellationToken)
    {
        try
        {
            _logger.LogInformation("Creating ETL job: {JobName}", request.Name);

            // Validate request
            var validationErrors = ValidateRequest(request);
            if (validationErrors.Any())
            {
                _logger.LogWarning("Validation failed for ETL job creation: {Errors}", string.Join(", ", validationErrors));
                return CreateETLJobResult.ValidationFailure(validationErrors);
            }

            // Map DTO to domain entity
            var configuration = _mapper.Map<ETLJobConfiguration>(request.Configuration);

            // Create ETL job
            var etlJob = new ETLJob(request.Name, request.Description, configuration);

            // Save to repository
            etlJob = await _etlJobRepository.AddAsync(etlJob, cancellationToken);

            // Start immediately if requested
            if (request.StartImmediately)
            {
                _logger.LogInformation("Starting ETL job immediately: {JobId}", etlJob.Id);
                etlJob = await _etlService.ExecuteJobAsync(etlJob.Id, cancellationToken);
            }

            // Map back to DTO
            var etlJobDto = _mapper.Map<ETLJobDto>(etlJob);

            _logger.LogInformation("Successfully created ETL job: {JobId}", etlJob.Id);
            return CreateETLJobResult.Success(etlJobDto);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error creating ETL job: {JobName}", request.Name);
            return CreateETLJobResult.Failure($"Failed to create ETL job: {ex.Message}");
        }
    }

    private List<string> ValidateRequest(CreateETLJobCommand request)
    {
        var errors = new List<string>();

        if (string.IsNullOrWhiteSpace(request.Name))
            errors.Add("Job name is required");

        if (request.Configuration == null)
            errors.Add("Configuration is required");

        if (request.Configuration?.Source == null)
            errors.Add("Source configuration is required");

        if (request.Configuration?.Target == null)
            errors.Add("Target configuration is required");

        if (string.IsNullOrWhiteSpace(request.Configuration?.Source?.ConnectionString))
            errors.Add("Source connection string is required");

        if (string.IsNullOrWhiteSpace(request.Configuration?.Target?.ConnectionString))
            errors.Add("Target connection string is required");

        return errors;
    }
}
