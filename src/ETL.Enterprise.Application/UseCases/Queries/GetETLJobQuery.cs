using MediatR;
using ETL.Enterprise.Application.DTOs;

namespace ETL.Enterprise.Application.UseCases.Queries;

/// <summary>
/// Query to get a specific ETL job by ID
/// </summary>
public class GetETLJobQuery : IRequest<GetETLJobResult>
{
    public Guid JobId { get; set; }
    public bool IncludeLogs { get; set; } = true;
    public int? MaxLogs { get; set; }
}

/// <summary>
/// Result of getting an ETL job
/// </summary>
public class GetETLJobResult
{
    public bool IsSuccess { get; set; }
    public ETLJobDto? ETLJob { get; set; }
    public string? ErrorMessage { get; set; }
    
    public static GetETLJobResult Success(ETLJobDto etlJob) =>
        new() { IsSuccess = true, ETLJob = etlJob };
    
    public static GetETLJobResult Failure(string errorMessage) =>
        new() { IsSuccess = false, ErrorMessage = errorMessage };
}
