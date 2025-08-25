using MediatR;
using ETL.Enterprise.Application.DTOs;
using ETL.Enterprise.Domain.Enums;

namespace ETL.Enterprise.Application.UseCases.Queries;

/// <summary>
/// Query to get multiple ETL jobs with filtering and pagination
/// </summary>
public class GetETLJobsQuery : IRequest<GetETLJobsResult>
{
    public ETLJobStatus? Status { get; set; }
    public string? NamePattern { get; set; }
    public DateTime? FromDate { get; set; }
    public DateTime? ToDate { get; set; }
    public int Page { get; set; } = 1;
    public int PageSize { get; set; } = 20;
}

/// <summary>
/// Result of getting multiple ETL jobs
/// </summary>
public class GetETLJobsResult
{
    public bool IsSuccess { get; set; }
    public List<ETLJobDto> ETLJobs { get; set; } = new();
    public int TotalCount { get; set; }
    public string? ErrorMessage { get; set; }
    
    public static GetETLJobsResult Success(List<ETLJobDto> etlJobs, int totalCount) =>
        new() { IsSuccess = true, ETLJobs = etlJobs, TotalCount = totalCount };
    
    public static GetETLJobsResult Failure(string errorMessage) =>
        new() { IsSuccess = false, ErrorMessage = errorMessage };
}
