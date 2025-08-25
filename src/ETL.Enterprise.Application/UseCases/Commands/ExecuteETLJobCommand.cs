using MediatR;
using ETL.Enterprise.Application.DTOs;

namespace ETL.Enterprise.Application.UseCases.Commands;

/// <summary>
/// Command to execute an ETL job
/// </summary>
public class ExecuteETLJobCommand : IRequest<ExecuteETLJobResult>
{
    public Guid JobId { get; set; }
    public bool WaitForCompletion { get; set; } = false;
    public int TimeoutMinutes { get; set; } = 30;
}

/// <summary>
/// Result of executing an ETL job
/// </summary>
public class ExecuteETLJobResult
{
    public bool IsSuccess { get; set; }
    public ETLJobDto? ETLJob { get; set; }
    public string? ErrorMessage { get; set; }
    public bool IsCompleted { get; set; }
    public TimeSpan ExecutionTime { get; set; }
    
    public static ExecuteETLJobResult Success(ETLJobDto etlJob, bool isCompleted, TimeSpan executionTime) =>
        new() { IsSuccess = true, ETLJob = etlJob, IsCompleted = isCompleted, ExecutionTime = executionTime };
    
    public static ExecuteETLJobResult Failure(string errorMessage) =>
        new() { IsSuccess = false, ErrorMessage = errorMessage };
}
