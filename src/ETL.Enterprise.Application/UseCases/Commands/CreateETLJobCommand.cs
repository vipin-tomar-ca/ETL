using MediatR;
using ETL.Enterprise.Application.DTOs;

namespace ETL.Enterprise.Application.UseCases.Commands;

/// <summary>
/// Command to create a new ETL job
/// </summary>
public class CreateETLJobCommand : IRequest<CreateETLJobResult>
{
    public string Name { get; set; } = string.Empty;
    public string Description { get; set; } = string.Empty;
    public ETLJobConfigurationDto Configuration { get; set; } = new();
    public bool StartImmediately { get; set; } = false;
}

/// <summary>
/// Result of creating an ETL job
/// </summary>
public class CreateETLJobResult
{
    public bool IsSuccess { get; set; }
    public ETLJobDto? ETLJob { get; set; }
    public string? ErrorMessage { get; set; }
    public List<string> ValidationErrors { get; set; } = new();
    
    public static CreateETLJobResult Success(ETLJobDto etlJob) =>
        new() { IsSuccess = true, ETLJob = etlJob };
    
    public static CreateETLJobResult Failure(string errorMessage) =>
        new() { IsSuccess = false, ErrorMessage = errorMessage };
    
    public static CreateETLJobResult ValidationFailure(List<string> errors) =>
        new() { IsSuccess = false, ValidationErrors = errors };
}
