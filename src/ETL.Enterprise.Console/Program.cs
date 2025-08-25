using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Serilog;
using MediatR;
using ETL.Enterprise.Infrastructure.Data;
using ETL.Enterprise.Infrastructure.Repositories;
using ETL.Enterprise.Domain.Repositories;
using ETL.Enterprise.Application.UseCases.Commands;
using ETL.Enterprise.Application.UseCases.Queries;
using ETL.Enterprise.Application.DTOs;
using ETL.Enterprise.Domain.Enums;
using ETL.Enterprise.Domain.Services;
using ETL.Enterprise.Infrastructure.ETL;
using ETL.Enterprise.Infrastructure.ETL.Engines;
using ETL.Enterprise.Infrastructure.Services;
using ETL.Enterprise.Application.Mapping;
using Microsoft.EntityFrameworkCore;
using AutoMapper;

using Microsoft.Extensions.Configuration;


namespace ETL.Enterprise.Console;

/// <summary>
/// Main entry point for the ETL Enterprise Console Application
/// </summary>
public class Program
{
    public static async Task Main(string[] args)
    {
        try
        {
            // Configure Serilog
            Log.Logger = new LoggerConfiguration()
                .WriteTo.Console()
                .WriteTo.File("logs/etl-enterprise-.txt", rollingInterval: RollingInterval.Day)
                .CreateLogger();

            Log.Information("Starting ETL Enterprise Console Application");

            // Create and configure the host
            var host = CreateHostBuilder(args).Build();

            // Run the application
            await host.RunAsync();
        }
        catch (Exception ex)
        {
            Log.Fatal(ex, "Application terminated unexpectedly");
        }
        finally
        {
            Log.CloseAndFlush();
        }
    }

    /// <summary>
    /// Creates and configures the host builder with all dependencies
    /// </summary>
    public static IHostBuilder CreateHostBuilder(string[] args) =>
        Host.CreateDefaultBuilder(args)
            .UseSerilog()
            .ConfigureServices((context, services) =>
            {
                // Configure Entity Framework
                services.AddDbContext<ETLDbContext>(options =>
                    options.UseSqlServer(context.Configuration.GetConnectionString("DefaultConnection")));

                // Register repositories
                services.AddScoped<IETLJobRepository, ETLJobRepository>();
                services.AddScoped<IETLJobLogRepository, ETLJobLogRepository>();

                // Register ETL engines
                services.AddScoped<CustomCSharpETLEngine>();
                services.AddScoped<ApacheSparkETLEngine>();
                services.AddScoped<SSISETLEngine>();

                // Register ETL services
                services.AddScoped<IETLEngineFactory, ETLEngineFactory>();
                services.AddScoped<IETLService, ETLService>();
                services.AddScoped<IDataExtractionService, DataExtractionService>();

                // Register MediatR
                services.AddMediatR(cfg => 
                {
                    cfg.RegisterServicesFromAssembly(typeof(CreateETLJobCommand).Assembly);
                });

                // Register AutoMapper
                services.AddSingleton<IMapper>(provider =>
                {
                    var config = new MapperConfiguration(cfg =>
                    {
                        cfg.AddMaps(typeof(ETLMappingProfile).Assembly);
                    });
                    return config.CreateMapper();
                });

                // Register application services
                services.AddScoped<ETLApplicationService>();

                // Register the main application
                services.AddHostedService<ETLConsoleService>();
            });
}

/// <summary>
/// Main ETL Console Service that orchestrates the application
/// </summary>
public class ETLConsoleService : BackgroundService
{
    private readonly ILogger<ETLConsoleService> _logger;
    private readonly ETLApplicationService _etlService;
    private readonly IMediator _mediator;

    public ETLConsoleService(
        ILogger<ETLConsoleService> logger,
        ETLApplicationService etlService,
        IMediator mediator)
    {
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));
        _etlService = etlService ?? throw new ArgumentNullException(nameof(etlService));
        _mediator = mediator ?? throw new ArgumentNullException(nameof(mediator));
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        _logger.LogInformation("ETL Console Service started");

        try
        {
            // Example: Create and execute a sample ETL job
            await RunSampleETLJob(stoppingToken);

            // Keep the service running
            while (!stoppingToken.IsCancellationRequested)
            {
                await Task.Delay(TimeSpan.FromMinutes(1), stoppingToken);
            }
        }
        catch (OperationCanceledException)
        {
            _logger.LogInformation("ETL Console Service is stopping");
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error in ETL Console Service");
        }
    }

    private async Task RunSampleETLJob(CancellationToken cancellationToken)
    {
        try
        {
            _logger.LogInformation("Creating sample ETL job");

            // Create a sample ETL job configuration with engine selection
            var configuration = new ETLJobConfigurationDto
            {
                Source = new DataSourceConfigurationDto
                {
                    ConnectionString = "Server=localhost;Database=SourceDB;Trusted_Connection=true;",
                    Query = "SELECT * FROM Customers",
                    Provider = "SqlServer",
                    CommandTimeout = 30
                },
                Target = new DataTargetConfigurationDto
                {
                    ConnectionString = "Server=localhost;Database=TargetDB;Trusted_Connection=true;",
                    TableName = "Customers",
                    Provider = "SqlServer",
                    LoadStrategy = LoadStrategy.Insert,
                    BatchSize = 1000
                },
                Engine = new ETLEngineConfigurationDto
                {
                    EngineType = ETLEngineType.CustomCSharp, // Can be changed to ApacheSpark or SSIS
                    AutoSelectEngine = true, // Will auto-select best engine based on configuration
                    EngineParameters = new Dictionary<string, string>
                    {
                        ["sparkMaster"] = "local[*]",
                        ["ssisCatalog"] = "SSISDB"
                    },
                    Resources = new ResourceAllocationDto
                    {
                        CpuCores = Environment.ProcessorCount,
                        MemoryMB = 1024,
                        Executors = 2,
                        ExecutorMemoryMB = 512
                    }
                },
                ErrorHandling = new ErrorHandlingConfigurationDto
                {
                    Strategy = ErrorHandlingStrategy.ContinueOnError,
                    MaxErrors = 100,
                    LogErrors = true
                },
                Performance = new PerformanceConfigurationDto
                {
                    MaxDegreeOfParallelism = Environment.ProcessorCount,
                    BufferSize = 8192,
                    MemoryLimitMB = 1024
                }
            };

            // Create the ETL job
            var createCommand = new CreateETLJobCommand
            {
                Name = "Sample Customer ETL Job",
                Description = "Sample ETL job to demonstrate enterprise patterns",
                Configuration = configuration,
                StartImmediately = true
            };

            var createResult = await _mediator.Send(createCommand, cancellationToken);

            if (createResult.IsSuccess && createResult.ETLJob != null)
            {
                _logger.LogInformation("Successfully created ETL job: {JobId}", createResult.ETLJob.Id);

                // Execute the job
                var executeCommand = new ExecuteETLJobCommand
                {
                    JobId = createResult.ETLJob.Id,
                    WaitForCompletion = true,
                    TimeoutMinutes = 30
                };

                var executeResult = await _mediator.Send(executeCommand, cancellationToken);

                if (executeResult.IsSuccess)
                {
                    _logger.LogInformation("ETL job executed successfully. Records processed: {RecordsProcessed}", 
                        executeResult.ETLJob?.RecordsProcessed);
                }
                else
                {
                    _logger.LogError("Failed to execute ETL job: {ErrorMessage}", executeResult.ErrorMessage);
                }
            }
            else
            {
                _logger.LogError("Failed to create ETL job: {Errors}", 
                    string.Join(", ", createResult.ValidationErrors));
            }
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error running sample ETL job");
        }
    }
}

/// <summary>
/// Application service that provides high-level ETL operations
/// </summary>
public class ETLApplicationService
{
    private readonly ILogger<ETLApplicationService> _logger;
    private readonly IETLJobRepository _etlJobRepository;
    private readonly IMediator _mediator;

    public ETLApplicationService(
        ILogger<ETLApplicationService> logger,
        IETLJobRepository etlJobRepository,
        IMediator mediator)
    {
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));
        _etlJobRepository = etlJobRepository ?? throw new ArgumentNullException(nameof(etlJobRepository));
        _mediator = mediator ?? throw new ArgumentNullException(nameof(mediator));
    }

    /// <summary>
    /// Gets all ETL jobs with optional filtering
    /// </summary>
    public async Task<GetETLJobsResult> GetETLJobsAsync(
        ETLJobStatus? status = null,
        string? namePattern = null,
        DateTime? fromDate = null,
        DateTime? toDate = null,
        int page = 1,
        int pageSize = 20,
        CancellationToken cancellationToken = default)
    {
        try
        {
            var query = new GetETLJobsQuery
            {
                Status = status,
                NamePattern = namePattern,
                FromDate = fromDate,
                ToDate = toDate,
                Page = page,
                PageSize = pageSize
            };

            return await _mediator.Send(query, cancellationToken);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error getting ETL jobs");
            return GetETLJobsResult.Failure(ex.Message);
        }
    }

    /// <summary>
    /// Gets a specific ETL job by ID
    /// </summary>
    public async Task<GetETLJobResult> GetETLJobAsync(Guid jobId, CancellationToken cancellationToken = default)
    {
        try
        {
            var query = new GetETLJobQuery
            {
                JobId = jobId,
                IncludeLogs = true,
                MaxLogs = 100
            };

            return await _mediator.Send(query, cancellationToken);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error getting ETL job: {JobId}", jobId);
            return GetETLJobResult.Failure(ex.Message);
        }
    }

    /// <summary>
    /// Creates a new ETL job
    /// </summary>
    public async Task<CreateETLJobResult> CreateETLJobAsync(
        string name,
        string description,
        ETLJobConfigurationDto configuration,
        bool startImmediately = false,
        CancellationToken cancellationToken = default)
    {
        try
        {
            var command = new CreateETLJobCommand
            {
                Name = name,
                Description = description,
                Configuration = configuration,
                StartImmediately = startImmediately
            };

            return await _mediator.Send(command, cancellationToken);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error creating ETL job: {JobName}", name);
            return CreateETLJobResult.Failure(ex.Message);
        }
    }

    /// <summary>
    /// Executes an ETL job
    /// </summary>
    public async Task<ExecuteETLJobResult> ExecuteETLJobAsync(
        Guid jobId,
        bool waitForCompletion = false,
        int timeoutMinutes = 30,
        CancellationToken cancellationToken = default)
    {
        try
        {
            var command = new ExecuteETLJobCommand
            {
                JobId = jobId,
                WaitForCompletion = waitForCompletion,
                TimeoutMinutes = timeoutMinutes
            };

            return await _mediator.Send(command, cancellationToken);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error executing ETL job: {JobId}", jobId);
            return ExecuteETLJobResult.Failure(ex.Message);
        }
    }
}
