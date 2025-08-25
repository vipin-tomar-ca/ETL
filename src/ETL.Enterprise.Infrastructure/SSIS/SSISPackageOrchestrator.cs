using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Configuration;
using System.Diagnostics;
using System.Text;
using System.Text.Json;
using System.Text.RegularExpressions;
using Microsoft.Data.SqlClient;

namespace ETL.Enterprise.Infrastructure.SSIS
{
    /// <summary>
    /// Orchestrates SSIS package execution with enhanced monitoring and scaling capabilities
    /// </summary>
    public class SSISPackageOrchestrator
    {
        private readonly ILogger<SSISPackageOrchestrator> _logger;
        private readonly IConfiguration _configuration;
        private readonly string _dtsExecPath;
        private readonly string _monitoringConnectionString;

        public SSISPackageOrchestrator(ILogger<SSISPackageOrchestrator> logger, IConfiguration configuration)
        {
            _logger = logger;
            _configuration = configuration;
            _dtsExecPath = _configuration["SSIS:DTExecPath"] ?? @"C:\Program Files\Microsoft SQL Server\150\DTS\Binn\DTExec.exe";
            _monitoringConnectionString = _configuration.GetConnectionString("MonitoringDB");
        }

        /// <summary>
        /// Executes an SSIS package with enhanced monitoring and error handling
        /// </summary>
        public async Task<PackageExecutionResult> ExecutePackageAsync(string packagePath, Dictionary<string, string> parameters)
        {
            var startTime = DateTime.UtcNow;
            var executionId = Guid.NewGuid().ToString();

            try
            {
                _logger.LogInformation($"Starting SSIS package execution: {packagePath}, ExecutionId: {executionId}");

                // Validate package exists
                if (!File.Exists(packagePath))
                {
                    throw new FileNotFoundException($"SSIS package not found: {packagePath}");
                }

                // Build DTExec command
                var arguments = BuildDTExecArguments(packagePath, parameters);
                
                // Log execution start
                await LogExecutionStartAsync(executionId, packagePath, parameters);
                
                // Execute package
                var result = await ExecuteDTExecAsync(arguments, executionId);
                
                // Calculate execution time
                result.ExecutionTime = DateTime.UtcNow - startTime;
                
                // Log execution completion
                await LogExecutionCompletionAsync(executionId, result);
                
                _logger.LogInformation($"SSIS package completed: {packagePath}, Exit Code: {result.ExitCode}, Duration: {result.ExecutionTime}");
                
                return result;
            }
            catch (Exception ex)
            {
                var executionTime = DateTime.UtcNow - startTime;
                _logger.LogError(ex, $"Error executing SSIS package: {packagePath}, ExecutionId: {executionId}");
                
                // Log execution failure
                await LogExecutionFailureAsync(executionId, packagePath, ex, executionTime);
                
                return new PackageExecutionResult
                {
                    ExitCode = -1,
                    Success = false,
                    Error = ex.Message,
                    ExecutionTime = executionTime
                };
            }
        }

        /// <summary>
        /// Executes multiple SSIS packages in parallel with controlled concurrency
        /// </summary>
        public async Task<List<PackageExecutionResult>> ExecutePackagesParallelAsync(
            List<PackageExecutionRequest> packages, 
            int maxConcurrency = 0)
        {
            if (maxConcurrency <= 0)
                maxConcurrency = Environment.ProcessorCount * 2;

            _logger.LogInformation($"Executing {packages.Count} packages in parallel with max concurrency: {maxConcurrency}");

            var semaphore = new SemaphoreSlim(maxConcurrency);
            var tasks = packages.Select(package => ExecutePackageWithSemaphoreAsync(package, semaphore));
            
            var results = await Task.WhenAll(tasks);
            return results.ToList();
        }

        /// <summary>
        /// Executes SSIS packages with dependency management
        /// </summary>
        public async Task<List<PackageExecutionResult>> ExecutePackagesWithDependenciesAsync(
            List<PackageExecutionRequest> packages)
        {
            var dependencyManager = new SSISDependencyManager(_logger);
            return await dependencyManager.ExecuteWithDependenciesAsync(packages);
        }

        /// <summary>
        /// Executes large datasets using chunking strategy
        /// </summary>
        public async Task<List<ChunkExecutionResult>> ExecuteChunkedProcessingAsync(
            string basePackagePath, 
            ChunkingConfiguration config)
        {
            var chunkingOrchestrator = new SSISChunkingOrchestrator(_logger, _configuration);
            return await chunkingOrchestrator.ExecuteChunkedProcessingAsync(basePackagePath, config);
        }

        private async Task<PackageExecutionResult> ExecutePackageWithSemaphoreAsync(
            PackageExecutionRequest request, 
            SemaphoreSlim semaphore)
        {
            await semaphore.WaitAsync();
            try
            {
                _logger.LogInformation($"Executing package: {request.PackagePath}");
                return await ExecutePackageAsync(request.PackagePath, request.Parameters);
            }
            finally
            {
                semaphore.Release();
            }
        }

        private string BuildDTExecArguments(string packagePath, Dictionary<string, string> parameters)
        {
            var args = new List<string>
            {
                $"/File \"{packagePath}\"",
                "/Reporter EWCDI", // Error, Warning, Custom, DataFlow, Information
                "/Reporting V", // Verbose
                "/ConsoleLog ON"
            };

            // Add parameters
            if (parameters != null)
            {
                foreach (var param in parameters)
                {
                    args.Add($"/Set \\Package.Variables[{param.Key}];{param.Value}");
                }
            }

            return string.Join(" ", args);
        }

        private async Task<PackageExecutionResult> ExecuteDTExecAsync(string arguments, string executionId)
        {
            var startInfo = new ProcessStartInfo
            {
                FileName = _dtsExecPath,
                Arguments = arguments,
                UseShellExecute = false,
                RedirectStandardOutput = true,
                RedirectStandardError = true,
                CreateNoWindow = true,
                StandardOutputEncoding = Encoding.UTF8,
                StandardErrorEncoding = Encoding.UTF8
            };

            using var process = new Process { StartInfo = startInfo };
            
            var outputBuilder = new StringBuilder();
            var errorBuilder = new StringBuilder();

            // Capture output
            process.OutputDataReceived += (sender, e) =>
            {
                if (e.Data != null)
                {
                    outputBuilder.AppendLine(e.Data);
                    _logger.LogDebug($"SSIS Output: {e.Data}");
                }
            };

            process.ErrorDataReceived += (sender, e) =>
            {
                if (e.Data != null)
                {
                    errorBuilder.AppendLine(e.Data);
                    _logger.LogWarning($"SSIS Error: {e.Data}");
                }
            };

            process.Start();
            process.BeginOutputReadLine();
            process.BeginErrorReadLine();

            await process.WaitForExitAsync();

            var output = outputBuilder.ToString();
            var error = errorBuilder.ToString();

            return new PackageExecutionResult
            {
                ExitCode = process.ExitCode,
                Output = output,
                Error = error,
                Success = process.ExitCode == 0,
                ExecutionId = executionId
            };
        }

        private async Task LogExecutionStartAsync(string executionId, string packagePath, Dictionary<string, string> parameters)
        {
            try
            {
                using var connection = new SqlConnection(_monitoringConnectionString);
                await connection.OpenAsync();

                var query = @"
                    INSERT INTO dbo.SSISExecutionHistory 
                    (ExecutionId, PackagePath, Status, StartTime, Parameters, CreatedDate)
                    VALUES 
                    (@ExecutionId, @PackagePath, 'Running', @StartTime, @Parameters, GETDATE())";

                await connection.ExecuteAsync(query, new
                {
                    ExecutionId = executionId,
                    PackagePath = packagePath,
                    StartTime = DateTime.UtcNow,
                    Parameters = JsonSerializer.Serialize(parameters)
                });
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "Failed to log execution start");
            }
        }

        private async Task LogExecutionCompletionAsync(string executionId, PackageExecutionResult result)
        {
            try
            {
                using var connection = new SqlConnection(_monitoringConnectionString);
                await connection.OpenAsync();

                var query = @"
                    UPDATE dbo.SSISExecutionHistory 
                    SET 
                        Status = @Status,
                        EndTime = @EndTime,
                        ExecutionTimeSeconds = @ExecutionTimeSeconds,
                        ExitCode = @ExitCode,
                        Output = @Output,
                        Error = @Error,
                        RecordsProcessed = @RecordsProcessed
                    WHERE ExecutionId = @ExecutionId";

                await connection.ExecuteAsync(query, new
                {
                    ExecutionId = executionId,
                    Status = result.Success ? "Success" : "Failed",
                    EndTime = DateTime.UtcNow,
                    ExecutionTimeSeconds = result.ExecutionTime.TotalSeconds,
                    ExitCode = result.ExitCode,
                    Output = result.Output,
                    Error = result.Error,
                    RecordsProcessed = ExtractRecordCount(result.Output)
                });
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "Failed to log execution completion");
            }
        }

        private async Task LogExecutionFailureAsync(string executionId, string packagePath, Exception ex, TimeSpan executionTime)
        {
            try
            {
                using var connection = new SqlConnection(_monitoringConnectionString);
                await connection.OpenAsync();

                var query = @"
                    UPDATE dbo.SSISExecutionHistory 
                    SET 
                        Status = 'Failed',
                        EndTime = @EndTime,
                        ExecutionTimeSeconds = @ExecutionTimeSeconds,
                        ExitCode = -1,
                        Error = @Error
                    WHERE ExecutionId = @ExecutionId";

                await connection.ExecuteAsync(query, new
                {
                    ExecutionId = executionId,
                    EndTime = DateTime.UtcNow,
                    ExecutionTimeSeconds = executionTime.TotalSeconds,
                    Error = ex.Message
                });
            }
            catch (Exception logEx)
            {
                _logger.LogWarning(logEx, "Failed to log execution failure");
            }
        }

        private int ExtractRecordCount(string output)
        {
            try
            {
                // Look for common SSIS output patterns that indicate record counts
                var patterns = new[]
                {
                    @"(\d+) rows copied",
                    @"Processed (\d+) rows",
                    @"Total rows: (\d+)",
                    @"Records processed: (\d+)"
                };

                foreach (var pattern in patterns)
                {
                    var match = Regex.Match(output, pattern, RegexOptions.IgnoreCase);
                    if (match.Success && int.TryParse(match.Groups[1].Value, out var count))
                    {
                        return count;
                    }
                }

                return 0;
            }
            catch
            {
                return 0;
            }
        }
    }

    /// <summary>
    /// Result of SSIS package execution
    /// </summary>
    public class PackageExecutionResult
    {
        public int ExitCode { get; set; }
        public string Output { get; set; }
        public string Error { get; set; }
        public bool Success { get; set; }
        public TimeSpan ExecutionTime { get; set; }
        public string ExecutionId { get; set; }
        public int RecordsProcessed { get; set; }
    }

    /// <summary>
    /// Request to execute an SSIS package
    /// </summary>
    public class PackageExecutionRequest
    {
        public string PackagePath { get; set; }
        public Dictionary<string, string> Parameters { get; set; }
        public int Priority { get; set; }
        public string DatabaseName { get; set; }
        public List<string> Dependencies { get; set; } = new List<string>();
    }

    /// <summary>
    /// Configuration for chunked processing
    /// </summary>
    public class ChunkingConfiguration
    {
        public string SourceQuery { get; set; }
        public int NumberOfChunks { get; set; }
        public string ChunkingStrategy { get; set; } = "Range";
        public int MaxConcurrentChunks { get; set; }
        public Dictionary<string, string> BaseParameters { get; set; }
    }

    /// <summary>
    /// Result of chunked processing
    /// </summary>
    public class ChunkExecutionResult
    {
        public int ChunkId { get; set; }
        public PackageExecutionResult PackageResult { get; set; }
        public int RecordsProcessed { get; set; }
        public TimeSpan ProcessingTime { get; set; }
    }
}
