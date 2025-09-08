using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace ETL.Enterprise.Infrastructure.SSIS
{
    /// <summary>
    /// Orchestrates chunked processing of large datasets using SSIS packages
    /// </summary>
    public class SSISChunkingOrchestrator
    {
        private readonly ILogger _logger;
        private readonly SSISPackageOrchestrator _packageOrchestrator;

        public SSISChunkingOrchestrator(ILogger logger, SSISPackageOrchestrator packageOrchestrator)
        {
            _logger = logger;
            _packageOrchestrator = packageOrchestrator;
        }

        /// <summary>
        /// Processes data in chunks using SSIS packages
        /// </summary>
        public async Task<ChunkingResult> ProcessDataInChunksAsync(
            string packagePath,
            Dictionary<string, string> baseParameters,
            ChunkingConfiguration configuration)
        {
            try
            {
                _logger.LogInformation($"Starting chunked processing with {configuration.TotalRecords} records, chunk size: {configuration.ChunkSize}");

                var results = new List<PackageExecutionResult>();
                var totalChunks = (int)Math.Ceiling((double)configuration.TotalRecords / configuration.ChunkSize);

                for (int chunkIndex = 0; chunkIndex < totalChunks; chunkIndex++)
                {
                    var startRecord = chunkIndex * configuration.ChunkSize;
                    var endRecord = Math.Min(startRecord + configuration.ChunkSize - 1, configuration.TotalRecords - 1);

                    var chunkParameters = new Dictionary<string, string>(baseParameters)
                    {
                        ["StartRecord"] = startRecord.ToString(),
                        ["EndRecord"] = endRecord.ToString(),
                        ["ChunkIndex"] = chunkIndex.ToString()
                    };

                    var result = await _packageOrchestrator.ExecutePackageAsync(packagePath, chunkParameters);
                    results.Add(result);

                    _logger.LogInformation($"Chunk {chunkIndex + 1}/{totalChunks} completed. Records {startRecord}-{endRecord}. Success: {result.Success}");

                    if (!result.Success)
                    {
                        _logger.LogError($"Chunk {chunkIndex + 1} failed: {result.Error}");
                        if (configuration.StopOnError)
                        {
                            break;
                        }
                    }

                    // Add delay between chunks if specified
                    if (configuration.DelayBetweenChunks > 0 && chunkIndex < totalChunks - 1)
                    {
                        await Task.Delay(configuration.DelayBetweenChunks);
                    }
                }

                var successCount = results.Count(r => r.Success);
                var failureCount = results.Count(r => !r.Success);

                _logger.LogInformation($"Chunked processing completed. Success: {successCount}, Failures: {failureCount}");

                return new ChunkingResult
                {
                    TotalChunks = totalChunks,
                    SuccessfulChunks = successCount,
                    FailedChunks = failureCount,
                    Results = results
                };
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error during chunked processing");
                throw;
            }
        }

        /// <summary>
        /// Executes chunked processing and returns detailed results
        /// </summary>
        public async Task<List<ChunkExecutionResult>> ExecuteChunkedProcessingAsync(
            string basePackagePath, 
            ChunkingConfiguration config)
        {
            try
            {
                _logger.LogInformation($"Starting chunked processing with {config.TotalRecords} records, chunk size: {config.ChunkSize}");

                var results = new List<ChunkExecutionResult>();
                var totalChunks = (int)Math.Ceiling((double)config.TotalRecords / config.ChunkSize);

                for (int chunkIndex = 0; chunkIndex < totalChunks; chunkIndex++)
                {
                    var startRecord = chunkIndex * config.ChunkSize;
                    var endRecord = Math.Min(startRecord + config.ChunkSize - 1, config.TotalRecords - 1);

                    var chunkParameters = new Dictionary<string, string>(config.BaseParameters)
                    {
                        ["StartRecord"] = startRecord.ToString(),
                        ["EndRecord"] = endRecord.ToString(),
                        ["ChunkIndex"] = chunkIndex.ToString()
                    };

                    var startTime = DateTime.UtcNow;
                    var result = await _packageOrchestrator.ExecutePackageAsync(basePackagePath, chunkParameters);
                    var processingTime = DateTime.UtcNow - startTime;

                    var chunkResult = new ChunkExecutionResult
                    {
                        ChunkId = chunkIndex,
                        PackageResult = result,
                        RecordsProcessed = endRecord - startRecord + 1,
                        ProcessingTime = processingTime
                    };

                    results.Add(chunkResult);

                    _logger.LogInformation($"Chunk {chunkIndex + 1}/{totalChunks} completed. Records {startRecord}-{endRecord}. Success: {result.Success}");

                    if (!result.Success)
                    {
                        _logger.LogError($"Chunk {chunkIndex + 1} failed: {result.Error}");
                        if (config.StopOnError)
                        {
                            break;
                        }
                    }

                    // Add delay between chunks if specified
                    if (config.DelayBetweenChunks > 0 && chunkIndex < totalChunks - 1)
                    {
                        await Task.Delay(config.DelayBetweenChunks);
                    }
                }

                var successCount = results.Count(r => r.PackageResult.Success);
                var failureCount = results.Count(r => !r.PackageResult.Success);

                _logger.LogInformation($"Chunked processing completed. Success: {successCount}, Failures: {failureCount}");

                return results;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error during chunked processing");
                throw;
            }
        }
    }

    /// <summary>
    /// Result of chunked processing
    /// </summary>
    public class ChunkingResult
    {
        public int TotalChunks { get; set; }
        public int SuccessfulChunks { get; set; }
        public int FailedChunks { get; set; }
        public List<PackageExecutionResult> Results { get; set; } = new List<PackageExecutionResult>();
    }

    /// <summary>
    /// Result of individual chunk execution
    /// </summary>
    public class ChunkExecutionResult
    {
        public int ChunkId { get; set; }
        public PackageExecutionResult PackageResult { get; set; } = new PackageExecutionResult();
        public int RecordsProcessed { get; set; }
        public TimeSpan ProcessingTime { get; set; }
    }
}
