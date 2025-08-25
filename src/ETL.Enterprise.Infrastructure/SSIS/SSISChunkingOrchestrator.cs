using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace ETL.Enterprise.Infrastructure.SSIS
{
    /// <summary>
    /// Orchestrates chunked processing of large datasets using SSIS packages
    /// </summary>
    public class SSISChunkingOrchestrator
    {
        private readonly ILogger<SSISChunkingOrchestrator> _logger;
        private readonly SSISPackageOrchestrator _packageOrchestrator;

        public SSISChunkingOrchestrator(ILogger<SSISChunkingOrchestrator> logger, SSISPackageOrchestrator packageOrchestrator)
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
}
