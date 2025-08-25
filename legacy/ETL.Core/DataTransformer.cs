using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using System.Threading.Tasks;
using System.Threading;
using Microsoft.Extensions.Logging;
using System.Text.RegularExpressions;

namespace ETL.Core
{
    /// <summary>
    /// Enterprise-grade data transformer for handling large-scale ETL operations
    /// Supports batch processing, parallel execution, and comprehensive error handling
    /// Designed to handle 1 TB/day across multiple databases
    /// </summary>
    public class DataTransformer : IDisposable
    {
        private readonly string _connectionString;
        private readonly ILogger<DataTransformer> _logger;
        private readonly int _batchSize;
        private readonly int _maxDegreeOfParallelism;
        private readonly SemaphoreSlim _semaphore;
        private bool _disposed = false;

        /// <summary>
        /// Initializes a new instance of the DataTransformer class
        /// </summary>
        /// <param name="connectionString">SQL Server connection string</param>
        /// <param name="logger">Logger instance for error tracking</param>
        /// <param name="batchSize">Number of records to process per batch (default: 100,000)</param>
        /// <param name="maxDegreeOfParallelism">Maximum number of parallel operations (default: Environment.ProcessorCount)</param>
        public DataTransformer(string connectionString, ILogger<DataTransformer> logger, 
            int batchSize = 100000, int maxDegreeOfParallelism = 0)
        {
            _connectionString = connectionString ?? throw new ArgumentNullException(nameof(connectionString));
            _logger = logger ?? throw new ArgumentNullException(nameof(logger));
            _batchSize = batchSize > 0 ? batchSize : throw new ArgumentException("Batch size must be greater than 0", nameof(batchSize));
            _maxDegreeOfParallelism = maxDegreeOfParallelism > 0 ? maxDegreeOfParallelism : Environment.ProcessorCount;
            _semaphore = new SemaphoreSlim(_maxDegreeOfParallelism, _maxDegreeOfParallelism);
        }

        /// <summary>
        /// Transforms data from multiple source databases in parallel batches
        /// </summary>
        /// <param name="sourceDatabases">List of source database configurations</param>
        /// <param name="transformationRules">Rules to apply during transformation</param>
        /// <param name="cancellationToken">Cancellation token for operation control</param>
        /// <returns>Transformation result with statistics</returns>
        public async Task<TransformationResult> TransformDataAsync(
            List<SourceDatabaseConfig> sourceDatabases,
            TransformationRules transformationRules,
            CancellationToken cancellationToken = default)
        {
            var result = new TransformationResult
            {
                StartTime = DateTime.UtcNow,
                SourceDatabases = sourceDatabases.Count
            };

            try
            {
                _logger.LogInformation("Starting data transformation for {DatabaseCount} databases", sourceDatabases.Count);

                // Process each database in parallel
                var databaseTasks = sourceDatabases.Select(async dbConfig =>
                {
                    await _semaphore.WaitAsync(cancellationToken);
                    try
                    {
                        return await ProcessDatabaseAsync(dbConfig, transformationRules, cancellationToken);
                    }
                    finally
                    {
                        _semaphore.Release();
                    }
                });

                var databaseResults = await Task.WhenAll(databaseTasks);

                // Aggregate results
                result.ProcessedRecords = databaseResults.Sum(r => r.ProcessedRecords);
                result.TransformedRecords = databaseResults.Sum(r => r.TransformedRecords);
                result.Errors = databaseResults.SelectMany(r => r.Errors).ToList();
                result.Success = !result.Errors.Any(e => e.Severity == ErrorSeverity.Critical);

                result.EndTime = DateTime.UtcNow;
                result.Duration = result.EndTime - result.StartTime;

                await LogTransformationResultAsync(result, cancellationToken);

                _logger.LogInformation("Data transformation completed. Processed {ProcessedRecords} records in {Duration}",
                    result.ProcessedRecords, result.Duration);

                return result;
            }
            catch (Exception ex)
            {
                await LogErrorAsync("TransformDataAsync", ex, cancellationToken);
                throw;
            }
        }

        /// <summary>
        /// Processes a single database with batch processing
        /// </summary>
        private async Task<DatabaseTransformationResult> ProcessDatabaseAsync(
            SourceDatabaseConfig dbConfig,
            TransformationRules rules,
            CancellationToken cancellationToken)
        {
            var dbResult = new DatabaseTransformationResult
            {
                DatabaseName = dbConfig.DatabaseName,
                StartTime = DateTime.UtcNow
            };

            try
            {
                using var connection = new SqlConnection(dbConfig.ConnectionString);
                await connection.OpenAsync(cancellationToken);

                // Get total record count for progress tracking
                var totalRecords = await GetRecordCountAsync(connection, dbConfig.SourceTable, cancellationToken);
                dbResult.TotalRecords = totalRecords;

                // Process in batches
                var offset = 0;
                var batchTasks = new List<Task<BatchResult>>();

                while (offset < totalRecords && !cancellationToken.IsCancellationRequested)
                {
                    var batchTask = ProcessBatchAsync(connection, dbConfig, rules, offset, _batchSize, cancellationToken);
                    batchTasks.Add(batchTask);
                    offset += _batchSize;

                    // Limit concurrent batches to prevent memory issues
                    if (batchTasks.Count >= _maxDegreeOfParallelism)
                    {
                        var completedBatch = await Task.WhenAny(batchTasks);
                        batchTasks.Remove(completedBatch);
                        var batchResult = await completedBatch;
                        dbResult.ProcessedRecords += batchResult.ProcessedRecords;
                        dbResult.TransformedRecords += batchResult.TransformedRecords;
                        dbResult.Errors.AddRange(batchResult.Errors);
                    }
                }

                // Wait for remaining batches
                var remainingBatches = await Task.WhenAll(batchTasks);
                foreach (var batchResult in remainingBatches)
                {
                    dbResult.ProcessedRecords += batchResult.ProcessedRecords;
                    dbResult.TransformedRecords += batchResult.TransformedRecords;
                    dbResult.Errors.AddRange(batchResult.Errors);
                }

                dbResult.EndTime = DateTime.UtcNow;
                dbResult.Duration = dbResult.EndTime - dbResult.StartTime;

                return dbResult;
            }
            catch (Exception ex)
            {
                await LogErrorAsync($"ProcessDatabaseAsync_{dbConfig.DatabaseName}", ex, cancellationToken);
                dbResult.Errors.Add(new TransformationError
                {
                    Message = ex.Message,
                    Severity = ErrorSeverity.Critical,
                    DatabaseName = dbConfig.DatabaseName,
                    Timestamp = DateTime.UtcNow
                });
                return dbResult;
            }
        }

        /// <summary>
        /// Processes a single batch of records
        /// </summary>
        private async Task<BatchResult> ProcessBatchAsync(
            SqlConnection connection,
            SourceDatabaseConfig dbConfig,
            TransformationRules rules,
            int offset,
            int batchSize,
            CancellationToken cancellationToken)
        {
            var batchResult = new BatchResult();

            try
            {
                // Fetch batch data
                var data = await FetchBatchAsync(connection, dbConfig.SourceTable, offset, batchSize, cancellationToken);
                batchResult.ProcessedRecords = data.Count;

                if (data.Count == 0) return batchResult;

                // Apply transformations
                var transformedData = await TransformBatchAsync(data, rules, cancellationToken);
                batchResult.TransformedRecords = transformedData.Count;

                // Save transformed data to staging table
                await SaveToStagingAsync(transformedData, cancellationToken);

                return batchResult;
            }
            catch (Exception ex)
            {
                batchResult.Errors.Add(new TransformationError
                {
                    Message = $"Batch processing failed at offset {offset}: {ex.Message}",
                    Severity = ErrorSeverity.Error,
                    DatabaseName = dbConfig.DatabaseName,
                    Timestamp = DateTime.UtcNow
                });
                return batchResult;
            }
        }

        /// <summary>
        /// Fetches a batch of records from the source table
        /// </summary>
        private async Task<List<DataRow>> FetchBatchAsync(
            SqlConnection connection,
            string sourceTable,
            int offset,
            int batchSize,
            CancellationToken cancellationToken)
        {
            var data = new List<DataRow>();

            using var command = new SqlCommand(
                $"SELECT * FROM {sourceTable} ORDER BY (SELECT NULL) OFFSET @Offset ROWS FETCH NEXT @BatchSize ROWS ONLY",
                connection);

            command.Parameters.AddWithValue("@Offset", offset);
            command.Parameters.AddWithValue("@BatchSize", batchSize);

            using var reader = await command.ExecuteReaderAsync(cancellationToken);
            var dataTable = new DataTable();
            dataTable.Load(reader);

            foreach (DataRow row in dataTable.Rows)
            {
                data.Add(row);
            }

            return data;
        }

        /// <summary>
        /// Applies transformation rules to a batch of data
        /// </summary>
        private async Task<List<TransformedRecord>> TransformBatchAsync(
            List<DataRow> sourceData,
            TransformationRules rules,
            CancellationToken cancellationToken)
        {
            var transformedData = new List<TransformedRecord>();

            // Process records in parallel within the batch
            var transformTasks = sourceData.Select(async row =>
            {
                try
                {
                    return await TransformRecordAsync(row, rules, cancellationToken);
                }
                catch (Exception ex)
                {
                    await LogErrorAsync("TransformRecordAsync", ex, cancellationToken);
                    return null;
                }
            });

            var results = await Task.WhenAll(transformTasks);
            transformedData.AddRange(results.Where(r => r != null)!);

            return transformedData;
        }

        /// <summary>
        /// Transforms a single record according to the specified rules
        /// </summary>
        private async Task<TransformedRecord> TransformRecordAsync(
            DataRow sourceRow,
            TransformationRules rules,
            CancellationToken cancellationToken)
        {
            var transformedRecord = new TransformedRecord();

            try
            {
                // Apply string cleaning rules
                if (rules.CleanStrings)
                {
                    foreach (DataColumn column in sourceRow.Table.Columns)
                    {
                        if (column.DataType == typeof(string) && !sourceRow.IsNull(column))
                        {
                            var originalValue = sourceRow[column].ToString();
                            var cleanedValue = CleanString(originalValue ?? string.Empty, rules.StringCleaningOptions);
                            transformedRecord.Data[column.ColumnName] = cleanedValue;
                        }
                        else
                        {
                            transformedRecord.Data[column.ColumnName] = sourceRow[column];
                        }
                    }
                }
                else
                {
                    // Copy all data without transformation
                    foreach (DataColumn column in sourceRow.Table.Columns)
                    {
                        transformedRecord.Data[column.ColumnName] = sourceRow[column];
                    }
                }

                // Apply custom transformations
                if (rules.CustomTransformations != null)
                {
                    foreach (var transformation in rules.CustomTransformations)
                    {
                        await transformation(transformedRecord, cancellationToken);
                    }
                }

                transformedRecord.TransformationTimestamp = DateTime.UtcNow;
                return transformedRecord;
            }
            catch (Exception ex)
            {
                await LogErrorAsync("TransformRecordAsync", ex, cancellationToken);
                throw;
            }
        }

        /// <summary>
        /// Cleans string data according to specified options
        /// </summary>
        private string CleanString(string input, StringCleaningOptions options)
        {
            if (string.IsNullOrEmpty(input)) return input;

            var result = input;

            if (options.TrimWhitespace)
                result = result.Trim();

            if (options.RemoveSpecialCharacters)
                result = Regex.Replace(result, @"[^\w\s]", "");

            if (options.NormalizeCase)
                result = result.ToLowerInvariant();

            if (options.RemoveExtraSpaces)
                result = Regex.Replace(result, @"\s+", " ");

            if (options.MaxLength > 0 && result.Length > options.MaxLength)
                result = result.Substring(0, options.MaxLength);

            return result;
        }

        /// <summary>
        /// Saves transformed data to the staging table
        /// </summary>
        private async Task SaveToStagingAsync(
            List<TransformedRecord> transformedData,
            CancellationToken cancellationToken)
        {
            if (!transformedData.Any()) return;

            using var connection = new SqlConnection(_connectionString);
            await connection.OpenAsync(cancellationToken);

            using var transaction = connection.BeginTransaction();

            try
            {
                // Use bulk insert for better performance
                using var bulkCopy = new SqlBulkCopy(connection, SqlBulkCopyOptions.Default, transaction)
                {
                    DestinationTableName = "StagingTable"
                };

                var dataTable = ConvertToDataTable(transformedData);
                await bulkCopy.WriteToServerAsync(dataTable, cancellationToken);

                await transaction.CommitAsync(cancellationToken);
            }
            catch
            {
                await transaction.RollbackAsync(cancellationToken);
                throw;
            }
        }

        /// <summary>
        /// Converts transformed records to DataTable for bulk insert
        /// </summary>
        private DataTable ConvertToDataTable(List<TransformedRecord> records)
        {
            var dataTable = new DataTable();

            if (!records.Any()) return dataTable;

            // Create columns based on the first record
            var firstRecord = records.First();
            foreach (var kvp in firstRecord.Data)
            {
                dataTable.Columns.Add(kvp.Key, kvp.Value?.GetType() ?? typeof(object));
            }

            // Add transformation timestamp column
            dataTable.Columns.Add("TransformationTimestamp", typeof(DateTime));

            // Add rows
            foreach (var record in records)
            {
                var row = dataTable.NewRow();
                foreach (var kvp in record.Data)
                {
                    row[kvp.Key] = kvp.Value ?? DBNull.Value;
                }
                row["TransformationTimestamp"] = record.TransformationTimestamp;
                dataTable.Rows.Add(row);
            }

            return dataTable;
        }

        /// <summary>
        /// Gets the total record count for a table
        /// </summary>
        private async Task<long> GetRecordCountAsync(
            SqlConnection connection,
            string tableName,
            CancellationToken cancellationToken)
        {
            using var command = new SqlCommand($"SELECT COUNT(*) FROM {tableName}", connection);
            var result = await command.ExecuteScalarAsync(cancellationToken);
            return Convert.ToInt64(result);
        }

        /// <summary>
        /// Logs transformation results to the database
        /// </summary>
        private async Task LogTransformationResultAsync(
            TransformationResult result,
            CancellationToken cancellationToken)
        {
            using var connection = new SqlConnection(_connectionString);
            await connection.OpenAsync(cancellationToken);

            using var command = new SqlCommand(@"
                INSERT INTO Logs (Operation, StartTime, EndTime, Duration, ProcessedRecords, 
                                 TransformedRecords, ErrorCount, Success, Details)
                VALUES (@Operation, @StartTime, @EndTime, @Duration, @ProcessedRecords,
                        @TransformedRecords, @ErrorCount, @Success, @Details)", connection);

            command.Parameters.AddWithValue("@Operation", "DataTransformation");
            command.Parameters.AddWithValue("@StartTime", result.StartTime);
            command.Parameters.AddWithValue("@EndTime", result.EndTime);
            command.Parameters.AddWithValue("@Duration", result.Duration.TotalMilliseconds);
            command.Parameters.AddWithValue("@ProcessedRecords", result.ProcessedRecords);
            command.Parameters.AddWithValue("@TransformedRecords", result.TransformedRecords);
            command.Parameters.AddWithValue("@ErrorCount", result.Errors.Count);
            command.Parameters.AddWithValue("@Success", result.Success);
            command.Parameters.AddWithValue("@Details", $"Processed {result.SourceDatabases} databases");

            await command.ExecuteNonQueryAsync(cancellationToken);
        }

        /// <summary>
        /// Logs errors to the database
        /// </summary>
        private async Task LogErrorAsync(
            string operation,
            Exception exception,
            CancellationToken cancellationToken)
        {
            try
            {
                using var connection = new SqlConnection(_connectionString);
                await connection.OpenAsync(cancellationToken);

                using var command = new SqlCommand(@"
                    INSERT INTO Logs (Operation, StartTime, ErrorMessage, StackTrace, Success)
                    VALUES (@Operation, @StartTime, @ErrorMessage, @StackTrace, @Success)", connection);

                command.Parameters.AddWithValue("@Operation", operation);
                command.Parameters.AddWithValue("@StartTime", DateTime.UtcNow);
                command.Parameters.AddWithValue("@ErrorMessage", exception.Message);
                command.Parameters.AddWithValue("@StackTrace", exception.StackTrace);
                command.Parameters.AddWithValue("@Success", false);

                await command.ExecuteNonQueryAsync(cancellationToken);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Failed to log error to database");
            }
        }

        /// <summary>
        /// Disposes resources
        /// </summary>
        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        /// <summary>
        /// Disposes resources
        /// </summary>
        protected virtual void Dispose(bool disposing)
        {
            if (!_disposed && disposing)
            {
                _semaphore?.Dispose();
                _disposed = true;
            }
        }
    }

    #region Supporting Classes

    /// <summary>
    /// Configuration for a source database
    /// </summary>
    public class SourceDatabaseConfig
    {
        public string DatabaseName { get; set; } = string.Empty;
        public string ConnectionString { get; set; } = string.Empty;
        public string SourceTable { get; set; } = string.Empty;
    }

    /// <summary>
    /// Rules for data transformation
    /// </summary>
    public class TransformationRules
    {
        public bool CleanStrings { get; set; } = true;
        public StringCleaningOptions StringCleaningOptions { get; set; } = new StringCleaningOptions();
        public List<Func<TransformedRecord, CancellationToken, Task>>? CustomTransformations { get; set; }
    }

    /// <summary>
    /// Options for string cleaning
    /// </summary>
    public class StringCleaningOptions
    {
        public bool TrimWhitespace { get; set; } = true;
        public bool RemoveSpecialCharacters { get; set; } = false;
        public bool NormalizeCase { get; set; } = false;
        public bool RemoveExtraSpaces { get; set; } = true;
        public int MaxLength { get; set; } = 0;
    }

    /// <summary>
    /// Result of a data transformation operation
    /// </summary>
    public class TransformationResult
    {
        public DateTime StartTime { get; set; }
        public DateTime EndTime { get; set; }
        public TimeSpan Duration { get; set; }
        public long ProcessedRecords { get; set; }
        public long TransformedRecords { get; set; }
        public int SourceDatabases { get; set; }
        public bool Success { get; set; }
        public List<TransformationError> Errors { get; set; } = new List<TransformationError>();
    }

    /// <summary>
    /// Result of processing a single database
    /// </summary>
    public class DatabaseTransformationResult
    {
        public string DatabaseName { get; set; } = string.Empty;
        public DateTime StartTime { get; set; }
        public DateTime EndTime { get; set; }
        public TimeSpan Duration { get; set; }
        public long TotalRecords { get; set; }
        public long ProcessedRecords { get; set; }
        public long TransformedRecords { get; set; }
        public List<TransformationError> Errors { get; set; } = new List<TransformationError>();
    }

    /// <summary>
    /// Result of processing a single batch
    /// </summary>
    public class BatchResult
    {
        public int ProcessedRecords { get; set; }
        public int TransformedRecords { get; set; }
        public List<TransformationError> Errors { get; set; } = new List<TransformationError>();
    }

    /// <summary>
    /// A transformed record
    /// </summary>
    public class TransformedRecord
    {
        public Dictionary<string, object> Data { get; set; } = new Dictionary<string, object>();
        public DateTime TransformationTimestamp { get; set; }
    }

    /// <summary>
    /// Represents a transformation error
    /// </summary>
    public class TransformationError
    {
        public string Message { get; set; } = string.Empty;
        public ErrorSeverity Severity { get; set; }
        public string DatabaseName { get; set; } = string.Empty;
        public DateTime Timestamp { get; set; }
    }

    /// <summary>
    /// Error severity levels
    /// </summary>
    public enum ErrorSeverity
    {
        Warning,
        Error,
        Critical
    }

    #endregion
}
