using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.SqlServer.Dts.Pipeline.Wrapper;
using Microsoft.SqlServer.Dts.Runtime.Wrapper;

namespace ETL.Scalable.Transform
{
    /// <summary>
    /// SSIS Script Component for high-throughput data transformations
    /// Optimized for 1 TB/day throughput with parallel batch processing
    /// </summary>
    [Microsoft.SqlServer.Dts.Pipeline.SSISScriptComponentEntryPointAttribute]
    public class ScriptMain : UserComponent
    {
        // Configuration constants for performance tuning
        private const int BATCH_SIZE = 100000; // Rows per batch for optimal throughput
        private const int MAX_CONCURRENT_BATCHES = 4; // Parallel batch processing
        private const int COMMAND_TIMEOUT = 300; // 5 minutes timeout for long-running operations
        private const string STAGING_TABLE_NAME = "staging_transformed_data";
        
        // Connection string for staging database
        private string _connectionString;
        private List<TransformedRow> _batchBuffer;
        private readonly object _batchLock = new object();
        private int _totalRowsProcessed = 0;
        private int _totalErrors = 0;
        private DateTime _startTime;

        /// <summary>
        /// Represents a transformed data row with all necessary fields
        /// </summary>
        private class TransformedRow
        {
            public int SourceId { get; set; }
            public string TransformedString { get; set; }
            public DateTime ProcessedDate { get; set; }
            public string ErrorMessage { get; set; }
            public bool HasError { get; set; }
            // Add additional fields as needed for your specific transformation
        }

        /// <summary>
        /// Initialize the script component
        /// </summary>
        public override void PreExecute()
        {
            base.PreExecute();
            
            try
            {
                _startTime = DateTime.Now;
                _batchBuffer = new List<TransformedRow>();
                
                // Get connection string from SSIS connection manager
                _connectionString = Connections["StagingConnection"].ConnectionString;
                
                // Log component initialization
                ComponentMetaData.FireInformation(0, "SSIS_Transform", 
                    $"Script Component initialized. Batch size: {BATCH_SIZE}, Max concurrent batches: {MAX_CONCURRENT_BATCHES}", 
                    "", 0, ref ComponentMetaData.FireInformation);
            }
            catch (Exception ex)
            {
                ComponentMetaData.FireError(0, "SSIS_Transform", 
                    $"Failed to initialize script component: {ex.Message}", 
                    "", 0, out _);
                throw;
            }
        }

        /// <summary>
        /// Process each input row and apply transformations
        /// </summary>
        public override void Input0_ProcessInputRow(Input0Buffer Row)
        {
            try
            {
                // Create transformed row object
                var transformedRow = new TransformedRow
                {
                    SourceId = Row.SourceId,
                    TransformedString = TransformString(Row.InputString),
                    ProcessedDate = DateTime.Now,
                    HasError = false,
                    ErrorMessage = null
                };

                // Add to batch buffer
                lock (_batchLock)
                {
                    _batchBuffer.Add(transformedRow);
                    
                    // Process batch when buffer is full
                    if (_batchBuffer.Count >= BATCH_SIZE)
                    {
                        var batchToProcess = new List<TransformedRow>(_batchBuffer);
                        _batchBuffer.Clear();
                        
                        // Process batch asynchronously to avoid blocking the data flow
                        Task.Run(() => ProcessBatchAsync(batchToProcess));
                    }
                }
            }
            catch (Exception ex)
            {
                _totalErrors++;
                ComponentMetaData.FireError(0, "SSIS_Transform", 
                    $"Error processing row {Row.SourceId}: {ex.Message}", 
                    "", 0, out _);
                
                // Add error row to batch for logging
                var errorRow = new TransformedRow
                {
                    SourceId = Row.SourceId,
                    TransformedString = null,
                    ProcessedDate = DateTime.Now,
                    HasError = true,
                    ErrorMessage = ex.Message
                };
                
                lock (_batchLock)
                {
                    _batchBuffer.Add(errorRow);
                }
            }
        }

        /// <summary>
        /// Apply string transformations (trim, uppercase, handle NULLs)
        /// </summary>
        private string TransformString(string inputString)
        {
            if (string.IsNullOrWhiteSpace(inputString))
            {
                return string.Empty; // Handle NULL/empty strings
            }
            
            // Apply transformations: trim whitespace and convert to uppercase
            return inputString.Trim().ToUpperInvariant();
        }

        /// <summary>
        /// Process a batch of transformed rows asynchronously
        /// </summary>
        private async Task ProcessBatchAsync(List<TransformedRow> batch)
        {
            try
            {
                using (var connection = new SqlConnection(_connectionString))
                {
                    await connection.OpenAsync();
                    
                    // Create staging table if it doesn't exist
                    await CreateStagingTableIfNotExistsAsync(connection);
                    
                    // Bulk insert the batch data
                    await BulkInsertBatchAsync(connection, batch);
                }
                
                // Update statistics
                lock (_batchLock)
                {
                    _totalRowsProcessed += batch.Count;
                }
                
                // Log batch completion
                ComponentMetaData.FireInformation(0, "SSIS_Transform", 
                    $"Processed batch of {batch.Count} rows. Total processed: {_totalRowsProcessed}", 
                    "", 0, ref ComponentMetaData.FireInformation);
            }
            catch (Exception ex)
            {
                ComponentMetaData.FireError(0, "SSIS_Transform", 
                    $"Error processing batch: {ex.Message}", 
                    "", 0, out _);
                
                // Fallback: process rows individually with retry logic
                await ProcessBatchWithRetryAsync(batch);
            }
        }

        /// <summary>
        /// Create staging table if it doesn't exist
        /// </summary>
        private async Task CreateStagingTableIfNotExistsAsync(SqlConnection connection)
        {
            var createTableSql = $@"
                IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[{STAGING_TABLE_NAME}]') AND type in (N'U'))
                BEGIN
                    CREATE TABLE [dbo].[{STAGING_TABLE_NAME}] (
                        [Id] INT IDENTITY(1,1) PRIMARY KEY,
                        [SourceId] INT NOT NULL,
                        [TransformedString] NVARCHAR(MAX) NULL,
                        [ProcessedDate] DATETIME2 NOT NULL,
                        [ErrorMessage] NVARCHAR(MAX) NULL,
                        [HasError] BIT NOT NULL DEFAULT(0),
                        [BatchId] UNIQUEIDENTIFIER NOT NULL,
                        [CreatedDate] DATETIME2 NOT NULL DEFAULT(GETUTCDATE())
                    )
                    
                    -- Create indexes for performance
                    CREATE NONCLUSTERED INDEX IX_{STAGING_TABLE_NAME}_SourceId ON [dbo].[{STAGING_TABLE_NAME}] ([SourceId])
                    CREATE NONCLUSTERED INDEX IX_{STAGING_TABLE_NAME}_ProcessedDate ON [dbo].[{STAGING_TABLE_NAME}] ([ProcessedDate])
                    CREATE NONCLUSTERED INDEX IX_{STAGING_TABLE_NAME}_HasError ON [dbo].[{STAGING_TABLE_NAME}] ([HasError])
                END";
            
            using (var command = new SqlCommand(createTableSql, connection))
            {
                command.CommandTimeout = COMMAND_TIMEOUT;
                await command.ExecuteNonQueryAsync();
            }
        }

        /// <summary>
        /// Bulk insert a batch of transformed rows
        /// </summary>
        private async Task BulkInsertBatchAsync(SqlConnection connection, List<TransformedRow> batch)
        {
            var batchId = Guid.NewGuid();
            var dataTable = new DataTable();
            
            // Define table structure
            dataTable.Columns.Add("SourceId", typeof(int));
            dataTable.Columns.Add("TransformedString", typeof(string));
            dataTable.Columns.Add("ProcessedDate", typeof(DateTime));
            dataTable.Columns.Add("ErrorMessage", typeof(string));
            dataTable.Columns.Add("HasError", typeof(bool));
            dataTable.Columns.Add("BatchId", typeof(Guid));
            
            // Add rows to DataTable
            foreach (var row in batch)
            {
                dataTable.Rows.Add(
                    row.SourceId,
                    row.TransformedString,
                    row.ProcessedDate,
                    row.ErrorMessage,
                    row.HasError,
                    batchId
                );
            }
            
            // Perform bulk insert
            using (var bulkCopy = new SqlBulkCopy(connection))
            {
                bulkCopy.DestinationTableName = STAGING_TABLE_NAME;
                bulkCopy.BatchSize = 1000; // Smaller batches for bulk copy
                bulkCopy.BulkCopyTimeout = COMMAND_TIMEOUT;
                
                // Map columns
                bulkCopy.ColumnMappings.Add("SourceId", "SourceId");
                bulkCopy.ColumnMappings.Add("TransformedString", "TransformedString");
                bulkCopy.ColumnMappings.Add("ProcessedDate", "ProcessedDate");
                bulkCopy.ColumnMappings.Add("ErrorMessage", "ErrorMessage");
                bulkCopy.ColumnMappings.Add("HasError", "HasError");
                bulkCopy.ColumnMappings.Add("BatchId", "BatchId");
                
                await bulkCopy.WriteToServerAsync(dataTable);
            }
        }

        /// <summary>
        /// Fallback method to process batch with retry logic
        /// </summary>
        private async Task ProcessBatchWithRetryAsync(List<TransformedRow> batch)
        {
            const int maxRetries = 3;
            var retryCount = 0;
            
            while (retryCount < maxRetries)
            {
                try
                {
                    using (var connection = new SqlConnection(_connectionString))
                    {
                        await connection.OpenAsync();
                        
                        // Process rows individually with retry
                        foreach (var row in batch)
                        {
                            await InsertRowWithRetryAsync(connection, row, maxRetries);
                        }
                    }
                    break; // Success, exit retry loop
                }
                catch (Exception ex)
                {
                    retryCount++;
                    if (retryCount >= maxRetries)
                    {
                        ComponentMetaData.FireError(0, "SSIS_Transform", 
                            $"Failed to process batch after {maxRetries} retries: {ex.Message}", 
                            "", 0, out _);
                        throw;
                    }
                    
                    // Wait before retry with exponential backoff
                    await Task.Delay(TimeSpan.FromSeconds(Math.Pow(2, retryCount)));
                }
            }
        }

        /// <summary>
        /// Insert a single row with retry logic
        /// </summary>
        private async Task InsertRowWithRetryAsync(SqlConnection connection, TransformedRow row, int maxRetries)
        {
            var retryCount = 0;
            
            while (retryCount < maxRetries)
            {
                try
                {
                    var insertSql = $@"
                        INSERT INTO [dbo].[{STAGING_TABLE_NAME}] 
                        (SourceId, TransformedString, ProcessedDate, ErrorMessage, HasError, BatchId)
                        VALUES (@SourceId, @TransformedString, @ProcessedDate, @ErrorMessage, @HasError, @BatchId)";
                    
                    using (var command = new SqlCommand(insertSql, connection))
                    {
                        command.Parameters.AddWithValue("@SourceId", row.SourceId);
                        command.Parameters.AddWithValue("@TransformedString", 
                            (object)row.TransformedString ?? DBNull.Value);
                        command.Parameters.AddWithValue("@ProcessedDate", row.ProcessedDate);
                        command.Parameters.AddWithValue("@ErrorMessage", 
                            (object)row.ErrorMessage ?? DBNull.Value);
                        command.Parameters.AddWithValue("@HasError", row.HasError);
                        command.Parameters.AddWithValue("@BatchId", Guid.NewGuid());
                        
                        await command.ExecuteNonQueryAsync();
                    }
                    break; // Success, exit retry loop
                }
                catch (Exception ex)
                {
                    retryCount++;
                    if (retryCount >= maxRetries)
                    {
                        ComponentMetaData.FireError(0, "SSIS_Transform", 
                            $"Failed to insert row {row.SourceId} after {maxRetries} retries: {ex.Message}", 
                            "", 0, out _);
                        throw;
                    }
                    
                    // Wait before retry
                    await Task.Delay(TimeSpan.FromSeconds(Math.Pow(2, retryCount)));
                }
            }
        }

        /// <summary>
        /// Finalize processing and handle remaining rows
        /// </summary>
        public override void PostExecute()
        {
            try
            {
                // Process any remaining rows in the buffer
                if (_batchBuffer.Count > 0)
                {
                    var finalBatch = new List<TransformedRow>(_batchBuffer);
                    _batchBuffer.Clear();
                    
                    // Process final batch synchronously to ensure completion
                    ProcessBatchAsync(finalBatch).Wait();
                }
                
                // Calculate and log performance metrics
                var processingTime = DateTime.Now - _startTime;
                var rowsPerSecond = _totalRowsProcessed / processingTime.TotalSeconds;
                
                ComponentMetaData.FireInformation(0, "SSIS_Transform", 
                    $"Processing completed. Total rows: {_totalRowsProcessed}, Errors: {_totalErrors}, " +
                    $"Processing time: {processingTime.TotalMinutes:F2} minutes, " +
                    $"Throughput: {rowsPerSecond:F0} rows/second", 
                    "", 0, ref ComponentMetaData.FireInformation);
            }
            catch (Exception ex)
            {
                ComponentMetaData.FireError(0, "SSIS_Transform", 
                    $"Error during post-execution: {ex.Message}", 
                    "", 0, out _);
                throw;
            }
            finally
            {
                base.PostExecute();
            }
        }

        /// <summary>
        /// Clean up resources
        /// </summary>
        public override void ReleaseConnections()
        {
            base.ReleaseConnections();
            
            // Clear batch buffer
            if (_batchBuffer != null)
            {
                _batchBuffer.Clear();
                _batchBuffer = null;
            }
        }
    }
}
