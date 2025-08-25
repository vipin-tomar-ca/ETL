using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ETL.Scalable.Transform
{
    /// <summary>
    /// Mock SSIS Script Component for high-throughput data transformations
    /// This is a template implementation that can be adapted for actual SSIS deployment
    /// Optimized for 1 TB/day throughput with parallel batch processing
    /// </summary>
    public class ScriptMain
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
        /// Mock input buffer for SSIS integration
        /// </summary>
        public class Input0Buffer
        {
            public int SourceId { get; set; }
            public string InputString { get; set; }
            // Add additional input fields as needed
        }

        /// <summary>
        /// Mock output buffer for SSIS integration
        /// </summary>
        public class Output0Buffer
        {
            public int SourceId { get; set; }
            public string TransformedString { get; set; }
            public DateTime ProcessedDate { get; set; }
            public string ErrorMessage { get; set; }
            public bool HasError { get; set; }
            // Add additional output fields as needed
        }

        /// <summary>
        /// Initialize the script component
        /// </summary>
        public void PreExecute()
        {
            try
            {
                _startTime = DateTime.Now;
                _batchBuffer = new List<TransformedRow>();
                
                // Get connection string from configuration
                _connectionString = GetConnectionString();
                
                // Log component initialization
                Console.WriteLine($"SSIS_Transform: Script Component initialized. Batch size: {BATCH_SIZE}, Max concurrent batches: {MAX_CONCURRENT_BATCHES}");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"SSIS_Transform: Failed to initialize script component: {ex.Message}");
                throw;
            }
        }

        /// <summary>
        /// Process each input row and apply transformations
        /// </summary>
        public void Input0_ProcessInputRow(Input0Buffer Row)
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
                        ProcessBatch();
                    }
                }

                _totalRowsProcessed++;
            }
            catch (Exception ex)
            {
                _totalErrors++;
                Console.WriteLine($"SSIS_Transform: Error processing row {Row.SourceId}: {ex.Message}");
                
                // Add error row to batch
                lock (_batchLock)
                {
                    _batchBuffer.Add(new TransformedRow
                    {
                        SourceId = Row.SourceId,
                        TransformedString = null,
                        ProcessedDate = DateTime.Now,
                        HasError = true,
                        ErrorMessage = ex.Message
                    });
                }
            }
        }

        /// <summary>
        /// Clean up resources after processing
        /// </summary>
        public void PostExecute()
        {
            try
            {
                // Process any remaining rows in buffer
                if (_batchBuffer.Count > 0)
                {
                    ProcessBatch();
                }

                var duration = DateTime.Now - _startTime;
                Console.WriteLine($"SSIS_Transform: Processing completed. Total rows: {_totalRowsProcessed}, Errors: {_totalErrors}, Duration: {duration}");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"SSIS_Transform: Error in PostExecute: {ex.Message}");
                throw;
            }
        }

        /// <summary>
        /// Process a batch of transformed rows
        /// </summary>
        private void ProcessBatch()
        {
            if (_batchBuffer.Count == 0) return;

            var batchToProcess = new List<TransformedRow>(_batchBuffer);
            _batchBuffer.Clear();

            // Process batch asynchronously
            Task.Run(() => ProcessBatchAsync(batchToProcess));
        }

        /// <summary>
        /// Process batch asynchronously for better performance
        /// </summary>
        private async Task ProcessBatchAsync(List<TransformedRow> batch)
        {
            try
            {
                using (var connection = new SqlConnection(_connectionString))
                {
                    await connection.OpenAsync();
                    
                    // Use bulk insert for optimal performance
                    await BulkInsertTransformedData(connection, batch);
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"SSIS_Transform: Error processing batch: {ex.Message}");
            }
        }

        /// <summary>
        /// Bulk insert transformed data for optimal performance
        /// </summary>
        private async Task BulkInsertTransformedData(SqlConnection connection, List<TransformedRow> batch)
        {
            using (var bulkCopy = new SqlBulkCopy(connection))
            {
                bulkCopy.DestinationTableName = STAGING_TABLE_NAME;
                bulkCopy.BatchSize = 1000;
                bulkCopy.BulkCopyTimeout = COMMAND_TIMEOUT;

                // Map columns
                bulkCopy.ColumnMappings.Add("SourceId", "SourceId");
                bulkCopy.ColumnMappings.Add("TransformedString", "TransformedString");
                bulkCopy.ColumnMappings.Add("ProcessedDate", "ProcessedDate");
                bulkCopy.ColumnMappings.Add("ErrorMessage", "ErrorMessage");
                bulkCopy.ColumnMappings.Add("HasError", "HasError");

                // Create data table for bulk insert
                var dataTable = new DataTable();
                dataTable.Columns.Add("SourceId", typeof(int));
                dataTable.Columns.Add("TransformedString", typeof(string));
                dataTable.Columns.Add("ProcessedDate", typeof(DateTime));
                dataTable.Columns.Add("ErrorMessage", typeof(string));
                dataTable.Columns.Add("HasError", typeof(bool));

                foreach (var row in batch)
                {
                    dataTable.Rows.Add(
                        row.SourceId,
                        row.TransformedString,
                        row.ProcessedDate,
                        row.ErrorMessage,
                        row.HasError
                    );
                }

                await bulkCopy.WriteToServerAsync(dataTable);
            }
        }

        /// <summary>
        /// Transform input string using business logic
        /// </summary>
        private string TransformString(string input)
        {
            if (string.IsNullOrEmpty(input))
                return string.Empty;

            // Apply transformation logic here
            // This is a simple example - replace with your actual transformation logic
            var transformed = input.ToUpper();
            
            // Remove special characters
            transformed = System.Text.RegularExpressions.Regex.Replace(transformed, @"[^A-Z0-9\s]", "");
            
            // Trim whitespace
            transformed = transformed.Trim();
            
            return transformed;
        }

        /// <summary>
        /// Get connection string from configuration
        /// </summary>
        private string GetConnectionString()
        {
            // In a real SSIS implementation, this would come from the connection manager
            // For this mock implementation, return a placeholder
            return "Server=localhost;Database=StagingDB;Trusted_Connection=true;";
        }

        /// <summary>
        /// Create staging table if it doesn't exist
        /// </summary>
        public static void CreateStagingTable(string connectionString)
        {
            var createTableSql = @"
                IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='staging_transformed_data' AND xtype='U')
                CREATE TABLE staging_transformed_data (
                    Id INT IDENTITY(1,1) PRIMARY KEY,
                    SourceId INT NOT NULL,
                    TransformedString NVARCHAR(MAX),
                    ProcessedDate DATETIME2 NOT NULL,
                    ErrorMessage NVARCHAR(MAX),
                    HasError BIT NOT NULL DEFAULT 0,
                    CreatedAt DATETIME2 NOT NULL DEFAULT GETDATE()
                )";

            using (var connection = new SqlConnection(connectionString))
            {
                connection.Open();
                using (var command = new SqlCommand(createTableSql, connection))
                {
                    command.ExecuteNonQuery();
                }
            }
        }
    }
}
