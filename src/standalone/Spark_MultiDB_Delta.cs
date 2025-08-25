using Microsoft.Spark.Sql;
using Microsoft.Spark.Sql.Types;
using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Configuration;
using System.Text.Json;

namespace ETL.Spark
{
    /// <summary>
    /// .NET Spark application for processing delta rows from CDC-enabled SQL Server databases
    /// Supports high-volume data processing (1 TB/day) with 10-20 Spark nodes
    /// </summary>
    public class SparkMultiDBDeltaProcessor
    {
        private readonly SparkSession _spark;
        private readonly ILogger<SparkMultiDBDeltaProcessor> _logger;
        private readonly IConfiguration _configuration;
        private readonly string _metadataConnectionString;
        private readonly string _targetConnectionString;
        private readonly Dictionary<string, object> _jdbcOptions;

        public SparkMultiDBDeltaProcessor(IConfiguration configuration, ILogger<SparkMultiDBDeltaProcessor> logger)
        {
            _configuration = configuration;
            _logger = logger;
            _metadataConnectionString = _configuration["ConnectionStrings:MetadataDB"];
            _targetConnectionString = _configuration["ConnectionStrings:TargetDB"];

            // Initialize Spark session with optimized configuration for 1 TB/day processing
            _spark = SparkSession.Builder()
                .AppName("ETL-Spark-MultiDB-Delta")
                .Config("spark.sql.adaptive.enabled", "true")
                .Config("spark.sql.adaptive.coalescePartitions.enabled", "true")
                .Config("spark.sql.adaptive.skewJoin.enabled", "true")
                .Config("spark.sql.adaptive.localShuffleReader.enabled", "true")
                .Config("spark.sql.adaptive.advisoryPartitionSizeInBytes", "128m")
                .Config("spark.sql.adaptive.minNumPostShufflePartitions", "10")
                .Config("spark.sql.adaptive.maxNumPostShufflePartitions", "200")
                .Config("spark.sql.shuffle.partitions", "200")
                .Config("spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes", "256m")
                .Config("spark.sql.adaptive.skewJoin.skewedPartitionFactor", "5")
                .Config("spark.sql.adaptive.advisoryPartitionSizeInBytes", "128m")
                .Config("spark.sql.adaptive.coalescePartitions.minPartitionNum", "10")
                .Config("spark.sql.adaptive.coalescePartitions.initialPartitionNum", "200")
                .Config("spark.sql.adaptive.coalescePartitions.parallelismFirst", "false")
                .Config("spark.sql.adaptive.coalescePartitions.parallelismFirst.numShufflePartitions", "200")
                .Config("spark.sql.adaptive.coalescePartitions.parallelismFirst.targetSize", "128m")
                .Config("spark.sql.adaptive.coalescePartitions.parallelismFirst.minPartitionNum", "10")
                .Config("spark.sql.adaptive.coalescePartitions.parallelismFirst.maxPartitionNum", "200")
                .Config("spark.sql.adaptive.coalescePartitions.parallelismFirst.parallelismFirst", "false")
                .Config("spark.sql.adaptive.coalescePartitions.parallelismFirst.parallelismFirst.numShufflePartitions", "200")
                .Config("spark.sql.adaptive.coalescePartitions.parallelismFirst.parallelismFirst.targetSize", "128m")
                .Config("spark.sql.adaptive.coalescePartitions.parallelismFirst.parallelismFirst.minPartitionNum", "10")
                .Config("spark.sql.adaptive.coalescePartitions.parallelismFirst.parallelismFirst.maxPartitionNum", "200")
                .GetOrCreate();

            // Configure JDBC options for optimal performance
            _jdbcOptions = new Dictionary<string, object>
            {
                ["url"] = _targetConnectionString,
                ["driver"] = "com.microsoft.sqlserver.jdbc.SQLServerDriver",
                ["batchsize"] = 10000,
                ["queryTimeout"] = 300,
                ["sessionInitStatement"] = "SET ARITHABORT ON; SET NUMERIC_ROUNDABORT OFF; SET ANSI_PADDING ON; SET ANSI_WARNINGS ON; SET CONCAT_NULL_YIELDS_NULL ON; SET ANSI_NULLS ON; SET QUOTED_IDENTIFIER ON; SET ANSI_NULL_DFLT_ON ON;",
                ["isolationLevel"] = "READ_COMMITTED"
            };

            _logger.LogInformation("Spark session initialized with optimized configuration for high-volume processing");
        }

        /// <summary>
        /// Main processing method that orchestrates the delta processing workflow
        /// </summary>
        public async Task ProcessDeltaDataAsync()
        {
            try
            {
                _logger.LogInformation("Starting delta data processing for multiple databases");

                // Step 1: Retrieve database configurations from metadata
                var databaseConfigs = await GetDatabaseConfigurationsAsync();
                _logger.LogInformation($"Retrieved {databaseConfigs.Count} database configurations");

                // Step 2: Process each database in parallel
                var processingTasks = databaseConfigs.Select(config => ProcessDatabaseAsync(config));
                await Task.WhenAll(processingTasks);

                // Step 3: Perform multi-database joins if multiple databases were processed
                if (databaseConfigs.Count > 1)
                {
                    await PerformMultiDatabaseJoinsAsync(databaseConfigs);
                }

                _logger.LogInformation("Delta data processing completed successfully");
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error during delta data processing");
                await LogErrorToMetadataAsync("ProcessDeltaDataAsync", ex);
                throw;
            }
        }

        /// <summary>
        /// Retrieves database configurations from the metadata database
        /// </summary>
        private async Task<List<DatabaseConfig>> GetDatabaseConfigurationsAsync()
        {
            var configs = new List<DatabaseConfig>();

            using (var connection = new SqlConnection(_metadataConnectionString))
            {
                await connection.OpenAsync();
                var query = @"
                    SELECT 
                        c.ConnectionID,
                        c.DatabaseName,
                        c.ConnectionString,
                        c.IsCDCEnabled,
                        c.LastProcessedLSN,
                        c.DeltaCondition,
                        c.TableName,
                        c.SchemaName,
                        c.ProcessingEngine,
                        c.BatchSize,
                        c.TimeoutMinutes,
                        cs.CaptureInstance,
                        cs.SupportsNetChanges,
                        cs.CaptureColumnList
                    FROM dbo.Connections c
                    LEFT JOIN dbo.CDCStatus cs ON c.ConnectionID = cs.ConnectionID
                    WHERE c.IsActive = 1 AND c.ProcessingEngine = 'Spark'
                    ORDER BY c.DatabaseName, c.TableName";

                using (var command = new SqlCommand(query, connection))
                using (var reader = await command.ExecuteReaderAsync())
                {
                    while (await reader.ReadAsync())
                    {
                        configs.Add(new DatabaseConfig
                        {
                            ConnectionID = reader.GetInt32("ConnectionID"),
                            DatabaseName = reader.GetString("DatabaseName"),
                            ConnectionString = reader.GetString("ConnectionString"),
                            IsCDCEnabled = reader.GetBoolean("IsCDCEnabled"),
                            LastProcessedLSN = reader.IsDBNull("LastProcessedLSN") ? null : reader.GetSqlBinary("LastProcessedLSN").Value,
                            DeltaCondition = reader.IsDBNull("DeltaCondition") ? null : reader.GetString("DeltaCondition"),
                            TableName = reader.GetString("TableName"),
                            SchemaName = reader.GetString("SchemaName"),
                            ProcessingEngine = reader.GetString("ProcessingEngine"),
                            BatchSize = reader.GetInt32("BatchSize"),
                            TimeoutMinutes = reader.GetInt32("TimeoutMinutes"),
                            CaptureInstance = reader.IsDBNull("CaptureInstance") ? null : reader.GetString("CaptureInstance"),
                            SupportsNetChanges = reader.IsDBNull("SupportsNetChanges") ? false : reader.GetBoolean("SupportsNetChanges"),
                            CaptureColumnList = reader.IsDBNull("CaptureColumnList") ? null : reader.GetString("CaptureColumnList")
                        });
                    }
                }
            }

            return configs;
        }

        /// <summary>
        /// Processes a single database for delta data extraction
        /// </summary>
        private async Task ProcessDatabaseAsync(DatabaseConfig config)
        {
            try
            {
                _logger.LogInformation($"Processing database: {config.DatabaseName}, Table: {config.TableName}");

                DataFrame deltaData;
                string lastProcessedLSN = null;

                if (config.IsCDCEnabled)
                {
                    // Process CDC-enabled database
                    (deltaData, lastProcessedLSN) = await ProcessCDCDatabaseAsync(config);
                }
                else
                {
                    // Process non-CDC database with timestamp-based delta
                    deltaData = await ProcessNonCDCDatabaseAsync(config);
                }

                if (deltaData != null && deltaData.Count() > 0)
                {
                    // Register the DataFrame as a temporary view for multi-database joins
                    deltaData.CreateOrReplaceTempView($"{config.DatabaseName}_{config.TableName}_delta");

                    // Write to target database
                    await WriteToTargetDatabaseAsync(deltaData, config);

                    // Update LastProcessedLSN in metadata
                    if (config.IsCDCEnabled && !string.IsNullOrEmpty(lastProcessedLSN))
                    {
                        await UpdateLastProcessedLSNAsync(config.ConnectionID, lastProcessedLSN);
                    }

                    _logger.LogInformation($"Successfully processed {deltaData.Count()} records from {config.DatabaseName}.{config.TableName}");
                }
                else
                {
                    _logger.LogInformation($"No delta data found for {config.DatabaseName}.{config.TableName}");
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, $"Error processing database {config.DatabaseName}");
                await LogErrorToMetadataAsync($"ProcessDatabaseAsync_{config.DatabaseName}", ex, config.DatabaseName);
                throw;
            }
        }

        /// <summary>
        /// Processes CDC-enabled database by querying CDC change tables
        /// </summary>
        private async Task<(DataFrame deltaData, string lastProcessedLSN)> ProcessCDCDatabaseAsync(DatabaseConfig config)
        {
            try
            {
                // Build CDC query based on configuration
                var cdcQuery = BuildCDCQuery(config);
                _logger.LogInformation($"CDC Query for {config.DatabaseName}: {cdcQuery}");

                // Create JDBC options for source database
                var sourceJdbcOptions = new Dictionary<string, object>
                {
                    ["url"] = config.ConnectionString,
                    ["driver"] = "com.microsoft.sqlserver.jdbc.SQLServerDriver",
                    ["query"] = cdcQuery,
                    ["fetchsize"] = config.BatchSize,
                    ["queryTimeout"] = config.TimeoutMinutes * 60
                };

                // Load CDC data from SQL Server
                var cdcData = _spark.Read()
                    .Format("jdbc")
                    .Options(sourceJdbcOptions)
                    .Load();

                // Transform CDC data to handle operation types
                var transformedData = TransformCDCData(cdcData, config);

                // Get the last processed LSN for metadata update
                var lastLSN = await GetLastProcessedLSNFromDataAsync(cdcData);

                return (transformedData, lastLSN);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, $"Error processing CDC database {config.DatabaseName}");
                throw;
            }
        }

        /// <summary>
        /// Processes non-CDC database using timestamp-based delta conditions
        /// </summary>
        private async Task<DataFrame> ProcessNonCDCDatabaseAsync(DatabaseConfig config)
        {
            try
            {
                // Build timestamp-based query
                var timestampQuery = BuildTimestampQuery(config);
                _logger.LogInformation($"Timestamp Query for {config.DatabaseName}: {timestampQuery}");

                // Create JDBC options for source database
                var sourceJdbcOptions = new Dictionary<string, object>
                {
                    ["url"] = config.ConnectionString,
                    ["driver"] = "com.microsoft.sqlserver.jdbc.SQLServerDriver",
                    ["query"] = timestampQuery,
                    ["fetchsize"] = config.BatchSize,
                    ["queryTimeout"] = config.TimeoutMinutes * 60
                };

                // Load data from SQL Server
                var data = _spark.Read()
                    .Format("jdbc")
                    .Options(sourceJdbcOptions)
                    .Load();

                // Add processing metadata
                var processedData = data.WithColumn("ProcessingEngine", Functions.Lit("Spark"))
                                       .WithColumn("SourceDatabase", Functions.Lit(config.DatabaseName))
                                       .WithColumn("ProcessingTimestamp", Functions.CurrentTimestamp());

                return processedData;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, $"Error processing non-CDC database {config.DatabaseName}");
                throw;
            }
        }

        /// <summary>
        /// Builds CDC query to extract delta data from CDC change tables
        /// </summary>
        private string BuildCDCQuery(DatabaseConfig config)
        {
            var baseQuery = $@"
                SELECT 
                    __$operation,
                    __$seqval,
                    __$update_mask,
                    __$start_lsn,
                    __$end_lsn,
                    {config.CaptureColumnList ?? "*"}
                FROM cdc.{config.CaptureInstance}_CT
                WHERE __$start_lsn > {GetLSNHexString(config.LastProcessedLSN)}
                ORDER BY __$start_lsn, __$seqval";

            return baseQuery;
        }

        /// <summary>
        /// Builds timestamp-based query for non-CDC databases
        /// </summary>
        private string BuildTimestampQuery(DatabaseConfig config)
        {
            var lastRunTimestamp = config.LastProcessedLSN != null 
                ? DateTime.Now.AddHours(-24).ToString("yyyy-MM-dd HH:mm:ss") 
                : DateTime.MinValue.ToString("yyyy-MM-dd HH:mm:ss");

            return $@"
                SELECT 
                    *,
                    '{config.DatabaseName}' as SourceDatabase,
                    GETDATE() as ProcessingTimestamp
                FROM {config.SchemaName}.{config.TableName}
                WHERE {config.DeltaCondition} > '{lastRunTimestamp}'
                ORDER BY {config.DeltaCondition}";
        }

        /// <summary>
        /// Transforms CDC data to handle operation types and prepare for joins
        /// </summary>
        private DataFrame TransformCDCData(DataFrame cdcData, DatabaseConfig config)
        {
            // Add operation type description
            var withOperationType = cdcData.WithColumn("OperationType", 
                Functions.When(Functions.Col("__$operation") == 1, Functions.Lit("Delete"))
                         .When(Functions.Col("__$operation") == 2, Functions.Lit("Insert"))
                         .When(Functions.Col("__$operation") == 3, Functions.Lit("Update_Before"))
                         .When(Functions.Col("__$operation") == 4, Functions.Lit("Update_After"))
                         .Otherwise(Functions.Lit("Unknown")));

            // Add processing metadata
            var withMetadata = withOperationType.WithColumn("ProcessingEngine", Functions.Lit("Spark"))
                                               .WithColumn("SourceDatabase", Functions.Lit(config.DatabaseName))
                                               .WithColumn("CaptureInstance", Functions.Lit(config.CaptureInstance))
                                               .WithColumn("ProcessingTimestamp", Functions.CurrentTimestamp());

            // Handle net changes if configured
            if (config.SupportsNetChanges)
            {
                return FilterNetChanges(withMetadata);
            }

            return withMetadata;
        }

        /// <summary>
        /// Filters CDC data to show only net changes (final state)
        /// </summary>
        private DataFrame FilterNetChanges(DataFrame cdcData)
        {
            // For net changes, we only want the final state of each record
            // This means keeping only the latest operation for each record
            return cdcData.GroupBy("__$seqval")
                         .agg(Functions.Max("__$start_lsn").As("__$start_lsn"),
                              Functions.First("__$operation").As("__$operation"),
                              Functions.First("OperationType").As("OperationType"),
                              Functions.First("ProcessingEngine").As("ProcessingEngine"),
                              Functions.First("SourceDatabase").As("SourceDatabase"),
                              Functions.First("CaptureInstance").As("CaptureInstance"),
                              Functions.First("ProcessingTimestamp").As("ProcessingTimestamp"))
                         .OrderBy("__$start_lsn");
        }

        /// <summary>
        /// Performs multi-database joins using Spark SQL
        /// </summary>
        private async Task PerformMultiDatabaseJoinsAsync(List<DatabaseConfig> configs)
        {
            try
            {
                _logger.LogInformation("Performing multi-database joins");

                // Build join query based on available temporary views
                var joinQuery = BuildMultiDatabaseJoinQuery(configs);
                _logger.LogInformation($"Multi-database join query: {joinQuery}");

                // Execute join query
                var joinedData = _spark.Sql(joinQuery);

                if (joinedData.Count() > 0)
                {
                    // Write joined results to target database
                    await WriteJoinedDataToTargetAsync(joinedData);

                    _logger.LogInformation($"Successfully processed {joinedData.Count()} joined records");
                }
                else
                {
                    _logger.LogInformation("No joined data found");
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error performing multi-database joins");
                await LogErrorToMetadataAsync("PerformMultiDatabaseJoinsAsync", ex);
                throw;
            }
        }

        /// <summary>
        /// Builds multi-database join query using Spark SQL
        /// </summary>
        private string BuildMultiDatabaseJoinQuery(List<DatabaseConfig> configs)
        {
            var joinConditions = new List<string>();
            var selectColumns = new List<string>();

            // Build join conditions and select columns
            for (int i = 0; i < configs.Count; i++)
            {
                var config = configs[i];
                var alias = $"db{i}";
                var viewName = $"{config.DatabaseName}_{config.TableName}_delta";

                selectColumns.Add($"{alias}.*");

                if (i > 0)
                {
                    // Join on common business keys (adjust based on your data model)
                    var joinCondition = $"{alias}.CustomerID = db0.CustomerID";
                    joinConditions.Add(joinCondition);
                }
            }

            var joinClause = string.Join(" AND ", joinConditions);
            var selectClause = string.Join(", ", selectColumns);

            return $@"
                SELECT {selectClause}
                FROM {configs[0].DatabaseName}_{configs[0].TableName}_delta db0
                {string.Join(" ", configs.Skip(1).Select((config, i) => 
                    $"INNER JOIN {config.DatabaseName}_{config.TableName}_delta db{i + 1} ON {joinClause}"))}
                WHERE db0.OperationType IN ('Insert', 'Update_After')";
        }

        /// <summary>
        /// Writes data to target SQL Server database
        /// </summary>
        private async Task WriteToTargetDatabaseAsync(DataFrame data, DatabaseConfig config)
        {
            try
            {
                var targetTableName = $"staging.{config.DatabaseName}_{config.TableName}_Delta";

                // Write data to target database
                data.Write()
                    .Format("jdbc")
                    .Options(_jdbcOptions)
                    .Option("dbtable", targetTableName)
                    .Mode(SaveMode.Append)
                    .Save();

                _logger.LogInformation($"Successfully wrote {data.Count()} records to {targetTableName}");
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, $"Error writing data to target database for {config.DatabaseName}");
                throw;
            }
        }

        /// <summary>
        /// Writes joined data to target database
        /// </summary>
        private async Task WriteJoinedDataToTargetAsync(DataFrame joinedData)
        {
            try
            {
                var targetTableName = "staging.MultiDB_Joined_Delta";

                // Write joined data to target database
                joinedData.Write()
                    .Format("jdbc")
                    .Options(_jdbcOptions)
                    .Option("dbtable", targetTableName)
                    .Mode(SaveMode.Append)
                    .Save();

                _logger.LogInformation($"Successfully wrote {joinedData.Count()} joined records to {targetTableName}");
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error writing joined data to target database");
                throw;
            }
        }

        /// <summary>
        /// Updates LastProcessedLSN in metadata database
        /// </summary>
        private async Task UpdateLastProcessedLSNAsync(int connectionId, string lastProcessedLSN)
        {
            try
            {
                using (var connection = new SqlConnection(_metadataConnectionString))
                {
                    await connection.OpenAsync();
                    var query = @"
                        UPDATE dbo.Connections 
                        SET LastProcessedLSN = @LastProcessedLSN, 
                            LastUpdated = GETDATE() 
                        WHERE ConnectionID = @ConnectionID";

                    using (var command = new SqlCommand(query, connection))
                    {
                        command.Parameters.AddWithValue("@LastProcessedLSN", Convert.FromBase64String(lastProcessedLSN));
                        command.Parameters.AddWithValue("@ConnectionID", connectionId);
                        await command.ExecuteNonQueryAsync();
                    }
                }

                _logger.LogInformation($"Updated LastProcessedLSN for connection {connectionId}");
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, $"Error updating LastProcessedLSN for connection {connectionId}");
                throw;
            }
        }

        /// <summary>
        /// Gets the last processed LSN from CDC data
        /// </summary>
        private async Task<string> GetLastProcessedLSNFromDataAsync(DataFrame cdcData)
        {
            try
            {
                // Get the maximum LSN from the CDC data
                var maxLSN = cdcData.Select(Functions.Max("__$start_lsn").As("max_lsn")).First();
                return maxLSN.GetAs<string>("max_lsn");
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error getting last processed LSN from CDC data");
                return null;
            }
        }

        /// <summary>
        /// Converts binary LSN to hexadecimal string for SQL query
        /// </summary>
        private string GetLSNHexString(byte[] lsn)
        {
            if (lsn == null || lsn.Length == 0)
            {
                return "0x00000000000000000000"; // Default LSN
            }

            return "0x" + BitConverter.ToString(lsn).Replace("-", "");
        }

        /// <summary>
        /// Logs errors to metadata database
        /// </summary>
        private async Task LogErrorToMetadataAsync(string methodName, Exception ex, string databaseName = null)
        {
            try
            {
                using (var connection = new SqlConnection(_metadataConnectionString))
                {
                    await connection.OpenAsync();
                    var query = @"
                        INSERT INTO dbo.Logs 
                        (BatchID, DatabaseName, LogLevel, LogMessage, ErrorMessage, ErrorCode, ErrorSource, CreatedDate, PackageName)
                        VALUES 
                        (@BatchID, @DatabaseName, 'ERROR', @LogMessage, @ErrorMessage, @ErrorCode, @ErrorSource, GETDATE(), 'Spark_MultiDB_Delta')";

                    using (var command = new SqlCommand(query, connection))
                    {
                        command.Parameters.AddWithValue("@BatchID", Guid.NewGuid().ToString());
                        command.Parameters.AddWithValue("@DatabaseName", databaseName ?? "Unknown");
                        command.Parameters.AddWithValue("@LogMessage", $"Error in {methodName}");
                        command.Parameters.AddWithValue("@ErrorMessage", ex.Message);
                        command.Parameters.AddWithValue("@ErrorCode", ex.HResult);
                        command.Parameters.AddWithValue("@ErrorSource", ex.StackTrace);
                        await command.ExecuteNonQueryAsync();
                    }
                }
            }
            catch (Exception logEx)
            {
                _logger.LogError(logEx, "Error logging to metadata database");
            }
        }

        /// <summary>
        /// Disposes Spark session
        /// </summary>
        public void Dispose()
        {
            _spark?.Stop();
            _spark?.Dispose();
        }
    }

    /// <summary>
    /// Configuration class for database processing
    /// </summary>
    public class DatabaseConfig
    {
        public int ConnectionID { get; set; }
        public string DatabaseName { get; set; }
        public string ConnectionString { get; set; }
        public bool IsCDCEnabled { get; set; }
        public byte[] LastProcessedLSN { get; set; }
        public string DeltaCondition { get; set; }
        public string TableName { get; set; }
        public string SchemaName { get; set; }
        public string ProcessingEngine { get; set; }
        public int BatchSize { get; set; }
        public int TimeoutMinutes { get; set; }
        public string CaptureInstance { get; set; }
        public bool SupportsNetChanges { get; set; }
        public string CaptureColumnList { get; set; }
    }
}
