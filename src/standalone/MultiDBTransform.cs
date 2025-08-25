using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using Microsoft.Spark.Sql;
using Microsoft.Spark.Sql.Types;
using static Microsoft.Spark.Sql.Functions;

namespace ETL.Scalable
{
    /// <summary>
    /// Multi-Database Transform Application using .NET Spark
    /// 
    /// This application demonstrates distributed multi-database queries using Microsoft.Spark.
    /// It reads connection strings and SQL queries from a metadata database, loads data from
    /// multiple databases into Spark DataFrames, performs joins and transformations,
    /// and writes results to target destinations.
    /// 
    /// Optimized for handling 1 TB/day with 10-20 Spark nodes.
    /// </summary>
    public class MultiDBTransform
    {
        private readonly SparkSession _spark;
        private readonly string _metadataConnectionString;
        private readonly ILogger _logger;

        /// <summary>
        /// Constructor initializes Spark session with optimized configurations
        /// </summary>
        /// <param name="metadataConnectionString">Connection string to metadata database</param>
        public MultiDBTransform(string metadataConnectionString)
        {
            _metadataConnectionString = metadataConnectionString;
            _logger = new ConsoleLogger();

            // Initialize Spark session with optimized configurations for large-scale processing
            _spark = SparkSession.Builder()
                .AppName("MultiDBTransform")
                .Config("spark.sql.adaptive.enabled", "true")
                .Config("spark.sql.adaptive.coalescePartitions.enabled", "true")
                .Config("spark.sql.adaptive.skewJoin.enabled", "true")
                .Config("spark.sql.adaptive.localShuffleReader.enabled", "true")
                .Config("spark.sql.adaptive.advisoryPartitionSizeInBytes", "128m")
                .Config("spark.sql.files.maxPartitionBytes", "128m")
                .Config("spark.sql.files.openCostInBytes", "4194304")
                .Config("spark.sql.broadcastTimeout", "3600")
                .Config("spark.sql.autoBroadcastJoinThreshold", "100485760")
                .Config("spark.sql.shuffle.partitions", "200")
                .Config("spark.default.parallelism", "200")
                .Config("spark.sql.execution.arrow.pyspark.enabled", "true")
                .Config("spark.sql.execution.arrow.pyspark.fallback.enabled", "true")
                .GetOrCreate();

            _logger.LogInfo("Spark session initialized with optimized configurations");
        }

        /// <summary>
        /// Main execution method orchestrating the entire ETL process
        /// </summary>
        public void Execute()
        {
            try
            {
                _logger.LogInfo("Starting Multi-Database Transform process");

                // Step 1: Load metadata from the metadata database
                var connections = LoadConnections();
                var queries = LoadQueries();

                // Step 2: Load data from multiple databases into Spark DataFrames
                var dataFrames = LoadDataFromDatabases(connections, queries);

                // Step 3: Perform multi-database joins and transformations
                var transformedData = PerformTransformations(dataFrames);

                // Step 4: Write results to target destination
                WriteResults(transformedData);

                _logger.LogInfo("Multi-Database Transform process completed successfully");
            }
            catch (Exception ex)
            {
                _logger.LogError($"Error in Execute method: {ex.Message}", ex);
                throw;
            }
        }

        /// <summary>
        /// Loads database connection information from metadata database
        /// </summary>
        /// <returns>Dictionary of connection configurations</returns>
        private Dictionary<string, DatabaseConnection> LoadConnections()
        {
            try
            {
                _logger.LogInfo("Loading database connections from metadata");

                var connections = new Dictionary<string, DatabaseConnection>();

                using (var connection = new SqlConnection(_metadataConnectionString))
                {
                    connection.Open();
                    using (var command = new SqlCommand("SELECT * FROM Connections WHERE IsActive = 1", connection))
                    using (var reader = command.ExecuteReader())
                    {
                        while (reader.Read())
                        {
                            var dbConnection = new DatabaseConnection
                            {
                                ConnectionId = reader["ConnectionId"].ToString(),
                                ConnectionName = reader["ConnectionName"].ToString(),
                                ConnectionString = reader["ConnectionString"].ToString(),
                                DatabaseType = reader["DatabaseType"].ToString(),
                                IsActive = Convert.ToBoolean(reader["IsActive"])
                            };

                            connections[dbConnection.ConnectionId] = dbConnection;
                        }
                    }
                }

                _logger.LogInfo($"Loaded {connections.Count} active database connections");
                return connections;
            }
            catch (Exception ex)
            {
                _logger.LogError($"Error loading connections: {ex.Message}", ex);
                throw;
            }
        }

        /// <summary>
        /// Loads SQL queries from metadata database
        /// </summary>
        /// <returns>Dictionary of query configurations</returns>
        private Dictionary<string, DatabaseQuery> LoadQueries()
        {
            try
            {
                _logger.LogInfo("Loading SQL queries from metadata");

                var queries = new Dictionary<string, DatabaseQuery>();

                using (var connection = new SqlConnection(_metadataConnectionString))
                {
                    connection.Open();
                    using (var command = new SqlCommand("SELECT * FROM Queries WHERE IsActive = 1", connection))
                    using (var reader = command.ExecuteReader())
                    {
                        while (reader.Read())
                        {
                            var query = new DatabaseQuery
                            {
                                QueryId = reader["QueryId"].ToString(),
                                ConnectionId = reader["ConnectionId"].ToString(),
                                QueryName = reader["QueryName"].ToString(),
                                SqlQuery = reader["SqlQuery"].ToString(),
                                IsActive = Convert.ToBoolean(reader["IsActive"]),
                                Priority = Convert.ToInt32(reader["Priority"])
                            };

                            queries[query.QueryId] = query;
                        }
                    }
                }

                _logger.LogInfo($"Loaded {queries.Count} active queries");
                return queries;
            }
            catch (Exception ex)
            {
                _logger.LogError($"Error loading queries: {ex.Message}", ex);
                throw;
            }
        }

        /// <summary>
        /// Loads data from multiple databases into Spark DataFrames using JDBC
        /// </summary>
        /// <param name="connections">Database connections</param>
        /// <param name="queries">SQL queries to execute</param>
        /// <returns>Dictionary of DataFrames keyed by query name</returns>
        private Dictionary<string, DataFrame> LoadDataFromDatabases(
            Dictionary<string, DatabaseConnection> connections,
            Dictionary<string, DatabaseQuery> queries)
        {
            try
            {
                _logger.LogInfo("Loading data from multiple databases into Spark DataFrames");

                var dataFrames = new Dictionary<string, DataFrame>();

                // Group queries by connection for efficient processing
                var queriesByConnection = queries.Values
                    .GroupBy(q => q.ConnectionId)
                    .ToDictionary(g => g.Key, g => g.ToList());

                foreach (var connectionGroup in queriesByConnection)
                {
                    var connectionId = connectionGroup.Key;
                    var connectionQueries = connectionGroup.Value;

                    if (!connections.ContainsKey(connectionId))
                    {
                        _logger.LogWarning($"Connection {connectionId} not found, skipping queries");
                        continue;
                    }

                    var connection = connections[connectionId];
                    _logger.LogInfo($"Processing {connectionQueries.Count} queries for connection: {connection.ConnectionName}");

                    foreach (var query in connectionQueries.OrderBy(q => q.Priority))
                    {
                        try
                        {
                            var dataFrame = LoadDataFrameFromDatabase(connection, query);
                            dataFrames[query.QueryName] = dataFrame;

                            _logger.LogInfo($"Successfully loaded DataFrame for query: {query.QueryName} " +
                                          $"(Rows: {dataFrame.Count()}, Partitions: {dataFrame.Rdd().GetNumPartitions()})");
                        }
                        catch (Exception ex)
                        {
                            _logger.LogError($"Error loading DataFrame for query {query.QueryName}: {ex.Message}", ex);
                            // Continue with other queries instead of failing completely
                        }
                    }
                }

                _logger.LogInfo($"Successfully loaded {dataFrames.Count} DataFrames");
                return dataFrames;
            }
            catch (Exception ex)
            {
                _logger.LogError($"Error loading data from databases: {ex.Message}", ex);
                throw;
            }
        }

        /// <summary>
        /// Loads a single DataFrame from a database using JDBC with optimized settings
        /// </summary>
        /// <param name="connection">Database connection configuration</param>
        /// <param name="query">SQL query to execute</param>
        /// <returns>Spark DataFrame</returns>
        private DataFrame LoadDataFrameFromDatabase(DatabaseConnection connection, DatabaseQuery query)
        {
            try
            {
                // Configure JDBC options for optimal performance
                var jdbcOptions = new Dictionary<string, string>
                {
                    ["url"] = connection.ConnectionString,
                    ["query"] = query.SqlQuery,
                    ["driver"] = GetJdbcDriver(connection.DatabaseType),
                    ["numPartitions"] = "20", // Optimize for 10-20 nodes
                    ["partitionColumn"] = GetPartitionColumn(query.SqlQuery),
                    ["lowerBound"] = "1",
                    ["upperBound"] = "1000000",
                    ["fetchsize"] = "10000",
                    ["sessionInitStatement"] = GetSessionInitStatement(connection.DatabaseType)
                };

                // Add database-specific optimizations
                AddDatabaseSpecificOptions(jdbcOptions, connection.DatabaseType);

                _logger.LogInfo($"Loading DataFrame from {connection.DatabaseType} using query: {query.QueryName}");

                var dataFrame = _spark.Read()
                    .Format("jdbc")
                    .Options(jdbcOptions)
                    .Load();

                // Cache frequently used DataFrames for better performance
                if (query.Priority <= 2) // High priority queries
                {
                    dataFrame = dataFrame.Cache();
                    _logger.LogInfo($"Cached DataFrame for high-priority query: {query.QueryName}");
                }

                return dataFrame;
            }
            catch (Exception ex)
            {
                _logger.LogError($"Error loading DataFrame from {connection.DatabaseType}: {ex.Message}", ex);
                throw;
            }
        }

        /// <summary>
        /// Performs multi-database joins and transformations on loaded DataFrames
        /// </summary>
        /// <param name="dataFrames">Input DataFrames</param>
        /// <returns>Transformed DataFrame</returns>
        private DataFrame PerformTransformations(Dictionary<string, DataFrame> dataFrames)
        {
            try
            {
                _logger.LogInfo("Performing multi-database joins and transformations");

                if (dataFrames.Count == 0)
                {
                    throw new InvalidOperationException("No DataFrames available for transformation");
                }

                // Create temporary views for SQL operations
                foreach (var kvp in dataFrames)
                {
                    kvp.Value.CreateOrReplaceTempView(kvp.Key);
                    _logger.LogInfo($"Created temporary view: {kvp.Key}");
                }

                // Perform multi-database join using Spark SQL
                var joinedData = PerformMultiDatabaseJoin(dataFrames);

                // Apply transformations
                var transformedData = ApplyTransformations(joinedData);

                _logger.LogInfo("Transformations completed successfully");
                return transformedData;
            }
            catch (Exception ex)
            {
                _logger.LogError($"Error performing transformations: {ex.Message}", ex);
                throw;
            }
        }

        /// <summary>
        /// Performs INNER JOIN on common ID columns across multiple DataFrames
        /// </summary>
        /// <param name="dataFrames">Input DataFrames</param>
        /// <returns>Joined DataFrame</returns>
        private DataFrame PerformMultiDatabaseJoin(Dictionary<string, DataFrame> dataFrames)
        {
            try
            {
                _logger.LogInfo("Performing multi-database INNER JOIN");

                if (dataFrames.Count == 1)
                {
                    return dataFrames.Values.First();
                }

                // Build dynamic SQL join query
                var tableNames = dataFrames.Keys.ToList();
                var joinQuery = BuildJoinQuery(tableNames);

                _logger.LogInfo($"Executing join query: {joinQuery}");

                var joinedData = _spark.Sql(joinQuery);

                // Optimize the joined DataFrame
                joinedData = joinedData.Repartition(200); // Optimize for 10-20 nodes

                _logger.LogInfo($"Join completed. Result DataFrame: {joinedData.Count()} rows, " +
                              $"{joinedData.Rdd().GetNumPartitions()} partitions");

                return joinedData;
            }
            catch (Exception ex)
            {
                _logger.LogError($"Error performing multi-database join: {ex.Message}", ex);
                throw;
            }
        }

        /// <summary>
        /// Applies various transformations to the joined data
        /// </summary>
        /// <param name="dataFrame">Input DataFrame</param>
        /// <returns>Transformed DataFrame</returns>
        private DataFrame ApplyTransformations(DataFrame dataFrame)
        {
            try
            {
                _logger.LogInfo("Applying data transformations");

                // Apply uppercase transformation to string columns
                var stringColumns = dataFrame.Schema()
                    .Fields
                    .Where(f => f.DataType is StringType)
                    .Select(f => f.Name)
                    .ToList();

                var transformedData = dataFrame;
                foreach (var column in stringColumns)
                {
                    transformedData = transformedData.WithColumn(column, Upper(Col(column)));
                    _logger.LogInfo($"Applied UPPER transformation to column: {column}");
                }

                // Add aggregations
                transformedData = AddAggregations(transformedData);

                // Add calculated columns
                transformedData = AddCalculatedColumns(transformedData);

                _logger.LogInfo("Data transformations completed");
                return transformedData;
            }
            catch (Exception ex)
            {
                _logger.LogError($"Error applying transformations: {ex.Message}", ex);
                throw;
            }
        }

        /// <summary>
        /// Adds aggregation operations to the DataFrame
        /// </summary>
        /// <param name="dataFrame">Input DataFrame</param>
        /// <returns>DataFrame with aggregations</returns>
        private DataFrame AddAggregations(DataFrame dataFrame)
        {
            try
            {
                _logger.LogInfo("Adding aggregation operations");

                // Group by common business key and calculate aggregations
                var numericColumns = dataFrame.Schema()
                    .Fields
                    .Where(f => f.DataType is IntegerType || f.DataType is LongType || f.DataType is DoubleType)
                    .Select(f => f.Name)
                    .ToList();

                if (numericColumns.Any())
                {
                    var aggregationExpressions = new List<Column>();

                    // Add COUNT aggregation
                    aggregationExpressions.Add(Count("*").As("total_records"));

                    // Add SUM aggregations for numeric columns
                    foreach (var column in numericColumns.Take(5)) // Limit to prevent too many aggregations
                    {
                        aggregationExpressions.Add(Sum(column).As($"sum_{column}"));
                        aggregationExpressions.Add(Avg(column).As($"avg_{column}"));
                    }

                    // Group by first string column if available
                    var groupByColumn = dataFrame.Schema()
                        .Fields
                        .Where(f => f.DataType is StringType)
                        .Select(f => f.Name)
                        .FirstOrDefault();

                    if (!string.IsNullOrEmpty(groupByColumn))
                    {
                        dataFrame = dataFrame.GroupBy(groupByColumn)
                            .Agg(aggregationExpressions.ToArray());

                        _logger.LogInfo($"Added aggregations grouped by: {groupByColumn}");
                    }
                }

                return dataFrame;
            }
            catch (Exception ex)
            {
                _logger.LogError($"Error adding aggregations: {ex.Message}", ex);
                throw;
            }
        }

        /// <summary>
        /// Adds calculated columns to the DataFrame
        /// </summary>
        /// <param name="dataFrame">Input DataFrame</param>
        /// <returns>DataFrame with calculated columns</returns>
        private DataFrame AddCalculatedColumns(DataFrame dataFrame)
        {
            try
            {
                _logger.LogInfo("Adding calculated columns");

                // Add timestamp column
                dataFrame = dataFrame.WithColumn("processed_timestamp", CurrentTimestamp());

                // Add row number for ordering
                dataFrame = dataFrame.WithColumn("row_number", MonotonicallyIncreasingId());

                // Add hash column for data integrity
                dataFrame = dataFrame.WithColumn("data_hash", 
                    Sha2(Concat(dataFrame.Columns().Select(c => Col(c.Name)).ToArray()), 256));

                _logger.LogInfo("Added calculated columns: processed_timestamp, row_number, data_hash");
                return dataFrame;
            }
            catch (Exception ex)
            {
                _logger.LogError($"Error adding calculated columns: {ex.Message}", ex);
                throw;
            }
        }

        /// <summary>
        /// Writes the transformed results to target destination
        /// </summary>
        /// <param name="dataFrame">Transformed DataFrame</param>
        private void WriteResults(DataFrame dataFrame)
        {
            try
            {
                _logger.LogInfo("Writing results to target destination");

                // Write to Parquet files (recommended for large datasets)
                var parquetPath = $"output/transformed_data_{DateTime.Now:yyyyMMdd_HHmmss}";
                
                dataFrame.Write()
                    .Mode(SaveMode.Overwrite)
                    .Option("compression", "snappy")
                    .Option("maxRecordsPerFile", "1000000")
                    .Parquet(parquetPath);

                _logger.LogInfo($"Results written to Parquet: {parquetPath}");

                // Optionally write to target database
                WriteToTargetDatabase(dataFrame);

                _logger.LogInfo("Results writing completed successfully");
            }
            catch (Exception ex)
            {
                _logger.LogError($"Error writing results: {ex.Message}", ex);
                throw;
            }
        }

        /// <summary>
        /// Writes results to target database using JDBC
        /// </summary>
        /// <param name="dataFrame">DataFrame to write</param>
        private void WriteToTargetDatabase(DataFrame dataFrame)
        {
            try
            {
                // Get target database connection from metadata
                var targetConnection = GetTargetConnection();
                if (targetConnection == null)
                {
                    _logger.LogWarning("No target database connection found, skipping database write");
                    return;
                }

                var jdbcOptions = new Dictionary<string, string>
                {
                    ["url"] = targetConnection.ConnectionString,
                    ["dbtable"] = "transformed_results",
                    ["driver"] = GetJdbcDriver(targetConnection.DatabaseType),
                    ["batchsize"] = "10000",
                    ["isolationLevel"] = "NONE"
                };

                dataFrame.Write()
                    .Format("jdbc")
                    .Mode(SaveMode.Append)
                    .Options(jdbcOptions)
                    .Save();

                _logger.LogInfo("Results written to target database successfully");
            }
            catch (Exception ex)
            {
                _logger.LogError($"Error writing to target database: {ex.Message}", ex);
                // Don't throw - Parquet write is the primary output
            }
        }

        /// <summary>
        /// Helper method to get JDBC driver class name
        /// </summary>
        /// <param name="databaseType">Database type</param>
        /// <returns>JDBC driver class name</returns>
        private string GetJdbcDriver(string databaseType)
        {
            return databaseType.ToUpper() switch
            {
                "SQLSERVER" => "com.microsoft.sqlserver.jdbc.SQLServerDriver",
                "ORACLE" => "oracle.jdbc.driver.OracleDriver",
                "MYSQL" => "com.mysql.cj.jdbc.Driver",
                "POSTGRESQL" => "org.postgresql.Driver",
                _ => throw new ArgumentException($"Unsupported database type: {databaseType}")
            };
        }

        /// <summary>
        /// Helper method to get partition column from SQL query
        /// </summary>
        /// <param name="sqlQuery">SQL query</param>
        /// <returns>Partition column name</returns>
        private string GetPartitionColumn(string sqlQuery)
        {
            // Simple heuristic to find a good partition column
            var commonIdColumns = new[] { "id", "ID", "Id", "customer_id", "order_id", "user_id" };
            
            foreach (var column in commonIdColumns)
            {
                if (sqlQuery.Contains(column, StringComparison.OrdinalIgnoreCase))
                {
                    return column;
                }
            }

            return "id"; // Default fallback
        }

        /// <summary>
        /// Helper method to get session initialization statement
        /// </summary>
        /// <param name="databaseType">Database type</param>
        /// <returns>Session initialization statement</returns>
        private string GetSessionInitStatement(string databaseType)
        {
            return databaseType.ToUpper() switch
            {
                "SQLSERVER" => "SET QUOTED_IDENTIFIER ON",
                "ORACLE" => "ALTER SESSION SET NLS_DATE_FORMAT = 'YYYY-MM-DD HH24:MI:SS'",
                "MYSQL" => "SET SESSION sql_mode = 'STRICT_TRANS_TABLES'",
                _ => ""
            };
        }

        /// <summary>
        /// Adds database-specific JDBC options for optimization
        /// </summary>
        /// <param name="options">JDBC options dictionary</param>
        /// <param name="databaseType">Database type</param>
        private void AddDatabaseSpecificOptions(Dictionary<string, string> options, string databaseType)
        {
            switch (databaseType.ToUpper())
            {
                case "SQLSERVER":
                    options["queryTimeout"] = "300";
                    options["cancelQueryTimeout"] = "5";
                    break;
                case "ORACLE":
                    options["oracle.jdbc.timezoneAsRegion"] = "false";
                    options["oracle.jdbc.fanEnabled"] = "false";
                    break;
                case "MYSQL":
                    options["useSSL"] = "false";
                    options["allowPublicKeyRetrieval"] = "true";
                    break;
            }
        }

        /// <summary>
        /// Builds dynamic SQL join query for multiple tables
        /// </summary>
        /// <param name="tableNames">List of table names</param>
        /// <returns>SQL join query</returns>
        private string BuildJoinQuery(List<string> tableNames)
        {
            if (tableNames.Count == 1)
            {
                return $"SELECT * FROM {tableNames[0]}";
            }

            var joinQuery = $"SELECT * FROM {tableNames[0]} t1";
            
            for (int i = 1; i < tableNames.Count; i++)
            {
                joinQuery += $" INNER JOIN {tableNames[i]} t{i + 1} ON t1.id = t{i + 1}.id";
            }

            return joinQuery;
        }

        /// <summary>
        /// Gets target database connection from metadata
        /// </summary>
        /// <returns>Target database connection or null</returns>
        private DatabaseConnection GetTargetConnection()
        {
            try
            {
                using (var connection = new SqlConnection(_metadataConnectionString))
                {
                    connection.Open();
                    using (var command = new SqlCommand(
                        "SELECT * FROM Connections WHERE IsActive = 1 AND IsTarget = 1", connection))
                    using (var reader = command.ExecuteReader())
                    {
                        if (reader.Read())
                        {
                            return new DatabaseConnection
                            {
                                ConnectionId = reader["ConnectionId"].ToString(),
                                ConnectionName = reader["ConnectionName"].ToString(),
                                ConnectionString = reader["ConnectionString"].ToString(),
                                DatabaseType = reader["DatabaseType"].ToString(),
                                IsActive = Convert.ToBoolean(reader["IsActive"])
                            };
                        }
                    }
                }

                return null;
            }
            catch (Exception ex)
            {
                _logger.LogError($"Error getting target connection: {ex.Message}", ex);
                return null;
            }
        }

        /// <summary>
        /// Disposes Spark session and resources
        /// </summary>
        public void Dispose()
        {
            try
            {
                _spark?.Stop();
                _logger.LogInfo("Spark session stopped");
            }
            catch (Exception ex)
            {
                _logger.LogError($"Error disposing Spark session: {ex.Message}", ex);
            }
        }
    }

    /// <summary>
    /// Database connection configuration model
    /// </summary>
    public class DatabaseConnection
    {
        public string ConnectionId { get; set; }
        public string ConnectionName { get; set; }
        public string ConnectionString { get; set; }
        public string DatabaseType { get; set; }
        public bool IsActive { get; set; }
        public bool IsTarget { get; set; }
    }

    /// <summary>
    /// Database query configuration model
    /// </summary>
    public class DatabaseQuery
    {
        public string QueryId { get; set; }
        public string ConnectionId { get; set; }
        public string QueryName { get; set; }
        public string SqlQuery { get; set; }
        public bool IsActive { get; set; }
        public int Priority { get; set; }
    }

    /// <summary>
    /// Simple console logger implementation
    /// </summary>
    public interface ILogger
    {
        void LogInfo(string message);
        void LogWarning(string message);
        void LogError(string message, Exception ex = null);
    }

    public class ConsoleLogger : ILogger
    {
        public void LogInfo(string message)
        {
            Console.WriteLine($"[INFO] {DateTime.Now:yyyy-MM-dd HH:mm:ss} - {message}");
        }

        public void LogWarning(string message)
        {
            Console.WriteLine($"[WARN] {DateTime.Now:yyyy-MM-dd HH:mm:ss} - {message}");
        }

        public void LogError(string message, Exception ex = null)
        {
            Console.WriteLine($"[ERROR] {DateTime.Now:yyyy-MM-dd HH:mm:ss} - {message}");
            if (ex != null)
            {
                Console.WriteLine($"Exception: {ex}");
            }
        }
    }
}
