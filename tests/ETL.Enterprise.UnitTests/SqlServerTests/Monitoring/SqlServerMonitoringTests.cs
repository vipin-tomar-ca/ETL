using Microsoft.VisualStudio.TestTools.UnitTesting;
using Microsoft.Extensions.Logging;
using Moq;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Threading.Tasks;
using System.Linq;
using System.IO;
using FluentAssertions;
using ETL.Enterprise.Domain.Entities;
using ETL.Enterprise.Domain.Enums;
using ETL.Enterprise.Infrastructure.Services;

namespace ETL.Tests.Unit.SqlServer.Payroll
{
    /// <summary>
    /// Tests for SQL Server monitoring, observability, and performance tracking
    /// This class covers query execution plan analysis, performance counters, and resource monitoring
    /// </summary>
    [TestClass]
    public class SqlServerMonitoringTests
    {
        private Mock<ILogger<SqlServerMonitoringTests>> _mockLogger;
        private string _connectionString;
        private string _sqlFilesDirectory;

        [TestInitialize]
        public void TestInitialize()
        {
            _mockLogger = new Mock<ILogger<SqlServerMonitoringTests>>();
            _connectionString = Environment.GetEnvironmentVariable("PAYROLL_TEST_CONNECTION_STRING") 
                ?? "Server=localhost;Database=PayrollTestDB;Integrated Security=true;TrustServerCertificate=true;";
            
            _sqlFilesDirectory = Path.Combine(AppDomain.CurrentDomain.BaseDirectory, "SqlServerTests", "SqlFiles");
        }

        #region Query Execution Plan Tests

        /// <summary>
        /// Tests query execution plan analysis
        /// </summary>
        [TestMethod]
        public async Task AnalyzeQueryExecutionPlan_ForPayrollQueries_ReturnsValidPlans()
        {
            // Arrange
            var queryExecutor = new SqlQueryExecutor(CreateConnection(), _mockLogger.Object);
            var queries = new List<(string Name, string Query)>
            {
                ("EmployeeStartData", LoadSqlScript("PayrollQueries/EmployeeStartData.sql")),
                ("EmployeeCompensationData", LoadSqlScript("PayrollQueries/EmployeeCompensationData.sql")),
                ("OrganizationalHierarchy", LoadSqlScript("PayrollQueries/OrganizationalHierarchy.sql")),
                ("EmployeeAbsenceData", LoadSqlScript("PayrollQueries/EmployeeAbsenceData.sql"))
            };

            var parameters = new Dictionary<string, object>
            {
                ["StartDate"] = new DateTime(2024, 1, 1),
                ["EndDate"] = new DateTime(2024, 12, 31),
                ["TenantID"] = "TENANT_001",
                ["ClientID"] = "CLIENT_ABC",
                ["ExtractionDate"] = DateTime.Now,
                ["BatchID"] = "BATCH_20241201_001"
            };

            // Act & Assert
            foreach (var (name, query) in queries)
            {
                var executionPlan = await AnalyzeExecutionPlan(queryExecutor, query, parameters);
                
                executionPlan.Should().NotBeNull($"Execution plan for {name} should be generated");
                executionPlan.Should().Contain("SELECT", $"Execution plan for {name} should contain SELECT operations");
                executionPlan.Should().Contain("FROM", $"Execution plan for {name} should contain FROM operations");
            }
        }

        /// <summary>
        /// Tests query execution plan optimization recommendations
        /// </summary>
        [TestMethod]
        public async Task AnalyzeQueryOptimization_ForPayrollQueries_ProvidesRecommendations()
        {
            // Arrange
            var queryExecutor = new SqlQueryExecutor(CreateConnection(), _mockLogger.Object);
            var query = LoadSqlScript("PayrollQueries/EmployeeStartData.sql");
            var parameters = new Dictionary<string, object>
            {
                ["StartDate"] = new DateTime(2024, 1, 1),
                ["EndDate"] = new DateTime(2024, 12, 31),
                ["TenantID"] = "TENANT_001",
                ["ClientID"] = "CLIENT_ABC",
                ["ExtractionDate"] = DateTime.Now,
                ["BatchID"] = "BATCH_20241201_001"
            };

            // Act
            var optimizationRecommendations = await AnalyzeQueryOptimization(queryExecutor, query, parameters);

            // Assert
            optimizationRecommendations.Should().NotBeNull("Optimization recommendations should be provided");
            optimizationRecommendations.Should().NotBeEmpty("Should provide specific recommendations");
        }

        #endregion

        #region Performance Counter Tests

        /// <summary>
        /// Tests SQL Server performance counter monitoring
        /// </summary>
        [TestMethod]
        public async Task MonitorPerformanceCounters_DuringQueryExecution_TracksMetrics()
        {
            // Arrange
            var queryExecutor = new SqlQueryExecutor(CreateConnection(), _mockLogger.Object);
            var query = LoadSqlScript("PayrollQueries/EmployeeStartData.sql");
            var parameters = new Dictionary<string, object>
            {
                ["StartDate"] = new DateTime(2024, 1, 1),
                ["EndDate"] = new DateTime(2024, 12, 31),
                ["TenantID"] = "TENANT_001",
                ["ClientID"] = "CLIENT_ABC",
                ["ExtractionDate"] = DateTime.Now,
                ["BatchID"] = "BATCH_20241201_001"
            };

            // Act
            var beforeMetrics = await GetPerformanceMetrics(queryExecutor);
            var result = await queryExecutor.ExecuteQueryAsync<Dictionary<string, object>>(query, parameters);
            var afterMetrics = await GetPerformanceMetrics(queryExecutor);

            // Assert
            result.Should().NotBeNull("Query should execute successfully");
            beforeMetrics.Should().NotBeNull("Before metrics should be captured");
            afterMetrics.Should().NotBeNull("After metrics should be captured");
            
            // Validate metrics changed
            afterMetrics.ExecutionCount.Should().BeGreaterThan(beforeMetrics.ExecutionCount, "Execution count should increase");
            afterMetrics.TotalElapsedTime.Should().BeGreaterThan(beforeMetrics.TotalElapsedTime, "Total elapsed time should increase");
        }

        /// <summary>
        /// Tests memory usage monitoring
        /// </summary>
        [TestMethod]
        public async Task MonitorMemoryUsage_DuringQueryExecution_TracksMemoryConsumption()
        {
            // Arrange
            var queryExecutor = new SqlQueryExecutor(CreateConnection(), _mockLogger.Object);
            var query = LoadSqlScript("AnalyticsQueries/PayrollAnalyticsSummary.sql");
            var parameters = new Dictionary<string, object>
            {
                ["StartDate"] = new DateTime(2024, 1, 1),
                ["EndDate"] = new DateTime(2024, 12, 31)
            };

            // Act
            var beforeMemory = await GetMemoryUsage(queryExecutor);
            var result = await queryExecutor.ExecuteQueryAsync<Dictionary<string, object>>(query, parameters);
            var afterMemory = await GetMemoryUsage(queryExecutor);

            // Assert
            result.Should().NotBeNull("Query should execute successfully");
            beforeMemory.Should().NotBeNull("Before memory usage should be captured");
            afterMemory.Should().NotBeNull("After memory usage should be captured");
            
            // Validate memory usage
            afterMemory.TotalServerMemory.Should().BeGreaterThan(0, "Total server memory should be positive");
            afterMemory.TargetServerMemory.Should().BeGreaterThan(0, "Target server memory should be positive");
        }

        /// <summary>
        /// Tests CPU usage monitoring
        /// </summary>
        [TestMethod]
        public async Task MonitorCPUUsage_DuringQueryExecution_TracksCPUConsumption()
        {
            // Arrange
            var queryExecutor = new SqlQueryExecutor(CreateConnection(), _mockLogger.Object);
            var query = LoadSqlScript("AnalyticsQueries/EmployeeAnalyticsDashboard.sql");
            var parameters = new Dictionary<string, object>
            {
                ["StartDate"] = new DateTime(2024, 1, 1),
                ["EndDate"] = new DateTime(2024, 12, 31)
            };

            // Act
            var beforeCPU = await GetCPUUsage(queryExecutor);
            var result = await queryExecutor.ExecuteQueryAsync<Dictionary<string, object>>(query, parameters);
            var afterCPU = await GetCPUUsage(queryExecutor);

            // Assert
            result.Should().NotBeNull("Query should execute successfully");
            beforeCPU.Should().NotBeNull("Before CPU usage should be captured");
            afterCPU.Should().NotBeNull("After CPU usage should be captured");
            
            // Validate CPU usage
            afterCPU.ProcessorTimePercent.Should().BeGreaterThan(0, "Processor time should be positive");
        }

        #endregion

        #region Deadlock Monitoring Tests

        /// <summary>
        /// Tests deadlock detection and monitoring
        /// </summary>
        [TestMethod]
        public async Task MonitorDeadlocks_DuringConcurrentOperations_DetectsDeadlocks()
        {
            // Arrange
            var queryExecutor1 = new SqlQueryExecutor(CreateConnection(), _mockLogger.Object);
            var queryExecutor2 = new SqlQueryExecutor(CreateConnection(), _mockLogger.Object);

            // Act
            var beforeDeadlocks = await GetDeadlockCount(queryExecutor1);
            
            var task1 = Task.Run(async () =>
            {
                try
                {
                    await queryExecutor1.ExecuteNonQueryAsync("BEGIN TRANSACTION");
                    await queryExecutor1.ExecuteNonQueryAsync("UPDATE Employees SET ModifiedDate = GETDATE() WHERE EmployeeID = 1");
                    await Task.Delay(100);
                    await queryExecutor1.ExecuteNonQueryAsync("UPDATE Departments SET ModifiedDate = GETDATE() WHERE DepartmentID = 1");
                    await queryExecutor1.ExecuteNonQueryAsync("COMMIT TRANSACTION");
                }
                catch (SqlException ex) when (ex.Number == 1205) // Deadlock
                {
                    await queryExecutor1.ExecuteNonQueryAsync("ROLLBACK TRANSACTION");
                }
            });

            var task2 = Task.Run(async () =>
            {
                try
                {
                    await queryExecutor2.ExecuteNonQueryAsync("BEGIN TRANSACTION");
                    await queryExecutor2.ExecuteNonQueryAsync("UPDATE Departments SET ModifiedDate = GETDATE() WHERE DepartmentID = 1");
                    await Task.Delay(100);
                    await queryExecutor2.ExecuteNonQueryAsync("UPDATE Employees SET ModifiedDate = GETDATE() WHERE EmployeeID = 1");
                    await queryExecutor2.ExecuteNonQueryAsync("COMMIT TRANSACTION");
                }
                catch (SqlException ex) when (ex.Number == 1205) // Deadlock
                {
                    await queryExecutor2.ExecuteNonQueryAsync("ROLLBACK TRANSACTION");
                }
            });

            await Task.WhenAll(task1, task2);
            
            var afterDeadlocks = await GetDeadlockCount(queryExecutor1);

            // Assert
            afterDeadlocks.Should().BeGreaterThanOrEqualTo(beforeDeadlocks, "Deadlock count should not decrease");
        }

        /// <summary>
        /// Tests deadlock graph analysis
        /// </summary>
        [TestMethod]
        public async Task AnalyzeDeadlockGraph_ForDeadlockEvents_ReturnsDeadlockInformation()
        {
            // Arrange
            var queryExecutor = new SqlQueryExecutor(CreateConnection(), _mockLogger.Object);

            // Act
            var deadlockGraph = await GetDeadlockGraph(queryExecutor);

            // Assert
            deadlockGraph.Should().NotBeNull("Deadlock graph should be available");
        }

        #endregion

        #region Resource Usage Monitoring Tests

        /// <summary>
        /// Tests database file size monitoring
        /// </summary>
        [TestMethod]
        public async Task MonitorDatabaseFileSize_DuringOperations_TracksFileGrowth()
        {
            // Arrange
            var queryExecutor = new SqlQueryExecutor(CreateConnection(), _mockLogger.Object);

            // Act
            var beforeFileSize = await GetDatabaseFileSize(queryExecutor);
            
            // Perform operations that might increase file size
            await queryExecutor.ExecuteNonQueryAsync(@"
                INSERT INTO Employees (
                    EmployeeNumber, FirstName, LastName, Email, DateOfBirth, Gender,
                    StartDate, DepartmentID, PositionID, SalaryGrade, JobLevel, EmploymentStatus,
                    BusinessUnit, Region, Country, ContractType, EmploymentType
                ) VALUES (
                    'FILE_SIZE_TEST_001', 'FileSize', 'Test', 'filesize.test@company.com', 
                    '1990-01-01', 'Male', GETDATE(), 1, 1, 'SE1', 'L3', 'Active',
                    'Technology', 'North America', 'USA', 'Full-Time', 'Permanent'
                )");

            var afterFileSize = await GetDatabaseFileSize(queryExecutor);

            // Clean up
            await queryExecutor.ExecuteNonQueryAsync("DELETE FROM Employees WHERE EmployeeNumber = 'FILE_SIZE_TEST_001'");

            // Assert
            beforeFileSize.Should().NotBeNull("Before file size should be captured");
            afterFileSize.Should().NotBeNull("After file size should be captured");
            afterFileSize.DataFileSizeMB.Should().BeGreaterThan(0, "Data file size should be positive");
            afterFileSize.LogFileSizeMB.Should().BeGreaterThan(0, "Log file size should be positive");
        }

        /// <summary>
        /// Tests index usage monitoring
        /// </summary>
        [TestMethod]
        public async Task MonitorIndexUsage_ForPayrollQueries_TracksIndexUtilization()
        {
            // Arrange
            var queryExecutor = new SqlQueryExecutor(CreateConnection(), _mockLogger.Object);
            var query = LoadSqlScript("PayrollQueries/EmployeeStartData.sql");
            var parameters = new Dictionary<string, object>
            {
                ["StartDate"] = new DateTime(2024, 1, 1),
                ["EndDate"] = new DateTime(2024, 12, 31),
                ["TenantID"] = "TENANT_001",
                ["ClientID"] = "CLIENT_ABC",
                ["ExtractionDate"] = DateTime.Now,
                ["BatchID"] = "BATCH_20241201_001"
            };

            // Act
            var beforeIndexUsage = await GetIndexUsage(queryExecutor);
            var result = await queryExecutor.ExecuteQueryAsync<Dictionary<string, object>>(query, parameters);
            var afterIndexUsage = await GetIndexUsage(queryExecutor);

            // Assert
            result.Should().NotBeNull("Query should execute successfully");
            beforeIndexUsage.Should().NotBeNull("Before index usage should be captured");
            afterIndexUsage.Should().NotBeNull("After index usage should be captured");
            
            // Validate index usage
            afterIndexUsage.Should().Contain(usage => usage.UserSeeks > 0, "Some indexes should be used");
        }

        /// <summary>
        /// Tests connection pool monitoring
        /// </summary>
        [TestMethod]
        public async Task MonitorConnectionPool_DuringOperations_TracksConnectionUsage()
        {
            // Arrange
            var queryExecutor = new SqlQueryExecutor(CreateConnection(), _mockLogger.Object);

            // Act
            var beforeConnections = await GetConnectionPoolStatus(queryExecutor);
            
            // Perform operations
            var tasks = new List<Task>();
            for (int i = 0; i < 10; i++)
            {
                tasks.Add(Task.Run(async () =>
                {
                    var connection = new SqlConnection(_connectionString);
                    connection.Open();
                    var executor = new SqlQueryExecutor(connection, _mockLogger.Object);
                    await executor.ExecuteScalarAsync<int>("SELECT COUNT(*) FROM Employees");
                    connection.Close();
                }));
            }
            
            await Task.WhenAll(tasks);
            
            var afterConnections = await GetConnectionPoolStatus(queryExecutor);

            // Assert
            beforeConnections.Should().NotBeNull("Before connection status should be captured");
            afterConnections.Should().NotBeNull("After connection status should be captured");
            afterConnections.ActiveConnections.Should().BeGreaterThan(0, "Should have active connections");
        }

        #endregion

        #region Query Performance Monitoring Tests

        /// <summary>
        /// Tests slow query detection
        /// </summary>
        [TestMethod]
        public async Task DetectSlowQueries_ForPayrollOperations_IdentifiesSlowQueries()
        {
            // Arrange
            var queryExecutor = new SqlQueryExecutor(CreateConnection(), _mockLogger.Object);

            // Act
            var slowQueries = await GetSlowQueries(queryExecutor);

            // Assert
            slowQueries.Should().NotBeNull("Slow queries should be detected");
        }

        /// <summary>
        /// Tests query execution statistics
        /// </summary>
        [TestMethod]
        public async Task GetQueryExecutionStatistics_ForPayrollQueries_ReturnsStatistics()
        {
            // Arrange
            var queryExecutor = new SqlQueryExecutor(CreateConnection(), _mockLogger.Object);
            var query = LoadSqlScript("PayrollQueries/EmployeeStartData.sql");
            var parameters = new Dictionary<string, object>
            {
                ["StartDate"] = new DateTime(2024, 1, 1),
                ["EndDate"] = new DateTime(2024, 12, 31),
                ["TenantID"] = "TENANT_001",
                ["ClientID"] = "CLIENT_ABC",
                ["ExtractionDate"] = DateTime.Now,
                ["BatchID"] = "BATCH_20241201_001"
            };

            // Act
            var result = await queryExecutor.ExecuteQueryAsync<Dictionary<string, object>>(query, parameters);
            var statistics = await GetQueryExecutionStatistics(queryExecutor, query);

            // Assert
            result.Should().NotBeNull("Query should execute successfully");
            statistics.Should().NotBeNull("Query statistics should be available");
            statistics.ExecutionCount.Should().BeGreaterThan(0, "Execution count should be positive");
            statistics.TotalElapsedTime.Should().BeGreaterThan(0, "Total elapsed time should be positive");
        }

        #endregion

        #region Helper Methods

        private IDbConnection CreateConnection()
        {
            var connection = new SqlConnection(_connectionString);
            connection.Open();
            return connection;
        }

        private string LoadSqlScript(string scriptPath)
        {
            var fullPath = Path.Combine(_sqlFilesDirectory, scriptPath);
            return File.ReadAllText(fullPath);
        }

        private async Task<string> AnalyzeExecutionPlan(SqlQueryExecutor queryExecutor, string query, Dictionary<string, object> parameters)
        {
            var planQuery = $"SET SHOWPLAN_XML ON; {query}; SET SHOWPLAN_XML OFF;";
            var result = await queryExecutor.ExecuteScalarAsync<string>(planQuery, parameters);
            return result ?? "";
        }

        private async Task<List<string>> AnalyzeQueryOptimization(SqlQueryExecutor queryExecutor, string query, Dictionary<string, object> parameters)
        {
            var recommendations = new List<string>();
            
            // Analyze execution plan for optimization opportunities
            var planQuery = $"SET SHOWPLAN_XML ON; {query}; SET SHOWPLAN_XML OFF;";
            var plan = await queryExecutor.ExecuteScalarAsync<string>(planQuery, parameters);
            
            if (plan != null)
            {
                if (plan.Contains("Table Scan"))
                    recommendations.Add("Consider adding indexes to avoid table scans");
                if (plan.Contains("Hash Match"))
                    recommendations.Add("Consider optimizing JOIN conditions");
                if (plan.Contains("Sort"))
                    recommendations.Add("Consider adding ORDER BY indexes");
            }
            
            return recommendations;
        }

        private async Task<PerformanceMetrics> GetPerformanceMetrics(SqlQueryExecutor queryExecutor)
        {
            var query = @"
                SELECT 
                    SUM(execution_count) as ExecutionCount,
                    SUM(total_elapsed_time) as TotalElapsedTime,
                    SUM(total_logical_reads) as TotalLogicalReads,
                    SUM(total_physical_reads) as TotalPhysicalReads
                FROM sys.dm_exec_query_stats";

            var result = await queryExecutor.ExecuteQueryAsync<Dictionary<string, object>>(query);
            var first = result.FirstOrDefault();
            
            return new PerformanceMetrics
            {
                ExecutionCount = first != null ? Convert.ToInt64(first["ExecutionCount"]) : 0,
                TotalElapsedTime = first != null ? Convert.ToInt64(first["TotalElapsedTime"]) : 0,
                TotalLogicalReads = first != null ? Convert.ToInt64(first["TotalLogicalReads"]) : 0,
                TotalPhysicalReads = first != null ? Convert.ToInt64(first["TotalPhysicalReads"]) : 0
            };
        }

        private async Task<MemoryUsage> GetMemoryUsage(SqlQueryExecutor queryExecutor)
        {
            var query = @"
                SELECT 
                    (SELECT cntr_value FROM sys.dm_os_performance_counters WHERE counter_name = 'Total Server Memory (KB)') * 1024 as TotalServerMemory,
                    (SELECT cntr_value FROM sys.dm_os_performance_counters WHERE counter_name = 'Target Server Memory (KB)') * 1024 as TargetServerMemory";

            var result = await queryExecutor.ExecuteQueryAsync<Dictionary<string, object>>(query);
            var first = result.FirstOrDefault();
            
            return new MemoryUsage
            {
                TotalServerMemory = first != null ? Convert.ToInt64(first["TotalServerMemory"]) : 0,
                TargetServerMemory = first != null ? Convert.ToInt64(first["TargetServerMemory"]) : 0
            };
        }

        private async Task<CPUUsage> GetCPUUsage(SqlQueryExecutor queryExecutor)
        {
            var query = @"
                SELECT 
                    (SELECT cntr_value FROM sys.dm_os_performance_counters WHERE counter_name = 'Processor Time %') as ProcessorTimePercent";

            var result = await queryExecutor.ExecuteQueryAsync<Dictionary<string, object>>(query);
            var first = result.FirstOrDefault();
            
            return new CPUUsage
            {
                ProcessorTimePercent = first != null ? Convert.ToDouble(first["ProcessorTimePercent"]) : 0
            };
        }

        private async Task<int> GetDeadlockCount(SqlQueryExecutor queryExecutor)
        {
            var query = @"
                SELECT 
                    (SELECT cntr_value FROM sys.dm_os_performance_counters WHERE counter_name = 'Number of Deadlocks/sec') as DeadlockCount";

            var result = await queryExecutor.ExecuteScalarAsync<int>(query);
            return result;
        }

        private async Task<string> GetDeadlockGraph(SqlQueryExecutor queryExecutor)
        {
            var query = @"
                SELECT 
                    CAST(event_data AS XML).value('(/event/data[@name=""xml_report""]/value)[1]', 'NVARCHAR(MAX)') as DeadlockGraph
                FROM sys.fn_xe_file_target_read_file('system_health*.xel', null, null, null)
                WHERE object_name = 'xml_deadlock_report'";

            var result = await queryExecutor.ExecuteScalarAsync<string>(query);
            return result ?? "";
        }

        private async Task<DatabaseFileSize> GetDatabaseFileSize(SqlQueryExecutor queryExecutor)
        {
            var query = @"
                SELECT 
                    SUM(CASE WHEN type = 0 THEN size * 8.0 / 1024 ELSE 0 END) as DataFileSizeMB,
                    SUM(CASE WHEN type = 1 THEN size * 8.0 / 1024 ELSE 0 END) as LogFileSizeMB
                FROM sys.database_files";

            var result = await queryExecutor.ExecuteQueryAsync<Dictionary<string, object>>(query);
            var first = result.FirstOrDefault();
            
            return new DatabaseFileSize
            {
                DataFileSizeMB = first != null ? Convert.ToDouble(first["DataFileSizeMB"]) : 0,
                LogFileSizeMB = first != null ? Convert.ToDouble(first["LogFileSizeMB"]) : 0
            };
        }

        private async Task<List<IndexUsage>> GetIndexUsage(SqlQueryExecutor queryExecutor)
        {
            var query = @"
                SELECT 
                    i.name as IndexName,
                    s.user_seeks,
                    s.user_scans,
                    s.user_lookups,
                    s.user_updates
                FROM sys.indexes i
                LEFT JOIN sys.dm_db_index_usage_stats s ON i.object_id = s.object_id AND i.index_id = s.index_id
                WHERE i.name IS NOT NULL";

            var result = await queryExecutor.ExecuteQueryAsync<Dictionary<string, object>>(query);
            
            return result.Select(r => new IndexUsage
            {
                IndexName = r["IndexName"]?.ToString() ?? "",
                UserSeeks = Convert.ToInt32(r["user_seeks"] ?? 0),
                UserScans = Convert.ToInt32(r["user_scans"] ?? 0),
                UserLookups = Convert.ToInt32(r["user_lookups"] ?? 0),
                UserUpdates = Convert.ToInt32(r["user_updates"] ?? 0)
            }).ToList();
        }

        private async Task<ConnectionPoolStatus> GetConnectionPoolStatus(SqlQueryExecutor queryExecutor)
        {
            var query = @"
                SELECT 
                    COUNT(*) as ActiveConnections,
                    SUM(CASE WHEN status = 'sleeping' THEN 1 ELSE 0 END) as SleepingConnections,
                    SUM(CASE WHEN status = 'running' THEN 1 ELSE 0 END) as RunningConnections
                FROM sys.dm_exec_sessions
                WHERE is_user_process = 1";

            var result = await queryExecutor.ExecuteQueryAsync<Dictionary<string, object>>(query);
            var first = result.FirstOrDefault();
            
            return new ConnectionPoolStatus
            {
                ActiveConnections = first != null ? Convert.ToInt32(first["ActiveConnections"]) : 0,
                SleepingConnections = first != null ? Convert.ToInt32(first["SleepingConnections"]) : 0,
                RunningConnections = first != null ? Convert.ToInt32(first["RunningConnections"]) : 0
            };
        }

        private async Task<List<SlowQuery>> GetSlowQueries(SqlQueryExecutor queryExecutor)
        {
            var query = @"
                SELECT TOP 10
                    qs.total_elapsed_time / qs.execution_count as avg_elapsed_time,
                    qs.execution_count,
                    SUBSTRING(st.text, (qs.statement_start_offset/2) + 1, 
                        ((CASE qs.statement_end_offset
                            WHEN -1 THEN DATALENGTH(st.text)
                            ELSE qs.statement_end_offset
                        END - qs.statement_start_offset)/2) + 1) as statement_text
                FROM sys.dm_exec_query_stats qs
                CROSS APPLY sys.dm_exec_sql_text(qs.sql_handle) st
                ORDER BY avg_elapsed_time DESC";

            var result = await queryExecutor.ExecuteQueryAsync<Dictionary<string, object>>(query);
            
            return result.Select(r => new SlowQuery
            {
                AverageElapsedTime = Convert.ToInt64(r["avg_elapsed_time"] ?? 0),
                ExecutionCount = Convert.ToInt32(r["execution_count"] ?? 0),
                StatementText = r["statement_text"]?.ToString() ?? ""
            }).ToList();
        }

        private async Task<QueryExecutionStatistics> GetQueryExecutionStatistics(SqlQueryExecutor queryExecutor, string query)
        {
            var statsQuery = @"
                SELECT 
                    execution_count,
                    total_elapsed_time,
                    total_logical_reads,
                    total_physical_reads,
                    total_logical_writes
                FROM sys.dm_exec_query_stats qs
                CROSS APPLY sys.dm_exec_sql_text(qs.sql_handle) st
                WHERE st.text LIKE @QueryPattern";

            var parameters = new Dictionary<string, object>
            {
                ["QueryPattern"] = $"%{query.Substring(0, Math.Min(50, query.Length))}%"
            };

            var result = await queryExecutor.ExecuteQueryAsync<Dictionary<string, object>>(statsQuery, parameters);
            var first = result.FirstOrDefault();
            
            return new QueryExecutionStatistics
            {
                ExecutionCount = first != null ? Convert.ToInt32(first["execution_count"] ?? 0) : 0,
                TotalElapsedTime = first != null ? Convert.ToInt64(first["total_elapsed_time"] ?? 0) : 0,
                TotalLogicalReads = first != null ? Convert.ToInt64(first["total_logical_reads"] ?? 0) : 0,
                TotalPhysicalReads = first != null ? Convert.ToInt64(first["total_physical_reads"] ?? 0) : 0,
                TotalLogicalWrites = first != null ? Convert.ToInt64(first["total_logical_writes"] ?? 0) : 0
            };
        }

        #endregion
    }

    #region Monitoring Data Classes

    public class PerformanceMetrics
    {
        public long ExecutionCount { get; set; }
        public long TotalElapsedTime { get; set; }
        public long TotalLogicalReads { get; set; }
        public long TotalPhysicalReads { get; set; }
    }

    public class MemoryUsage
    {
        public long TotalServerMemory { get; set; }
        public long TargetServerMemory { get; set; }
    }

    public class CPUUsage
    {
        public double ProcessorTimePercent { get; set; }
    }

    public class DatabaseFileSize
    {
        public double DataFileSizeMB { get; set; }
        public double LogFileSizeMB { get; set; }
    }

    public class IndexUsage
    {
        public string IndexName { get; set; }
        public int UserSeeks { get; set; }
        public int UserScans { get; set; }
        public int UserLookups { get; set; }
        public int UserUpdates { get; set; }
    }

    public class ConnectionPoolStatus
    {
        public int ActiveConnections { get; set; }
        public int SleepingConnections { get; set; }
        public int RunningConnections { get; set; }
    }

    public class SlowQuery
    {
        public long AverageElapsedTime { get; set; }
        public int ExecutionCount { get; set; }
        public string StatementText { get; set; }
    }

    public class QueryExecutionStatistics
    {
        public int ExecutionCount { get; set; }
        public long TotalElapsedTime { get; set; }
        public long TotalLogicalReads { get; set; }
        public long TotalPhysicalReads { get; set; }
        public long TotalLogicalWrites { get; set; }
    }

    #endregion
}
