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
using ETL.Enterprise.Domain.Entities;
using ETL.Enterprise.Domain.Enums;
using ETL.Enterprise.Infrastructure.Services;

namespace ETL.Tests.Unit.SqlServer.Payroll
{
    /// <summary>
    /// Tests for regression testing of payroll queries
    /// This class compares current query results with baseline data to detect changes
    /// </summary>
    [TestClass]
    public class PayrollRegressionTests
    {
        private Mock<ILogger<PayrollRegressionTests>> _mockLogger;
        private string _connectionString;
        private string _baselineDataDirectory;
        private string _currentDataDirectory;
        private string _sqlFilesDirectory;

        [TestInitialize]
        public void TestInitialize()
        {
            _mockLogger = new Mock<ILogger<PayrollRegressionTests>>();
            _connectionString = Environment.GetEnvironmentVariable("PAYROLL_TEST_CONNECTION_STRING") 
                ?? "Server=localhost;Database=PayrollTestDB;Integrated Security=true;TrustServerCertificate=true;";
            
            _baselineDataDirectory = Path.Combine(AppDomain.CurrentDomain.BaseDirectory, "TestData", "Baseline");
            _currentDataDirectory = Path.Combine(AppDomain.CurrentDomain.BaseDirectory, "TestData", "Current");
            _sqlFilesDirectory = Path.Combine(AppDomain.CurrentDomain.BaseDirectory, "SqlServerTests", "SqlFiles");
            
            // Create directories if they don't exist
            Directory.CreateDirectory(_currentDataDirectory);
        }

        #region Data Comparison Tests

        /// <summary>
        /// Tests comparison of current data with baseline data for all tables
        /// </summary>
        [TestMethod]
        public async Task CompareCurrentDataWithBaseline_AllTables_DetectsChanges()
        {
            // Arrange
            var queryExecutor = new SqlQueryExecutor(CreateConnection(), _mockLogger.Object);
            var tables = new List<string>
            {
                "Departments", "Positions", "Employees", "EmployeeAddresses",
                "EmployeeBankAccounts", "EmployeeCompensation", "EmployeeAbsences",
                "SecurityRoles", "SecurityGroups", "EmployeeSecurity"
            };

            // Act & Assert
            foreach (var table in tables)
            {
                // Get current data
                var currentQuery = $"SELECT * FROM {table} ORDER BY 1";
                var currentData = await queryExecutor.ExecuteQueryAsync<Dictionary<string, object>>(currentQuery);
                
                // Save current data
                var currentJson = System.Text.Json.JsonSerializer.Serialize(currentData, new System.Text.Json.JsonSerializerOptions
                {
                    WriteIndented = true
                });
                var currentFilePath = Path.Combine(_currentDataDirectory, $"{table}_current.json");
                await File.WriteAllTextAsync(currentFilePath, currentJson);
                
                // Load baseline data
                var baselineData = await LoadBaselineData($"{table}_baseline.json");
                
                // Compare data
                currentData.CountAssert.AreEqual(baselineData.Count, $"Table {table} should have same number of records as baseline");
                
                if (currentData.Any() && baselineData.Any())
                {
                    // Compare first record structure
                    var currentFirst = currentData.First();
                    var baselineFirst = baselineData.First();
                    
                    currentFirst.KeysCollectionAssert.AreEqual(baselineFirst.Keys, $"Table {table} should have same columns as baseline");
                    
                    // Compare data values (first few records)
                    var recordsToCompare = Math.Min(10, Math.Min(currentData.Count, baselineData.Count));
                    for (int i = 0; i < recordsToCompare; i++)
                    {
                        var currentRecord = currentData[i];
                        var baselineRecord = baselineData[i];
                        
                        foreach (var key in currentRecord.Keys)
                        {
                            var currentValue = currentRecord[key]?.ToString() ?? "";
                            var baselineValue = baselineRecord[key]?.ToString() ?? "";
                            
                            currentValueAssert.AreEqual(baselineValue, $"Table {table}, record {i}, column {key} should match baseline");
                        }
                    }
                }
            }
        }

        /// <summary>
        /// Tests comparison of payroll query results with baseline
        /// </summary>
        [TestMethod]
        public async Task ComparePayrollQueryResultsWithBaseline_AllQueries_DetectsChanges()
        {
            // Arrange
            var queryExecutor = new SqlQueryExecutor(CreateConnection(), _mockLogger.Object);
            var queries = new List<(string Name, string Query, Dictionary<string, object> Parameters)>
            {
                ("EmployeeStartData", LoadSqlScript("PayrollQueries/EmployeeStartData.sql"), new Dictionary<string, object>
                {
                    ["StartDate"] = new DateTime(2024, 1, 1),
                    ["EndDate"] = new DateTime(2024, 12, 31),
                    ["TenantID"] = "TENANT_001",
                    ["ClientID"] = "CLIENT_ABC",
                    ["ExtractionDate"] = DateTime.Now,
                    ["BatchID"] = "BATCH_20241201_001"
                }),
                ("EmployeeCompensationData", LoadSqlScript("PayrollQueries/EmployeeCompensationData.sql"), new Dictionary<string, object>
                {
                    ["ExtractionDate"] = DateTime.Now,
                    ["TenantID"] = "TENANT_001",
                    ["ClientID"] = "CLIENT_ABC"
                }),
                ("OrganizationalHierarchy", LoadSqlScript("PayrollQueries/OrganizationalHierarchy.sql"), new Dictionary<string, object>
                {
                    ["TenantID"] = "TENANT_001",
                    ["ClientID"] = "CLIENT_ABC"
                }),
                ("EmployeeAbsenceData", LoadSqlScript("PayrollQueries/EmployeeAbsenceData.sql"), new Dictionary<string, object>
                {
                    ["StartDate"] = new DateTime(2024, 1, 1),
                    ["EndDate"] = new DateTime(2024, 12, 31),
                    ["TenantID"] = "TENANT_001",
                    ["ClientID"] = "CLIENT_ABC"
                }),
                ("PayrollAnalyticsSummary", LoadSqlScript("AnalyticsQueries/PayrollAnalyticsSummary.sql"), new Dictionary<string, object>
                {
                    ["StartDate"] = new DateTime(2024, 1, 1),
                    ["EndDate"] = new DateTime(2024, 12, 31)
                }),
                ("EmployeeAnalyticsDashboard", LoadSqlScript("AnalyticsQueries/EmployeeAnalyticsDashboard.sql"), new Dictionary<string, object>
                {
                    ["StartDate"] = new DateTime(2024, 1, 1),
                    ["EndDate"] = new DateTime(2024, 12, 31)
                })
            };

            // Act & Assert
            foreach (var (name, query, parameters) in queries)
            {
                // Get current results
                var currentResults = await queryExecutor.ExecuteQueryAsync<Dictionary<string, object>>(query, parameters);
                
                // Save current results
                var currentJson = System.Text.Json.JsonSerializer.Serialize(currentResults, new System.Text.Json.JsonSerializerOptions
                {
                    WriteIndented = true
                });
                var currentFilePath = Path.Combine(_currentDataDirectory, $"{name}_current.json");
                await File.WriteAllTextAsync(currentFilePath, currentJson);
                
                // Load baseline results
                var baselineResults = await LoadBaselineData($"{name}_baseline.json");
                
                // Compare results
                currentResults.CountAssert.AreEqual(baselineResults.Count, $"Query {name} should return same number of records as baseline");
                
                if (currentResults.Any() && baselineResults.Any())
                {
                    // Compare result structure
                    var currentFirst = currentResults.First();
                    var baselineFirst = baselineResults.First();
                    
                    currentFirst.KeysCollectionAssert.AreEqual(baselineFirst.Keys, $"Query {name} should return same columns as baseline");
                    
                    // Compare data values (first few records)
                    var recordsToCompare = Math.Min(5, Math.Min(currentResults.Count, baselineResults.Count));
                    for (int i = 0; i < recordsToCompare; i++)
                    {
                        var currentRecord = currentResults[i];
                        var baselineRecord = baselineResults[i];
                        
                        foreach (var key in currentRecord.Keys)
                        {
                            var currentValue = currentRecord[key]?.ToString() ?? "";
                            var baselineValue = baselineRecord[key]?.ToString() ?? "";
                            
                            currentValueAssert.AreEqual(baselineValue, $"Query {name}, record {i}, column {key} should match baseline");
                        }
                    }
                }
            }
        }

        #endregion

        #region Scenario-Based Regression Tests

        /// <summary>
        /// Tests regression for specific scenarios
        /// </summary>
        [TestMethod]
        public async Task CompareScenarioResultsWithBaseline_VariousScenarios_DetectsChanges()
        {
            // Arrange
            var queryExecutor = new SqlQueryExecutor(CreateConnection(), _mockLogger.Object);
            var scenarios = new List<(string Name, string Query)>
            {
                ("ActiveEmployees", "SELECT * FROM Employees WHERE IsActive = 1 ORDER BY EmployeeID"),
                ("TerminatedEmployees", "SELECT * FROM Employees WHERE IsActive = 0 ORDER BY EmployeeID"),
                ("HighSalaryEmployees", "SELECT * FROM EmployeeCompensation WHERE BaseSalary > 100000 ORDER BY BaseSalary DESC"),
                ("RecentHires", "SELECT * FROM Employees WHERE StartDate >= '2024-01-01' ORDER BY StartDate DESC"),
                ("LongAbsences", "SELECT * FROM EmployeeAbsences WHERE TotalDays > 5 ORDER BY TotalDays DESC"),
                ("ManagerEmployees", "SELECT * FROM Employees WHERE ManagerID IS NOT NULL ORDER BY ManagerID, EmployeeID"),
                ("PIIAccessEmployees", "SELECT * FROM EmployeeSecurity WHERE CanViewPII = 1 ORDER BY EmployeeID")
            };

            // Act & Assert
            foreach (var (name, query) in scenarios)
            {
                // Get current results
                var currentResults = await queryExecutor.ExecuteQueryAsync<Dictionary<string, object>>(query);
                
                // Save current results
                var currentJson = System.Text.Json.JsonSerializer.Serialize(currentResults, new System.Text.Json.JsonSerializerOptions
                {
                    WriteIndented = true
                });
                var currentFilePath = Path.Combine(_currentDataDirectory, $"{name}_scenario_current.json");
                await File.WriteAllTextAsync(currentFilePath, currentJson);
                
                // Load baseline results
                var baselineResults = await LoadBaselineData($"{name}_scenario.json");
                
                // Compare results
                currentResults.CountAssert.AreEqual(baselineResults.Count, $"Scenario {name} should return same number of records as baseline");
                
                if (currentResults.Any() && baselineResults.Any())
                {
                    // Compare result structure
                    var currentFirst = currentResults.First();
                    var baselineFirst = baselineResults.First();
                    
                    currentFirst.KeysCollectionAssert.AreEqual(baselineFirst.Keys, $"Scenario {name} should return same columns as baseline");
                }
            }
        }

        #endregion

        #region Data Integrity Regression Tests

        /// <summary>
        /// Tests regression for data integrity checks
        /// </summary>
        [TestMethod]
        public async Task CompareDataIntegrityWithBaseline_AllChecks_DetectsChanges()
        {
            // Arrange
            var queryExecutor = new SqlQueryExecutor(CreateConnection(), _mockLogger.Object);
            var integrityChecks = new List<(string Name, string Query, int ExpectedCount)>
            {
                ("NullEmployeeNumbers", "SELECT COUNT(*) FROM Employees WHERE EmployeeNumber IS NULL OR EmployeeNumber = ''", 0),
                ("NullEmails", "SELECT COUNT(*) FROM Employees WHERE Email IS NULL OR Email = ''", 0),
                ("NullFirstNames", "SELECT COUNT(*) FROM Employees WHERE FirstName IS NULL OR FirstName = ''", 0),
                ("NullLastNames", "SELECT COUNT(*) FROM Employees WHERE LastName IS NULL OR LastName = ''", 0),
                ("NullStartDates", "SELECT COUNT(*) FROM Employees WHERE StartDate IS NULL", 0),
                ("InvalidBaseSalaries", "SELECT COUNT(*) FROM EmployeeCompensation WHERE BaseSalary IS NULL OR BaseSalary <= 0", 0),
                ("InvalidGrossSalaries", "SELECT COUNT(*) FROM EmployeeCompensation WHERE GrossSalary IS NULL OR GrossSalary <= 0", 0),
                ("InvalidNetSalaries", "SELECT COUNT(*) FROM EmployeeCompensation WHERE NetSalary IS NULL OR NetSalary <= 0", 0),
                ("NullAddressLine1", "SELECT COUNT(*) FROM EmployeeAddresses WHERE AddressLine1 IS NULL OR AddressLine1 = ''", 0),
                ("NullBankAccountNumbers", "SELECT COUNT(*) FROM EmployeeBankAccounts WHERE BankAccountNumber IS NULL OR BankAccountNumber = ''", 0)
            };

            // Act & Assert
            foreach (var (name, query, expectedCount) in integrityChecks)
            {
                var currentCount = await queryExecutor.ExecuteScalarAsync<int>(query);
                
                currentCountAssert.AreEqual(expectedCount, $"Data integrity check {name} should return {expectedCount} records");
            }
        }

        #endregion

        #region Performance Regression Tests

        /// <summary>
        /// Tests regression for query performance
        /// </summary>
        [TestMethod]
        public async Task CompareQueryPerformanceWithBaseline_AllQueries_DetectsPerformanceChanges()
        {
            // Arrange
            var queryExecutor = new SqlQueryExecutor(CreateConnection(), _mockLogger.Object);
            var queries = new List<(string Name, string Query, int MaxSeconds)>
            {
                ("EmployeeStartData", LoadSqlScript("PayrollQueries/EmployeeStartData.sql"), 5),
                ("EmployeeCompensationData", LoadSqlScript("PayrollQueries/EmployeeCompensationData.sql"), 10),
                ("OrganizationalHierarchy", LoadSqlScript("PayrollQueries/OrganizationalHierarchy.sql"), 15),
                ("EmployeeAbsenceData", LoadSqlScript("PayrollQueries/EmployeeAbsenceData.sql"), 8),
                ("PayrollAnalyticsSummary", LoadSqlScript("AnalyticsQueries/PayrollAnalyticsSummary.sql"), 20),
                ("EmployeeAnalyticsDashboard", LoadSqlScript("AnalyticsQueries/EmployeeAnalyticsDashboard.sql"), 25)
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
            foreach (var (name, query, maxSeconds) in queries)
            {
                var stopwatch = System.Diagnostics.Stopwatch.StartNew();
                var result = await queryExecutor.ExecuteQueryAsync<Dictionary<string, object>>(query, parameters);
                stopwatch.Stop();

                resultAssert.IsNotNull($"Query {name} should return results");
                stopwatch.Elapsed.TotalSecondsAssert.IsTrue(stopwatch.Elapsed.TotalSeconds < maxSeconds, $"Query {name} should execute within {maxSeconds} seconds");
                
                // Save performance data
                var performanceData = new
                {
                    QueryName = name,
                    ExecutionTimeSeconds = stopwatch.Elapsed.TotalSeconds,
                    RecordCount = result.Count,
                    Timestamp = DateTime.Now
                };
                
                var performanceJson = System.Text.Json.JsonSerializer.Serialize(performanceData, new System.Text.Json.JsonSerializerOptions
                {
                    WriteIndented = true
                });
                var performanceFilePath = Path.Combine(_currentDataDirectory, $"{name}_performance.json");
                await File.WriteAllTextAsync(performanceFilePath, performanceJson);
            }
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

        private async Task<List<Dictionary<string, object>>> LoadBaselineData(string fileName)
        {
            var filePath = Path.Combine(_baselineDataDirectory, fileName);
            if (!File.Exists(filePath))
            {
                return new List<Dictionary<string, object>>();
            }

            var json = await File.ReadAllTextAsync(filePath);
            return System.Text.Json.JsonSerializer.Deserialize<List<Dictionary<string, object>>>(json) ?? new List<Dictionary<string, object>>();
        }

        #endregion
    }
}
