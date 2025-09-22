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
    /// Orchestrates comprehensive payroll testing including database setup, data generation, query validation, and regression testing
    /// This class provides a complete testing workflow for payroll data extraction and analytics
    /// </summary>
    [TestClass]
    public class PayrollTestOrchestrator
    {
        private Mock<ILogger<PayrollTestOrchestrator>> _mockLogger;
        private string _connectionString;
        private string _testDataDirectory;
        private string _baselineDataDirectory;
        private string _currentDataDirectory;
        private string _sqlFilesDirectory;

        [TestInitialize]
        public void TestInitialize()
        {
            _mockLogger = new Mock<ILogger<PayrollTestOrchestrator>>();
            _connectionString = Environment.GetEnvironmentVariable("PAYROLL_TEST_CONNECTION_STRING") 
                ?? "Server=localhost;Database=PayrollTestDB;Integrated Security=true;TrustServerCertificate=true;";
            
            _testDataDirectory = Path.Combine(AppDomain.CurrentDomain.BaseDirectory, "TestData");
            _baselineDataDirectory = Path.Combine(_testDataDirectory, "Baseline");
            _currentDataDirectory = Path.Combine(_testDataDirectory, "Current");
            _sqlFilesDirectory = Path.Combine(AppDomain.CurrentDomain.BaseDirectory, "SqlServerTests", "SqlFiles");
            
            // Create directories if they don't exist
            Directory.CreateDirectory(_testDataDirectory);
            Directory.CreateDirectory(_baselineDataDirectory);
            Directory.CreateDirectory(_currentDataDirectory);
        }

        #region Complete Testing Workflow

        /// <summary>
        /// Executes complete payroll testing workflow
        /// </summary>
        [TestMethod]
        public async Task ExecuteCompletePayrollTestingWorkflow_AllPhases_CompletesSuccessfully()
        {
            // Phase 1: Database Setup
            await ExecuteDatabaseSetupPhase();
            
            // Phase 2: Test Data Generation
            await ExecuteTestDataGenerationPhase();
            
            // Phase 3: Baseline Data Generation
            await ExecuteBaselineDataGenerationPhase();
            
            // Phase 4: Query Validation
            await ExecuteQueryValidationPhase();
            
            // Phase 5: Regression Testing
            await ExecuteRegressionTestingPhase();
            
            // Phase 6: Performance Testing
            await ExecutePerformanceTestingPhase();
            
            // Phase 7: Data Integrity Testing
            await ExecuteDataIntegrityTestingPhase();
            
            // Phase 8: Generate Test Report
            await GenerateTestReport();
        }

        #endregion

        #region Individual Testing Phases

        /// <summary>
        /// Executes database setup phase
        /// </summary>
        private async Task ExecuteDatabaseSetupPhase()
        {
            _mockLogger.Object.LogInformation("Starting Database Setup Phase...");
            
            var queryExecutor = new SqlQueryExecutor(CreateConnection(), _mockLogger.Object);
            
            // Create database and tables
            var setupScript = LoadSqlScript("DatabaseSetup/CreatePayrollTestDatabase.sql");
            var result = await queryExecutor.ExecuteNonQueryAsync(setupScript);
            
            resultAssert.IsTrue(result > 0, "Database setup should create tables successfully");
            
            // Verify tables exist
            var tableCheckQuery = @"
                SELECT COUNT(*) as TableCount
                FROM INFORMATION_SCHEMA.TABLES 
                WHERE TABLE_TYPE = 'BASE TABLE' 
                AND TABLE_NAME IN (
                    'Departments', 'Positions', 'Employees', 'EmployeeAddresses',
                    'EmployeeBankAccounts', 'EmployeeCompensation', 'EmployeeAbsences',
                    'SecurityRoles', 'SecurityGroups', 'EmployeeSecurity'
                )";

            var tableCount = await queryExecutor.ExecuteScalarAsync<int>(tableCheckQuery);
            tableCountAssert.AreEqual(10, "All required tables should be created");
            
            _mockLogger.Object.LogInformation("Database Setup Phase completed successfully.");
        }

        /// <summary>
        /// Executes test data generation phase
        /// </summary>
        private async Task ExecuteTestDataGenerationPhase()
        {
            _mockLogger.Object.LogInformation("Starting Test Data Generation Phase...");
            
            var queryExecutor = new SqlQueryExecutor(CreateConnection(), _mockLogger.Object);
            
            // Generate comprehensive test data
            var dataGenerationScript = LoadSqlScript("TestDataGeneration/GenerateComprehensiveTestData.sql");
            var result = await queryExecutor.ExecuteNonQueryAsync(dataGenerationScript);
            
            resultAssert.IsTrue(result > 0, "Test data generation should insert records successfully");
            
            // Verify data counts
            var countQuery = @"
                SELECT 
                    (SELECT COUNT(*) FROM Departments) as DepartmentCount,
                    (SELECT COUNT(*) FROM Positions) as PositionCount,
                    (SELECT COUNT(*) FROM Employees) as EmployeeCount,
                    (SELECT COUNT(*) FROM EmployeeAddresses) as AddressCount,
                    (SELECT COUNT(*) FROM EmployeeBankAccounts) as BankAccountCount,
                    (SELECT COUNT(*) FROM EmployeeCompensation) as CompensationCount,
                    (SELECT COUNT(*) FROM EmployeeAbsences) as AbsenceCount,
                    (SELECT COUNT(*) FROM SecurityRoles) as RoleCount,
                    (SELECT COUNT(*) FROM SecurityGroups) as GroupCount,
                    (SELECT COUNT(*) FROM EmployeeSecurity) as SecurityCount";

            var counts = await queryExecutor.ExecuteQueryAsync<PayrollDataCounts>(countQuery);
            countsAssert.IsNotNull();
            countsAssert.AreEqual(1);
            
            var count = counts.First();
            count.EmployeeCountAssert.AreEqual(1002, "Should have 1002 employees");
            count.DepartmentCountAssert.AreEqual(10, "Should have 10 departments");
            count.PositionCountAssert.AreEqual(28, "Should have 28 positions");
            
            _mockLogger.Object.LogInformation("Test Data Generation Phase completed successfully.");
        }

        /// <summary>
        /// Executes baseline data generation phase
        /// </summary>
        private async Task ExecuteBaselineDataGenerationPhase()
        {
            _mockLogger.Object.LogInformation("Starting Baseline Data Generation Phase...");
            
            var queryExecutor = new SqlQueryExecutor(CreateConnection(), _mockLogger.Object);
            var tables = new List<string>
            {
                "Departments", "Positions", "Employees", "EmployeeAddresses",
                "EmployeeBankAccounts", "EmployeeCompensation", "EmployeeAbsences",
                "SecurityRoles", "SecurityGroups", "EmployeeSecurity"
            };

            // Generate baseline data for all tables
            foreach (var table in tables)
            {
                var query = $"SELECT * FROM {table} ORDER BY 1";
                var data = await queryExecutor.ExecuteQueryAsync<Dictionary<string, object>>(query);
                
                var json = System.Text.Json.JsonSerializer.Serialize(data, new System.Text.Json.JsonSerializerOptions
                {
                    WriteIndented = true
                });
                
                var filePath = Path.Combine(_baselineDataDirectory, $"{table}_baseline.json");
                await File.WriteAllTextAsync(filePath, json);
            }

            // Generate baseline data for scenarios
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

            foreach (var (name, query) in scenarios)
            {
                var data = await queryExecutor.ExecuteQueryAsync<Dictionary<string, object>>(query);
                
                var json = System.Text.Json.JsonSerializer.Serialize(data, new System.Text.Json.JsonSerializerOptions
                {
                    WriteIndented = true
                });
                
                var filePath = Path.Combine(_baselineDataDirectory, $"{name}_scenario.json");
                await File.WriteAllTextAsync(filePath, json);
            }
            
            _mockLogger.Object.LogInformation("Baseline Data Generation Phase completed successfully.");
        }

        /// <summary>
        /// Executes query validation phase
        /// </summary>
        private async Task ExecuteQueryValidationPhase()
        {
            _mockLogger.Object.LogInformation("Starting Query Validation Phase...");
            
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

            foreach (var (name, query, parameters) in queries)
            {
                var result = await queryExecutor.ExecuteQueryAsync<Dictionary<string, object>>(query, parameters);
                
                resultAssert.IsNotNull($"Query {name} should return results");
                resultAssert.IsTrue(result.Count > 0, $"Query {name} should return data");
                
                // Save current results for comparison
                var currentJson = System.Text.Json.JsonSerializer.Serialize(result, new System.Text.Json.JsonSerializerOptions
                {
                    WriteIndented = true
                });
                var currentFilePath = Path.Combine(_currentDataDirectory, $"{name}_current.json");
                await File.WriteAllTextAsync(currentFilePath, currentJson);
            }
            
            _mockLogger.Object.LogInformation("Query Validation Phase completed successfully.");
        }

        /// <summary>
        /// Executes regression testing phase
        /// </summary>
        private async Task ExecuteRegressionTestingPhase()
        {
            _mockLogger.Object.LogInformation("Starting Regression Testing Phase...");
            
            var queryExecutor = new SqlQueryExecutor(CreateConnection(), _mockLogger.Object);
            var tables = new List<string>
            {
                "Departments", "Positions", "Employees", "EmployeeAddresses",
                "EmployeeBankAccounts", "EmployeeCompensation", "EmployeeAbsences",
                "SecurityRoles", "SecurityGroups", "EmployeeSecurity"
            };

            foreach (var table in tables)
            {
                // Get current data
                var currentQuery = $"SELECT * FROM {table} ORDER BY 1";
                var currentData = await queryExecutor.ExecuteQueryAsync<Dictionary<string, object>>(currentQuery);
                
                // Load baseline data
                var baselineData = await LoadBaselineData($"{table}_baseline.json");
                
                // Compare data
                currentData.CountAssert.AreEqual(baselineData.Count, $"Table {table} should have same number of records as baseline");
                
                if (currentData.Any() && baselineData.Any())
                {
                    var currentFirst = currentData.First();
                    var baselineFirst = baselineData.First();
                    
                    currentFirst.KeysCollectionAssert.AreEqual(baselineFirst.Keys, $"Table {table} should have same columns as baseline");
                }
            }
            
            _mockLogger.Object.LogInformation("Regression Testing Phase completed successfully.");
        }

        /// <summary>
        /// Executes performance testing phase
        /// </summary>
        private async Task ExecutePerformanceTestingPhase()
        {
            _mockLogger.Object.LogInformation("Starting Performance Testing Phase...");
            
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
            
            _mockLogger.Object.LogInformation("Performance Testing Phase completed successfully.");
        }

        /// <summary>
        /// Executes data integrity testing phase
        /// </summary>
        private async Task ExecuteDataIntegrityTestingPhase()
        {
            _mockLogger.Object.LogInformation("Starting Data Integrity Testing Phase...");
            
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

            foreach (var (name, query, expectedCount) in integrityChecks)
            {
                var currentCount = await queryExecutor.ExecuteScalarAsync<int>(query);
                currentCountAssert.AreEqual(expectedCount, $"Data integrity check {name} should return {expectedCount} records");
            }
            
            _mockLogger.Object.LogInformation("Data Integrity Testing Phase completed successfully.");
        }

        /// <summary>
        /// Generates comprehensive test report
        /// </summary>
        private async Task GenerateTestReport()
        {
            _mockLogger.Object.LogInformation("Generating Test Report...");
            
            var report = new
            {
                TestExecutionDate = DateTime.Now,
                TestPhases = new[]
                {
                    "Database Setup",
                    "Test Data Generation",
                    "Baseline Data Generation",
                    "Query Validation",
                    "Regression Testing",
                    "Performance Testing",
                    "Data Integrity Testing"
                },
                TestResults = new
                {
                    DatabaseSetup = "PASSED",
                    TestDataGeneration = "PASSED",
                    BaselineDataGeneration = "PASSED",
                    QueryValidation = "PASSED",
                    RegressionTesting = "PASSED",
                    PerformanceTesting = "PASSED",
                    DataIntegrityTesting = "PASSED"
                },
                TestSummary = new
                {
                    TotalTables = 10,
                    TotalEmployees = 1002,
                    TotalDepartments = 10,
                    TotalPositions = 28,
                    TotalQueries = 6,
                    TotalScenarios = 7,
                    TotalIntegrityChecks = 10
                }
            };
            
            var reportJson = System.Text.Json.JsonSerializer.Serialize(report, new System.Text.Json.JsonSerializerOptions
            {
                WriteIndented = true
            });
            
            var reportFilePath = Path.Combine(_testDataDirectory, "PayrollTestReport.json");
            await File.WriteAllTextAsync(reportFilePath, reportJson);
            
            _mockLogger.Object.LogInformation("Test Report generated successfully.");
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

    #region Test Data Classes

    public class PayrollDataCounts
    {
        public int DepartmentCount { get; set; }
        public int PositionCount { get; set; }
        public int EmployeeCount { get; set; }
        public int AddressCount { get; set; }
        public int BankAccountCount { get; set; }
        public int CompensationCount { get; set; }
        public int AbsenceCount { get; set; }
        public int RoleCount { get; set; }
        public int GroupCount { get; set; }
        public int SecurityCount { get; set; }
    }

    #endregion
}
