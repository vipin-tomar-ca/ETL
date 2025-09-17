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
    /// Tests for validating payroll queries against real database with actual data
    /// This class validates query syntax, structure, and results against baseline data
    /// </summary>
    [TestClass]
    public class PayrollQueryValidationTests
    {
        private Mock<ILogger<PayrollQueryValidationTests>> _mockLogger;
        private string _connectionString;
        private string _baselineDataDirectory;
        private string _sqlFilesDirectory;

        [TestInitialize]
        public void TestInitialize()
        {
            _mockLogger = new Mock<ILogger<PayrollQueryValidationTests>>();
            _connectionString = Environment.GetEnvironmentVariable("PAYROLL_TEST_CONNECTION_STRING") 
                ?? "Server=localhost;Database=PayrollTestDB;Integrated Security=true;TrustServerCertificate=true;";
            
            _baselineDataDirectory = Path.Combine(AppDomain.CurrentDomain.BaseDirectory, "TestData", "Baseline");
            _sqlFilesDirectory = Path.Combine(AppDomain.CurrentDomain.BaseDirectory, "SqlServerTests", "SqlFiles");
        }

        #region Individual Table Query Validation

        /// <summary>
        /// Tests validation of individual table queries against real data
        /// </summary>
        [TestMethod]
        public async Task ValidateIndividualTableQueries_AllTables_ReturnsExpectedResults()
        {
            // Arrange
            var queryExecutor = new SqlQueryExecutor(CreateConnection(), _mockLogger.Object);
            var tableQueries = new List<(string TableName, string Query)>
            {
                ("Departments", "SELECT * FROM Departments ORDER BY DepartmentID"),
                ("Positions", "SELECT * FROM Positions ORDER BY PositionID"),
                ("Employees", "SELECT * FROM Employees ORDER BY EmployeeID"),
                ("EmployeeAddresses", "SELECT * FROM EmployeeAddresses ORDER BY AddressID"),
                ("EmployeeBankAccounts", "SELECT * FROM EmployeeBankAccounts ORDER BY BankAccountID"),
                ("EmployeeCompensation", "SELECT * FROM EmployeeCompensation ORDER BY CompensationID"),
                ("EmployeeAbsences", "SELECT * FROM EmployeeAbsences ORDER BY AbsenceID"),
                ("SecurityRoles", "SELECT * FROM SecurityRoles ORDER BY RoleID"),
                ("SecurityGroups", "SELECT * FROM SecurityGroups ORDER BY SecurityGroupID"),
                ("EmployeeSecurity", "SELECT * FROM EmployeeSecurity ORDER BY SecurityID")
            };

            // Act & Assert
            foreach (var (tableName, query) in tableQueries)
            {
                var result = await queryExecutor.ExecuteQueryAsync<Dictionary<string, object>>(query);
                
                result.Should().NotBeNull($"Query for {tableName} should return results");
                result.Should().NotBeEmpty($"Query for {tableName} should return data");
                
                // Validate against baseline data
                var baselineData = await LoadBaselineData($"{tableName}_baseline.json");
                result.Count.Should().Be(baselineData.Count, $"Query for {tableName} should return same number of records as baseline");
                
                // Validate first record structure
                if (result.Any())
                {
                    var firstResult = result.First();
                    var firstBaseline = baselineData.First();
                    
                    firstResult.Keys.Should().BeEquivalentTo(firstBaseline.Keys, $"Query for {tableName} should return same columns as baseline");
                }
            }
        }

        /// <summary>
        /// Tests validation of filtered table queries
        /// </summary>
        [TestMethod]
        public async Task ValidateFilteredTableQueries_VariousFilters_ReturnsExpectedResults()
        {
            // Arrange
            var queryExecutor = new SqlQueryExecutor(CreateConnection(), _mockLogger.Object);
            var filteredQueries = new List<(string Description, string Query, int ExpectedMinCount)>
            {
                ("Active Employees", "SELECT * FROM Employees WHERE IsActive = 1", 900),
                ("Terminated Employees", "SELECT * FROM Employees WHERE IsActive = 0", 100),
                ("High Salary Employees", "SELECT * FROM EmployeeCompensation WHERE BaseSalary > 100000", 10),
                ("Recent Hires", "SELECT * FROM Employees WHERE StartDate >= '2024-01-01'", 0),
                ("Long Absences", "SELECT * FROM EmployeeAbsences WHERE TotalDays > 5", 1),
                ("Manager Employees", "SELECT * FROM Employees WHERE ManagerID IS NOT NULL", 990),
                ("PII Access Employees", "SELECT * FROM EmployeeSecurity WHERE CanViewPII = 1", 10)
            };

            // Act & Assert
            foreach (var (description, query, expectedMinCount) in filteredQueries)
            {
                var result = await queryExecutor.ExecuteQueryAsync<Dictionary<string, object>>(query);
                
                result.Should().NotBeNull($"Filtered query '{description}' should return results");
                result.Count.Should().BeGreaterOrEqualTo(expectedMinCount, $"Filtered query '{description}' should return at least {expectedMinCount} records");
            }
        }

        #endregion

        #region Payroll Query Validation

        /// <summary>
        /// Tests validation of employee start data query
        /// </summary>
        [TestMethod]
        public async Task ValidateEmployeeStartDataQuery_WithRealData_ReturnsExpectedResults()
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

            // Assert
            result.Should().NotBeNull("Employee start data query should return results");
            result.Should().NotBeEmpty("Employee start data query should return data");
            
            // Validate result structure
            var firstResult = result.First();
            firstResult.Should().ContainKey("EmployeeID");
            firstResult.Should().ContainKey("EmployeeNumber");
            firstResult.Should().ContainKey("FirstName");
            firstResult.Should().ContainKey("LastName");
            firstResult.Should().ContainKey("StartDate");
            firstResult.Should().ContainKey("TenantID");
            firstResult.Should().ContainKey("ClientID");
            
            // Validate tenant isolation
            result.All(r => r["TenantID"].ToString() == "TENANT_001").Should().BeTrue("All results should have correct tenant ID");
            result.All(r => r["ClientID"].ToString() == "CLIENT_ABC").Should().BeTrue("All results should have correct client ID");
        }

        /// <summary>
        /// Tests validation of employee compensation data query
        /// </summary>
        [TestMethod]
        public async Task ValidateEmployeeCompensationDataQuery_WithRealData_ReturnsExpectedResults()
        {
            // Arrange
            var queryExecutor = new SqlQueryExecutor(CreateConnection(), _mockLogger.Object);
            var query = LoadSqlScript("PayrollQueries/EmployeeCompensationData.sql");
            var parameters = new Dictionary<string, object>
            {
                ["ExtractionDate"] = DateTime.Now,
                ["TenantID"] = "TENANT_001",
                ["ClientID"] = "CLIENT_ABC"
            };

            // Act
            var result = await queryExecutor.ExecuteQueryAsync<Dictionary<string, object>>(query, parameters);

            // Assert
            result.Should().NotBeNull("Employee compensation data query should return results");
            result.Should().NotBeEmpty("Employee compensation data query should return data");
            
            // Validate result structure
            var firstResult = result.First();
            firstResult.Should().ContainKey("EmployeeID");
            firstResult.Should().ContainKey("BaseSalary");
            firstResult.Should().ContainKey("GrossSalary");
            firstResult.Should().ContainKey("NetSalary");
            firstResult.Should().ContainKey("TotalAllowances");
            firstResult.Should().ContainKey("TotalDeductions");
            
            // Validate data integrity
            result.All(r => Convert.ToDecimal(r["BaseSalary"]) > 0).Should().BeTrue("All base salaries should be positive");
            result.All(r => Convert.ToDecimal(r["GrossSalary"]) > 0).Should().BeTrue("All gross salaries should be positive");
            result.All(r => Convert.ToDecimal(r["NetSalary"]) > 0).Should().BeTrue("All net salaries should be positive");
        }

        /// <summary>
        /// Tests validation of organizational hierarchy query
        /// </summary>
        [TestMethod]
        public async Task ValidateOrganizationalHierarchyQuery_WithRealData_ReturnsExpectedResults()
        {
            // Arrange
            var queryExecutor = new SqlQueryExecutor(CreateConnection(), _mockLogger.Object);
            var query = LoadSqlScript("PayrollQueries/OrganizationalHierarchy.sql");
            var parameters = new Dictionary<string, object>
            {
                ["TenantID"] = "TENANT_001",
                ["ClientID"] = "CLIENT_ABC"
            };

            // Act
            var result = await queryExecutor.ExecuteQueryAsync<Dictionary<string, object>>(query, parameters);

            // Assert
            result.Should().NotBeNull("Organizational hierarchy query should return results");
            result.Should().NotBeEmpty("Organizational hierarchy query should return data");
            
            // Validate result structure
            var firstResult = result.First();
            firstResult.Should().ContainKey("EmployeeID");
            firstResult.Should().ContainKey("HierarchyLevel");
            firstResult.Should().ContainKey("HierarchyPath");
            firstResult.Should().ContainKey("DirectReportsCount");
            firstResult.Should().ContainKey("TotalReportsCount");
            
            // Validate hierarchy data
            result.All(r => Convert.ToInt32(r["HierarchyLevel"]) >= 0).Should().BeTrue("All hierarchy levels should be non-negative");
            result.All(r => Convert.ToInt32(r["DirectReportsCount"]) >= 0).Should().BeTrue("All direct reports counts should be non-negative");
        }

        /// <summary>
        /// Tests validation of employee absence data query
        /// </summary>
        [TestMethod]
        public async Task ValidateEmployeeAbsenceDataQuery_WithRealData_ReturnsExpectedResults()
        {
            // Arrange
            var queryExecutor = new SqlQueryExecutor(CreateConnection(), _mockLogger.Object);
            var query = LoadSqlScript("PayrollQueries/EmployeeAbsenceData.sql");
            var parameters = new Dictionary<string, object>
            {
                ["StartDate"] = new DateTime(2024, 1, 1),
                ["EndDate"] = new DateTime(2024, 12, 31),
                ["TenantID"] = "TENANT_001",
                ["ClientID"] = "CLIENT_ABC"
            };

            // Act
            var result = await queryExecutor.ExecuteQueryAsync<Dictionary<string, object>>(query, parameters);

            // Assert
            result.Should().NotBeNull("Employee absence data query should return results");
            
            if (result.Any())
            {
                // Validate result structure
                var firstResult = result.First();
                firstResult.Should().ContainKey("AbsenceID");
                firstResult.Should().ContainKey("EmployeeID");
                firstResult.Should().ContainKey("AbsenceType");
                firstResult.Should().ContainKey("StartDate");
                firstResult.Should().ContainKey("EndDate");
                firstResult.Should().ContainKey("TotalDays");
                firstResult.Should().ContainKey("IsApproved");
                firstResult.Should().ContainKey("IsPaid");
                
                // Validate absence data
                result.All(r => Convert.ToDecimal(r["TotalDays"]) > 0).Should().BeTrue("All absence days should be positive");
                result.All(r => Convert.ToDateTime(r["StartDate"]) <= Convert.ToDateTime(r["EndDate"])).Should().BeTrue("Start date should be before or equal to end date");
            }
        }

        #endregion

        #region Analytics Query Validation

        /// <summary>
        /// Tests validation of payroll analytics summary query
        /// </summary>
        [TestMethod]
        public async Task ValidatePayrollAnalyticsSummaryQuery_WithRealData_ReturnsExpectedResults()
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
            var result = await queryExecutor.ExecuteQueryAsync<Dictionary<string, object>>(query, parameters);

            // Assert
            result.Should().NotBeNull("Payroll analytics summary query should return results");
            result.Should().NotBeEmpty("Payroll analytics summary query should return data");
            
            // Validate result structure
            var firstResult = result.First();
            firstResult.Should().ContainKey("TenantID");
            firstResult.Should().ContainKey("ClientID");
            firstResult.Should().ContainKey("TotalEmployees");
            firstResult.Should().ContainKey("ActiveEmployees");
            firstResult.Should().ContainKey("TotalGrossSalary");
            firstResult.Should().ContainKey("AverageGrossSalary");
            firstResult.Should().ContainKey("CostPerEmployee");
            
            // Validate analytics data
            result.All(r => Convert.ToInt32(r["TotalEmployees"]) > 0).Should().BeTrue("All departments should have employees");
            result.All(r => Convert.ToInt32(r["ActiveEmployees"]) >= 0).Should().BeTrue("Active employees count should be non-negative");
            result.All(r => Convert.ToDecimal(r["TotalGrossSalary"]) > 0).Should().BeTrue("Total gross salary should be positive");
            result.All(r => Convert.ToDecimal(r["CostPerEmployee"]) > 0).Should().BeTrue("Cost per employee should be positive");
        }

        /// <summary>
        /// Tests validation of employee analytics dashboard query
        /// </summary>
        [TestMethod]
        public async Task ValidateEmployeeAnalyticsDashboardQuery_WithRealData_ReturnsExpectedResults()
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
            var result = await queryExecutor.ExecuteQueryAsync<Dictionary<string, object>>(query, parameters);

            // Assert
            result.Should().NotBeNull("Employee analytics dashboard query should return results");
            result.Should().NotBeEmpty("Employee analytics dashboard query should return data");
            
            // Validate result structure
            var firstResult = result.First();
            firstResult.Should().ContainKey("EmployeeID");
            firstResult.Should().ContainKey("BaseSalary");
            firstResult.Should().ContainKey("SalaryRank");
            firstResult.Should().ContainKey("SalaryPercentile");
            firstResult.Should().ContainKey("DepartmentAvgSalary");
            firstResult.Should().ContainKey("CompanyAvgSalary");
            
            // Validate analytics data
            result.All(r => Convert.ToInt32(r["SalaryRank"]) > 0).Should().BeTrue("All salary ranks should be positive");
            result.All(r => Convert.ToDouble(r["SalaryPercentile"]) >= 0 && Convert.ToDouble(r["SalaryPercentile"]) <= 1).Should().BeTrue("Salary percentiles should be between 0 and 1");
        }

        #endregion

        #region Performance Validation

        /// <summary>
        /// Tests performance of payroll queries with real data
        /// </summary>
        [TestMethod]
        public async Task ValidateQueryPerformance_AllPayrollQueries_ExecutesWithinTimeLimit()
        {
            // Arrange
            var queryExecutor = new SqlQueryExecutor(CreateConnection(), _mockLogger.Object);
            var queries = new List<(string Name, string Query, int MaxSeconds)>
            {
                ("Employee Start Data", LoadSqlScript("PayrollQueries/EmployeeStartData.sql"), 5),
                ("Employee Compensation Data", LoadSqlScript("PayrollQueries/EmployeeCompensationData.sql"), 10),
                ("Organizational Hierarchy", LoadSqlScript("PayrollQueries/OrganizationalHierarchy.sql"), 15),
                ("Employee Absence Data", LoadSqlScript("PayrollQueries/EmployeeAbsenceData.sql"), 8),
                ("Payroll Analytics Summary", LoadSqlScript("AnalyticsQueries/PayrollAnalyticsSummary.sql"), 20),
                ("Employee Analytics Dashboard", LoadSqlScript("AnalyticsQueries/EmployeeAnalyticsDashboard.sql"), 25)
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

                result.Should().NotBeNull($"Query '{name}' should return results");
                stopwatch.Elapsed.TotalSeconds.Should().BeLessThan(maxSeconds, $"Query '{name}' should execute within {maxSeconds} seconds");
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
