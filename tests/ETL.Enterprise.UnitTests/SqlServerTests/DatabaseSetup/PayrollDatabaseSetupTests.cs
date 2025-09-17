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
    /// Tests for setting up payroll test database and generating comprehensive test data
    /// This class handles database creation, table setup, and test data generation
    /// </summary>
    [TestClass]
    public class PayrollDatabaseSetupTests
    {
        private Mock<ILogger<PayrollDatabaseSetupTests>> _mockLogger;
        private string _connectionString;
        private string _testDataDirectory;
        private string _baselineDataDirectory;

        [TestInitialize]
        public void TestInitialize()
        {
            _mockLogger = new Mock<ILogger<PayrollDatabaseSetupTests>>();
            _connectionString = Environment.GetEnvironmentVariable("PAYROLL_TEST_CONNECTION_STRING") 
                ?? "Server=localhost;Database=PayrollTestDB;Integrated Security=true;TrustServerCertificate=true;";
            
            _testDataDirectory = Path.Combine(AppDomain.CurrentDomain.BaseDirectory, "TestData");
            _baselineDataDirectory = Path.Combine(_testDataDirectory, "Baseline");
            
            // Create directories if they don't exist
            Directory.CreateDirectory(_testDataDirectory);
            Directory.CreateDirectory(_baselineDataDirectory);
        }

        #region Database Setup Tests

        /// <summary>
        /// Tests creation of payroll test database and tables
        /// </summary>
        [TestMethod]
        public async Task CreatePayrollTestDatabase_WithAllTables_CreatesSuccessfully()
        {
            // Arrange
            var setupScript = LoadSqlScript("DatabaseSetup/CreatePayrollTestDatabase.sql");
            var queryExecutor = new SqlQueryExecutor(CreateConnection(), _mockLogger.Object);

            // Act
            var result = await queryExecutor.ExecuteNonQueryAsync(setupScript);

            // Assert
            result.Should().BeGreaterThan(0, "Database setup should create tables successfully");
            
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
            tableCount.Should().Be(10, "All required tables should be created");
        }

        /// <summary>
        /// Tests database schema validation
        /// </summary>
        [TestMethod]
        public async Task ValidateDatabaseSchema_AllTables_ReturnsCorrectSchema()
        {
            // Arrange
            var queryExecutor = new SqlQueryExecutor(CreateConnection(), _mockLogger.Object);
            var expectedTables = new List<string>
            {
                "Departments", "Positions", "Employees", "EmployeeAddresses",
                "EmployeeBankAccounts", "EmployeeCompensation", "EmployeeAbsences",
                "SecurityRoles", "SecurityGroups", "EmployeeSecurity"
            };

            // Act
            var schemaQuery = @"
                SELECT TABLE_NAME, COLUMN_NAME, DATA_TYPE, IS_NULLABLE, COLUMN_DEFAULT
                FROM INFORMATION_SCHEMA.COLUMNS
                WHERE TABLE_NAME IN (
                    'Departments', 'Positions', 'Employees', 'EmployeeAddresses',
                    'EmployeeBankAccounts', 'EmployeeCompensation', 'EmployeeAbsences',
                    'SecurityRoles', 'SecurityGroups', 'EmployeeSecurity'
                )
                ORDER BY TABLE_NAME, ORDINAL_POSITION";

            var schemaData = await queryExecutor.ExecuteQueryAsync<SchemaInfo>(schemaQuery);

            // Assert
            schemaData.Should().NotBeNull();
            schemaData.Should().NotBeEmpty();
            
            var actualTables = schemaData.Select(s => s.TABLE_NAME).Distinct().ToList();
            actualTables.Should().Contain(expectedTables);
            
            // Verify key columns exist
            var employeeColumns = schemaData.Where(s => s.TABLE_NAME == "Employees").Select(s => s.COLUMN_NAME).ToList();
            employeeColumns.Should().Contain("EmployeeID");
            employeeColumns.Should().Contain("EmployeeNumber");
            employeeColumns.Should().Contain("FirstName");
            employeeColumns.Should().Contain("LastName");
            employeeColumns.Should().Contain("Email");
        }

        #endregion

        #region Test Data Generation Tests

        /// <summary>
        /// Tests generation of comprehensive test data
        /// </summary>
        [TestMethod]
        public async Task GenerateComprehensiveTestData_WithAllScenarios_CreatesExpectedData()
        {
            // Arrange
            var dataGenerationScript = LoadSqlScript("TestDataGeneration/GenerateComprehensiveTestData.sql");
            var queryExecutor = new SqlQueryExecutor(CreateConnection(), _mockLogger.Object);

            // Act
            var result = await queryExecutor.ExecuteNonQueryAsync(dataGenerationScript);

            // Assert
            result.Should().BeGreaterThan(0, "Test data generation should insert records successfully");

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

            var counts = await queryExecutor.ExecuteQueryAsync<DataCounts>(countQuery);

            counts.Should().NotBeNull();
            counts.Should().HaveCount(1);
            
            var count = counts.First();
            count.DepartmentCount.Should().Be(10, "Should have 10 departments");
            count.PositionCount.Should().Be(28, "Should have 28 positions");
            count.EmployeeCount.Should().Be(1002, "Should have 1002 employees (1000 generated + 2 edge cases)");
            count.AddressCount.Should().Be(1002, "Should have addresses for all employees");
            count.BankAccountCount.Should().Be(1002, "Should have bank accounts for all employees");
            count.CompensationCount.Should().Be(1002, "Should have compensation for all employees");
            count.RoleCount.Should().Be(7, "Should have 7 security roles");
            count.GroupCount.Should().Be(6, "Should have 6 security groups");
            count.SecurityCount.Should().Be(1002, "Should have security records for all employees");
        }

        /// <summary>
        /// Tests data quality and integrity
        /// </summary>
        [TestMethod]
        public async Task ValidateTestDataQuality_AllTables_ReturnsValidData()
        {
            // Arrange
            var queryExecutor = new SqlQueryExecutor(CreateConnection(), _mockLogger.Object);

            // Act & Assert - Check for data quality issues
            var qualityChecks = new List<(string Query, string Description)>
            {
                (@"SELECT COUNT(*) FROM Employees WHERE EmployeeNumber IS NULL OR EmployeeNumber = ''", 
                 "Employees with null or empty employee numbers"),
                
                (@"SELECT COUNT(*) FROM Employees WHERE Email IS NULL OR Email = ''", 
                 "Employees with null or empty emails"),
                
                (@"SELECT COUNT(*) FROM Employees WHERE FirstName IS NULL OR FirstName = ''", 
                 "Employees with null or empty first names"),
                
                (@"SELECT COUNT(*) FROM Employees WHERE LastName IS NULL OR LastName = ''", 
                 "Employees with null or empty last names"),
                
                (@"SELECT COUNT(*) FROM Employees WHERE StartDate IS NULL", 
                 "Employees with null start dates"),
                
                (@"SELECT COUNT(*) FROM EmployeeCompensation WHERE BaseSalary IS NULL OR BaseSalary <= 0", 
                 "Employees with invalid base salary"),
                
                (@"SELECT COUNT(*) FROM EmployeeCompensation WHERE GrossSalary IS NULL OR GrossSalary <= 0", 
                 "Employees with invalid gross salary"),
                
                (@"SELECT COUNT(*) FROM EmployeeCompensation WHERE NetSalary IS NULL OR NetSalary <= 0", 
                 "Employees with invalid net salary"),
                
                (@"SELECT COUNT(*) FROM EmployeeAddresses WHERE AddressLine1 IS NULL OR AddressLine1 = ''", 
                 "Employees with null or empty address line 1"),
                
                (@"SELECT COUNT(*) FROM EmployeeBankAccounts WHERE BankAccountNumber IS NULL OR BankAccountNumber = ''", 
                 "Employees with null or empty bank account numbers")
            };

            foreach (var (query, description) in qualityChecks)
            {
                var count = await queryExecutor.ExecuteScalarAsync<int>(query);
                count.Should().Be(0, description);
            }
        }

        /// <summary>
        /// Tests edge case data scenarios
        /// </summary>
        [TestMethod]
        public async Task ValidateEdgeCaseData_AllScenarios_ReturnsExpectedResults()
        {
            // Arrange
            var queryExecutor = new SqlQueryExecutor(CreateConnection(), _mockLogger.Object);

            // Act & Assert - Check edge cases
            var edgeCaseQueries = new List<(string Query, int ExpectedCount, string Description)>
            {
                (@"SELECT COUNT(*) FROM Employees WHERE IsActive = 0", 100, "Terminated employees"),
                (@"SELECT COUNT(*) FROM Employees WHERE ManagerID IS NULL", 10, "Employees without managers"),
                (@"SELECT COUNT(*) FROM EmployeeAbsences WHERE IsApproved = 0", 0, "Unapproved absences"),
                (@"SELECT COUNT(*) FROM EmployeeSecurity WHERE SecurityLevel = 'High'", 10, "High security level employees"),
                (@"SELECT COUNT(*) FROM EmployeeSecurity WHERE CanViewPII = 1", 10, "Employees with PII access"),
                (@"SELECT COUNT(*) FROM EmployeeCompensation WHERE BaseSalary > 100000", 10, "High salary employees"),
                (@"SELECT COUNT(*) FROM EmployeeAbsences WHERE TotalDays > 10", 1, "Long absences")
            };

            foreach (var (query, expectedCount, description) in edgeCaseQueries)
            {
                var count = await queryExecutor.ExecuteScalarAsync<int>(query);
                count.Should().Be(expectedCount, description);
            }
        }

        #endregion

        #region Baseline Data Generation Tests

        /// <summary>
        /// Tests generation of baseline data for regression testing
        /// </summary>
        [TestMethod]
        public async Task GenerateBaselineData_AllTables_ExportsToJson()
        {
            // Arrange
            var queryExecutor = new SqlQueryExecutor(CreateConnection(), _mockLogger.Object);
            var tables = new List<string>
            {
                "Departments", "Positions", "Employees", "EmployeeAddresses",
                "EmployeeBankAccounts", "EmployeeCompensation", "EmployeeAbsences",
                "SecurityRoles", "SecurityGroups", "EmployeeSecurity"
            };

            // Act
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

            // Assert
            foreach (var table in tables)
            {
                var filePath = Path.Combine(_baselineDataDirectory, $"{table}_baseline.json");
                File.Exists(filePath).Should().BeTrue($"Baseline file for {table} should exist");
                
                var fileContent = await File.ReadAllTextAsync(filePath);
                fileContent.Should().NotBeNullOrEmpty($"Baseline file for {table} should not be empty");
            }
        }

        /// <summary>
        /// Tests generation of baseline data for specific scenarios
        /// </summary>
        [TestMethod]
        public async Task GenerateScenarioBaselineData_VariousScenarios_ExportsToJson()
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

            // Act
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

            // Assert
            foreach (var (name, _) in scenarios)
            {
                var filePath = Path.Combine(_baselineDataDirectory, $"{name}_scenario.json");
                File.Exists(filePath).Should().BeTrue($"Scenario baseline file for {name} should exist");
                
                var fileContent = await File.ReadAllTextAsync(filePath);
                fileContent.Should().NotBeNullOrEmpty($"Scenario baseline file for {name} should not be empty");
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
            var fullPath = Path.Combine(AppDomain.CurrentDomain.BaseDirectory, "SqlServerTests", "SqlFiles", scriptPath);
            return File.ReadAllText(fullPath);
        }

        #endregion
    }

    #region Test Data Classes

    public class SchemaInfo
    {
        public string TABLE_NAME { get; set; }
        public string COLUMN_NAME { get; set; }
        public string DATA_TYPE { get; set; }
        public string IS_NULLABLE { get; set; }
        public string COLUMN_DEFAULT { get; set; }
    }

    public class DataCounts
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
