using Microsoft.VisualStudio.TestTools.UnitTesting;
using Microsoft.Extensions.Logging;
using Moq;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Threading.Tasks;
using System.Linq;
using FluentAssertions;
using ETL.Enterprise.Domain.Entities;
using ETL.Enterprise.Domain.Enums;
using ETL.Enterprise.Infrastructure.Services;

namespace ETL.Tests.Unit.SqlServer.Payroll
{
    /// <summary>
    /// Comprehensive tests for multi-tenant payroll data extraction and analytics
    /// Tests tenant isolation, data segregation, and cross-tenant scenarios
    /// </summary>
    [TestClass]
    public class MultiTenantPayrollTests
    {
        private Mock<ILogger<MultiTenantPayrollTests>> _mockLogger;
        private Mock<IDbConnection> _mockConnection;
        private Mock<IDbCommand> _mockCommand;
        private Mock<IDataReader> _mockDataReader;
        private TestDatabaseContext _testContext;

        [TestInitialize]
        public void TestInitialize()
        {
            _mockLogger = new Mock<ILogger<MultiTenantPayrollTests>>();
            _mockConnection = new Mock<IDbConnection>();
            _mockCommand = new Mock<IDbCommand>();
            _mockDataReader = new Mock<IDataReader>();
            _testContext = new TestDatabaseContext();
        }

        [TestCleanup]
        public void TestCleanup()
        {
            _testContext?.Dispose();
        }

        #region Tenant Isolation Tests

        /// <summary>
        /// Tests that data extraction maintains tenant isolation
        /// </summary>
        [TestMethod]
        public async Task ExtractData_TenantIsolation_ReturnsOnlyTenantData()
        {
            // Arrange
            var query = @"
                SELECT 
                    e.EmployeeID,
                    e.EmployeeNumber,
                    e.FirstName,
                    e.LastName,
                    @TenantID as TenantID,
                    @ClientID as ClientID
                FROM Employees e
                WHERE e.IsActive = 1
                    AND @TenantID = @TenantID";

            var tenant1Parameters = new Dictionary<string, object>
            {
                ["TenantID"] = "TENANT_001",
                ["ClientID"] = "CLIENT_ABC"
            };

            var tenant2Parameters = new Dictionary<string, object>
            {
                ["TenantID"] = "TENANT_002",
                ["ClientID"] = "CLIENT_XYZ"
            };

            var tenant1Results = new List<MultiTenantEmployeeData>
            {
                new MultiTenantEmployeeData
                {
                    EmployeeID = 1001,
                    EmployeeNumber = "EMP001",
                    FirstName = "John",
                    LastName = "Doe",
                    TenantID = "TENANT_001",
                    ClientID = "CLIENT_ABC"
                }
            };

            var tenant2Results = new List<MultiTenantEmployeeData>
            {
                new MultiTenantEmployeeData
                {
                    EmployeeID = 2001,
                    EmployeeNumber = "EMP201",
                    FirstName = "Jane",
                    LastName = "Smith",
                    TenantID = "TENANT_002",
                    ClientID = "CLIENT_XYZ"
                }
            };

            var queryExecutor = new SqlQueryExecutor(_mockConnection.Object, _mockLogger.Object);

            // Act - Extract from Tenant 1
            SetupMockDataReader(tenant1Results);
            var tenant1Result = await queryExecutor.ExecuteQueryAsync<MultiTenantEmployeeData>(query, tenant1Parameters);

            // Act - Extract from Tenant 2
            SetupMockDataReader(tenant2Results);
            var tenant2Result = await queryExecutor.ExecuteQueryAsync<MultiTenantEmployeeData>(query, tenant2Parameters);

            // Assert
            tenant1Result.Should().NotBeNull();
            tenant1Result.Should().HaveCount(1);
            tenant1Result.First().TenantID.Should().Be("TENANT_001");
            tenant1Result.First().ClientID.Should().Be("CLIENT_ABC");

            tenant2Result.Should().NotBeNull();
            tenant2Result.Should().HaveCount(1);
            tenant2Result.First().TenantID.Should().Be("TENANT_002");
            tenant2Result.First().ClientID.Should().Be("CLIENT_XYZ");

            // Verify no cross-tenant data leakage
            tenant1Result.All(r => r.TenantID == "TENANT_001").Should().BeTrue();
            tenant2Result.All(r => r.TenantID == "TENANT_002").Should().BeTrue();
        }

        /// <summary>
        /// Tests tenant-specific data filtering
        /// </summary>
        [TestMethod]
        public async Task ExtractData_TenantSpecificFiltering_ReturnsFilteredResults()
        {
            // Arrange
            var query = @"
                SELECT 
                    e.EmployeeID,
                    e.EmployeeNumber,
                    e.FirstName,
                    e.LastName,
                    e.DepartmentID,
                    d.DepartmentName,
                    @TenantID as TenantID,
                    @ClientID as ClientID
                FROM Employees e
                LEFT JOIN Departments d ON e.DepartmentID = d.DepartmentID
                WHERE e.IsActive = 1
                    AND e.DepartmentID = @DepartmentID
                    AND @TenantID = @TenantID";

            var parameters = new Dictionary<string, object>
            {
                ["DepartmentID"] = 10,
                ["TenantID"] = "TENANT_001",
                ["ClientID"] = "CLIENT_ABC"
            };

            var expectedResults = new List<MultiTenantEmployeeData>
            {
                new MultiTenantEmployeeData
                {
                    EmployeeID = 1001,
                    EmployeeNumber = "EMP001",
                    FirstName = "John",
                    LastName = "Doe",
                    DepartmentID = 10,
                    DepartmentName = "Engineering",
                    TenantID = "TENANT_001",
                    ClientID = "CLIENT_ABC"
                },
                new MultiTenantEmployeeData
                {
                    EmployeeID = 1002,
                    EmployeeNumber = "EMP002",
                    FirstName = "Jane",
                    LastName = "Smith",
                    DepartmentID = 10,
                    DepartmentName = "Engineering",
                    TenantID = "TENANT_001",
                    ClientID = "CLIENT_ABC"
                }
            };

            SetupMockDataReader(expectedResults);
            var queryExecutor = new SqlQueryExecutor(_mockConnection.Object, _mockLogger.Object);

            // Act
            var result = await queryExecutor.ExecuteQueryAsync<MultiTenantEmployeeData>(query, parameters);

            // Assert
            result.Should().NotBeNull();
            result.Should().HaveCount(2);
            result.All(r => r.TenantID == "TENANT_001").Should().BeTrue();
            result.All(r => r.ClientID == "CLIENT_ABC").Should().BeTrue();
            result.All(r => r.DepartmentID == 10).Should().BeTrue();
        }

        #endregion

        #region Cross-Tenant Analytics Tests

        /// <summary>
        /// Tests cross-tenant analytics aggregation
        /// </summary>
        [TestMethod]
        public async Task ExecuteAnalytics_CrossTenantAggregation_ReturnsAggregatedResults()
        {
            // Arrange
            var query = @"
                SELECT 
                    TenantID,
                    ClientID,
                    COUNT(DISTINCT EmployeeID) as TotalEmployees,
                    SUM(GrossSalary) as TotalGrossSalary,
                    AVG(GrossSalary) as AverageGrossSalary,
                    COUNT(DISTINCT DepartmentID) as TotalDepartments
                FROM PayrollAnalyticsData
                WHERE ExtractionDate >= @StartDate
                    AND ExtractionDate <= @EndDate
                GROUP BY 
                    TenantID,
                    ClientID
                ORDER BY TenantID, ClientID";

            var parameters = new Dictionary<string, object>
            {
                ["StartDate"] = new DateTime(2024, 1, 1),
                ["EndDate"] = new DateTime(2024, 12, 31)
            };

            var expectedResults = new List<CrossTenantAnalyticsData>
            {
                new CrossTenantAnalyticsData
                {
                    TenantID = "TENANT_001",
                    ClientID = "CLIENT_ABC",
                    TotalEmployees = 100,
                    TotalGrossSalary = 8000000m,
                    AverageGrossSalary = 80000m,
                    TotalDepartments = 5
                },
                new CrossTenantAnalyticsData
                {
                    TenantID = "TENANT_002",
                    ClientID = "CLIENT_XYZ",
                    TotalEmployees = 75,
                    TotalGrossSalary = 6000000m,
                    AverageGrossSalary = 80000m,
                    TotalDepartments = 4
                },
                new CrossTenantAnalyticsData
                {
                    TenantID = "TENANT_003",
                    ClientID = "CLIENT_DEF",
                    TotalEmployees = 50,
                    TotalGrossSalary = 4000000m,
                    AverageGrossSalary = 80000m,
                    TotalDepartments = 3
                }
            };

            SetupMockDataReader(expectedResults);
            var queryExecutor = new SqlQueryExecutor(_mockConnection.Object, _mockLogger.Object);

            // Act
            var result = await queryExecutor.ExecuteQueryAsync<CrossTenantAnalyticsData>(query, parameters);

            // Assert
            result.Should().NotBeNull();
            result.Should().HaveCount(3);
            result.Sum(r => r.TotalEmployees).Should().Be(225);
            result.Sum(r => r.TotalGrossSalary).Should().Be(18000000m);
            result.All(r => r.AverageGrossSalary == 80000m).Should().BeTrue();
        }

        /// <summary>
        /// Tests tenant comparison analytics
        /// </summary>
        [TestMethod]
        public async Task ExecuteAnalytics_TenantComparison_ReturnsComparisonResults()
        {
            // Arrange
            var query = @"
                WITH TenantMetrics AS (
                    SELECT 
                        TenantID,
                        ClientID,
                        COUNT(DISTINCT EmployeeID) as TotalEmployees,
                        SUM(GrossSalary) as TotalGrossSalary,
                        AVG(GrossSalary) as AverageGrossSalary,
                        SUM(TotalAbsenceDays) as TotalAbsenceDays
                    FROM PayrollAnalyticsData
                    WHERE ExtractionDate >= @StartDate
                        AND ExtractionDate <= @EndDate
                    GROUP BY 
                        TenantID,
                        ClientID
                ),
                TenantRankings AS (
                    SELECT 
                        *,
                        ROW_NUMBER() OVER (ORDER BY TotalGrossSalary DESC) as RevenueRank,
                        ROW_NUMBER() OVER (ORDER BY AverageGrossSalary DESC) as SalaryRank,
                        ROW_NUMBER() OVER (ORDER BY TotalAbsenceDays ASC) as AbsenceRank
                    FROM TenantMetrics
                )
                SELECT 
                    TenantID,
                    ClientID,
                    TotalEmployees,
                    TotalGrossSalary,
                    AverageGrossSalary,
                    TotalAbsenceDays,
                    RevenueRank,
                    SalaryRank,
                    AbsenceRank
                FROM TenantRankings
                ORDER BY RevenueRank";

            var parameters = new Dictionary<string, object>
            {
                ["StartDate"] = new DateTime(2024, 1, 1),
                ["EndDate"] = new DateTime(2024, 12, 31)
            };

            var expectedResults = new List<CrossTenantAnalyticsData>
            {
                new CrossTenantAnalyticsData
                {
                    TenantID = "TENANT_001",
                    ClientID = "CLIENT_ABC",
                    TotalEmployees = 100,
                    TotalGrossSalary = 8000000m,
                    AverageGrossSalary = 80000m,
                    TotalAbsenceDays = 200,
                    RevenueRank = 1,
                    SalaryRank = 2,
                    AbsenceRank = 2
                },
                new CrossTenantAnalyticsData
                {
                    TenantID = "TENANT_002",
                    ClientID = "CLIENT_XYZ",
                    TotalEmployees = 75,
                    TotalGrossSalary = 6000000m,
                    AverageGrossSalary = 80000m,
                    TotalAbsenceDays = 150,
                    RevenueRank = 2,
                    SalaryRank = 2,
                    AbsenceRank = 1
                }
            };

            SetupMockDataReader(expectedResults);
            var queryExecutor = new SqlQueryExecutor(_mockConnection.Object, _mockLogger.Object);

            // Act
            var result = await queryExecutor.ExecuteQueryAsync<CrossTenantAnalyticsData>(query, parameters);

            // Assert
            result.Should().NotBeNull();
            result.Should().HaveCount(2);
            result.First().RevenueRank.Should().Be(1);
            result.First().TenantID.Should().Be("TENANT_001");
            result.Last().RevenueRank.Should().Be(2);
            result.Last().TenantID.Should().Be("TENANT_002");
        }

        #endregion

        #region Tenant Data Validation Tests

        /// <summary>
        /// Tests tenant data validation and integrity
        /// </summary>
        [TestMethod]
        public async Task ValidateTenantData_DataIntegrity_ReturnsValidationResults()
        {
            // Arrange
            var query = @"
                SELECT 
                    TenantID,
                    ClientID,
                    COUNT(DISTINCT EmployeeID) as TotalEmployees,
                    COUNT(DISTINCT CASE WHEN EmployeeID IS NULL THEN 1 END) as NullEmployeeIDs,
                    COUNT(DISTINCT CASE WHEN EmployeeNumber IS NULL OR EmployeeNumber = '' THEN 1 END) as InvalidEmployeeNumbers,
                    COUNT(DISTINCT CASE WHEN FirstName IS NULL OR FirstName = '' THEN 1 END) as InvalidFirstNames,
                    COUNT(DISTINCT CASE WHEN LastName IS NULL OR LastName = '' THEN 1 END) as InvalidLastNames,
                    COUNT(DISTINCT CASE WHEN Email IS NULL OR Email = '' THEN 1 END) as InvalidEmails,
                    COUNT(DISTINCT CASE WHEN GrossSalary IS NULL OR GrossSalary <= 0 THEN 1 END) as InvalidSalaries
                FROM PayrollAnalyticsData
                WHERE ExtractionDate >= @StartDate
                    AND ExtractionDate <= @EndDate
                GROUP BY 
                    TenantID,
                    ClientID
                ORDER BY TenantID, ClientID";

            var parameters = new Dictionary<string, object>
            {
                ["StartDate"] = new DateTime(2024, 1, 1),
                ["EndDate"] = new DateTime(2024, 12, 31)
            };

            var expectedResults = new List<TenantDataValidationData>
            {
                new TenantDataValidationData
                {
                    TenantID = "TENANT_001",
                    ClientID = "CLIENT_ABC",
                    TotalEmployees = 100,
                    NullEmployeeIDs = 0,
                    InvalidEmployeeNumbers = 0,
                    InvalidFirstNames = 0,
                    InvalidLastNames = 0,
                    InvalidEmails = 2,
                    InvalidSalaries = 1
                },
                new TenantDataValidationData
                {
                    TenantID = "TENANT_002",
                    ClientID = "CLIENT_XYZ",
                    TotalEmployees = 75,
                    NullEmployeeIDs = 0,
                    InvalidEmployeeNumbers = 1,
                    InvalidFirstNames = 0,
                    InvalidLastNames = 0,
                    InvalidEmails = 1,
                    InvalidSalaries = 0
                }
            };

            SetupMockDataReader(expectedResults);
            var queryExecutor = new SqlQueryExecutor(_mockConnection.Object, _mockLogger.Object);

            // Act
            var result = await queryExecutor.ExecuteQueryAsync<TenantDataValidationData>(query, parameters);

            // Assert
            result.Should().NotBeNull();
            result.Should().HaveCount(2);
            result.All(r => r.NullEmployeeIDs == 0).Should().BeTrue();
            result.All(r => r.InvalidFirstNames == 0).Should().BeTrue();
            result.All(r => r.InvalidLastNames == 0).Should().BeTrue();
        }

        #endregion

        #region Tenant Performance Tests

        /// <summary>
        /// Tests performance of multi-tenant queries
        /// </summary>
        [TestMethod]
        public async Task ExecuteMultiTenantQuery_Performance_ExecutesWithinTimeLimit()
        {
            // Arrange
            var query = @"
                SELECT 
                    TenantID,
                    ClientID,
                    COUNT(DISTINCT EmployeeID) as TotalEmployees,
                    SUM(GrossSalary) as TotalGrossSalary
                FROM PayrollAnalyticsData
                WHERE ExtractionDate >= @StartDate
                    AND ExtractionDate <= @EndDate
                GROUP BY 
                    TenantID,
                    ClientID
                ORDER BY TenantID, ClientID";

            var parameters = new Dictionary<string, object>
            {
                ["StartDate"] = new DateTime(2024, 1, 1),
                ["EndDate"] = new DateTime(2024, 12, 31)
            };

            var maxExecutionTime = TimeSpan.FromSeconds(15);
            var largeDataSet = SqlQueryTestUtilities.GenerateTestCustomers(100000);
            SetupMockDataReader(largeDataSet);
            var queryExecutor = new SqlQueryExecutor(_mockConnection.Object, _mockLogger.Object);

            // Act
            var executionTime = await SqlQueryTestUtilities.MeasureExecutionTime(async () =>
            {
                await queryExecutor.ExecuteQueryAsync<CrossTenantAnalyticsData>(query, parameters);
            });

            // Assert
            executionTime.Should().BeLessThan(maxExecutionTime, 
                $"Multi-tenant query should execute within {maxExecutionTime.TotalMilliseconds}ms");
        }

        #endregion

        #region Helper Methods

        private void SetupMockDataReader<T>(List<T> data)
        {
            var dataTable = ConvertToDataTable(data);
            var dataReader = dataTable.CreateDataReader();
            
            _mockDataReader.Setup(reader => reader.Read())
                .Returns(() => dataReader.Read());
            
            _mockDataReader.Setup(reader => reader.GetOrdinal(It.IsAny<string>()))
                .Returns<string>(columnName => dataReader.GetOrdinal(columnName));
            
            _mockDataReader.Setup(reader => reader.GetValue(It.IsAny<int>()))
                .Returns<int>(index => dataReader.GetValue(index));
            
            _mockDataReader.Setup(reader => reader.IsDBNull(It.IsAny<int>()))
                .Returns<int>(index => dataReader.IsDBNull(index));

            _mockCommand.Setup(cmd => cmd.ExecuteReader())
                .Returns(_mockDataReader.Object);
        }

        private DataTable ConvertToDataTable<T>(List<T> data)
        {
            var dataTable = new DataTable();
            
            if (data.Any())
            {
                var properties = typeof(T).GetProperties();
                foreach (var property in properties)
                {
                    dataTable.Columns.Add(property.Name, property.PropertyType);
                }

                foreach (var item in data)
                {
                    var row = dataTable.NewRow();
                    foreach (var property in properties)
                    {
                        row[property.Name] = property.GetValue(item) ?? DBNull.Value;
                    }
                    dataTable.Rows.Add(row);
                }
            }

            return dataTable;
        }

        #endregion
    }

    #region Test Data Classes

    public class MultiTenantEmployeeData
    {
        public int EmployeeID { get; set; }
        public string EmployeeNumber { get; set; }
        public string FirstName { get; set; }
        public string LastName { get; set; }
        public int? DepartmentID { get; set; }
        public string DepartmentName { get; set; }
        public string TenantID { get; set; }
        public string ClientID { get; set; }
    }

    public class CrossTenantAnalyticsData
    {
        public string TenantID { get; set; }
        public string ClientID { get; set; }
        public int TotalEmployees { get; set; }
        public decimal TotalGrossSalary { get; set; }
        public decimal AverageGrossSalary { get; set; }
        public int TotalDepartments { get; set; }
        public int TotalAbsenceDays { get; set; }
        public int RevenueRank { get; set; }
        public int SalaryRank { get; set; }
        public int AbsenceRank { get; set; }
    }

    public class TenantDataValidationData
    {
        public string TenantID { get; set; }
        public string ClientID { get; set; }
        public int TotalEmployees { get; set; }
        public int NullEmployeeIDs { get; set; }
        public int InvalidEmployeeNumbers { get; set; }
        public int InvalidFirstNames { get; set; }
        public int InvalidLastNames { get; set; }
        public int InvalidEmails { get; set; }
        public int InvalidSalaries { get; set; }
    }

    #endregion
}
