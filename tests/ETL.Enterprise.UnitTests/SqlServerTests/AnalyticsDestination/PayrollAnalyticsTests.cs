using Microsoft.VisualStudio.TestTools.UnitTesting;
using Microsoft.Extensions.Logging;
using Moq;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Threading.Tasks;
using System.Linq;
using ETL.Enterprise.Domain.Entities;
using ETL.Enterprise.Domain.Enums;
using ETL.Enterprise.Infrastructure.Services;

namespace ETL.Tests.Unit.SqlServer.Payroll
{
    /// <summary>
    /// Comprehensive tests for payroll analytics queries in destination analytics database
    /// Tests analytics queries for payroll data aggregation and reporting
    /// </summary>
    [TestClass]
    public class PayrollAnalyticsTests
    {
        private Mock<ILogger<PayrollAnalyticsTests>> _mockLogger;
        private Mock<IDbConnection> _mockConnection;
        private Mock<IDbCommand> _mockCommand;
        private Mock<IDataReader> _mockDataReader;
        private TestDatabaseContext _testContext;

        [TestInitialize]
        public void TestInitialize()
        {
            _mockLogger = new Mock<ILogger<PayrollAnalyticsTests>>();
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

        #region Payroll Analytics Summary Tests

        /// <summary>
        /// Tests payroll analytics summary query with aggregations
        /// </summary>
        [TestMethod]
        public async Task ExecutePayrollAnalyticsSummary_WithAggregations_ReturnsExpectedResults()
        {
            // Arrange
            var query = @"
                WITH PayrollSummary AS (
                    SELECT 
                        TenantID,
                        ClientID,
                        DepartmentID,
                        DepartmentName,
                        BusinessUnit,
                        Region,
                        Country,
                        COUNT(DISTINCT EmployeeID) as TotalEmployees,
                        COUNT(DISTINCT CASE WHEN IsActive = 1 THEN EmployeeID END) as ActiveEmployees,
                        SUM(BaseSalary) as TotalBaseSalary,
                        AVG(BaseSalary) as AverageBaseSalary,
                        SUM(TotalAllowances) as TotalAllowances,
                        SUM(TotalDeductions) as TotalDeductions,
                        SUM(GrossSalary) as TotalGrossSalary,
                        SUM(NetSalary) as TotalNetSalary,
                        SUM(TotalAbsenceDays) as TotalAbsenceDays,
                        AVG(TotalAbsenceDays) as AverageAbsenceDays
                    FROM PayrollAnalyticsData
                    WHERE ExtractionDate >= @StartDate
                        AND ExtractionDate <= @EndDate
                    GROUP BY 
                        TenantID,
                        ClientID,
                        DepartmentID,
                        DepartmentName,
                        BusinessUnit,
                        Region,
                        Country
                )
                SELECT 
                    TenantID,
                    ClientID,
                    DepartmentID,
                    DepartmentName,
                    BusinessUnit,
                    Region,
                    Country,
                    TotalEmployees,
                    ActiveEmployees,
                    TotalBaseSalary,
                    AverageBaseSalary,
                    TotalAllowances,
                    TotalDeductions,
                    TotalGrossSalary,
                    TotalNetSalary,
                    TotalAbsenceDays,
                    AverageAbsenceDays,
                    CASE 
                        WHEN TotalEmployees > 0 THEN (TotalGrossSalary / TotalEmployees)
                        ELSE 0 
                    END as CostPerEmployee,
                    CASE 
                        WHEN TotalGrossSalary > 0 THEN (TotalDeductions * 100.0 / TotalGrossSalary)
                        ELSE 0 
                    END as DeductionPercentage
                FROM PayrollSummary
                ORDER BY TenantID, ClientID, DepartmentID";

            var parameters = new Dictionary<string, object>
            {
                ["StartDate"] = new DateTime(2024, 1, 1),
                ["EndDate"] = new DateTime(2024, 12, 31)
            };

            var expectedResults = new List<PayrollAnalyticsSummaryData>
            {
                new PayrollAnalyticsSummaryData
                {
                    TenantID = "TENANT_001",
                    ClientID = "CLIENT_ABC",
                    DepartmentID = 10,
                    DepartmentName = "Engineering",
                    BusinessUnit = "Technology",
                    Region = "North America",
                    Country = "USA",
                    TotalEmployees = 50,
                    ActiveEmployees = 48,
                    TotalBaseSalary = 3750000m,
                    AverageBaseSalary = 75000m,
                    TotalAllowances = 250000m,
                    TotalDeductions = 750000m,
                    TotalGrossSalary = 4000000m,
                    TotalNetSalary = 3250000m,
                    TotalAbsenceDays = 120,
                    AverageAbsenceDays = 2.4m,
                    CostPerEmployee = 80000m,
                    DeductionPercentage = 18.75m
                }
            };

            SetupMockDataReader(expectedResults);
            var queryExecutor = new SqlQueryExecutor(_mockConnection.Object, _mockLogger.Object);

            // Act
            var result = await queryExecutor.ExecuteQueryAsync<PayrollAnalyticsSummaryData>(query, parameters);

            // Assert
            Assert.IsNotNull(result);
            Assert.AreEqual(1);
            result.First().TenantIDAssert.AreEqual("TENANT_001");
            result.First().TotalEmployeesAssert.AreEqual(50);
            result.First().ActiveEmployeesAssert.AreEqual(48);
            result.First().CostPerEmployeeAssert.AreEqual(80000m);
            result.First().DeductionPercentageAssert.AreEqual(18.75m);
        }

        /// <summary>
        /// Tests payroll analytics summary with tenant filtering
        /// </summary>
        [TestMethod]
        public async Task ExecutePayrollAnalyticsSummary_WithTenantFilter_ReturnsFilteredResults()
        {
            // Arrange
            var query = @"
                SELECT 
                    TenantID,
                    ClientID,
                    DepartmentID,
                    DepartmentName,
                    COUNT(DISTINCT EmployeeID) as TotalEmployees,
                    SUM(GrossSalary) as TotalGrossSalary,
                    AVG(GrossSalary) as AverageGrossSalary
                FROM PayrollAnalyticsData
                WHERE ExtractionDate >= @StartDate
                    AND ExtractionDate <= @EndDate
                    AND TenantID = @TenantID
                GROUP BY 
                    TenantID,
                    ClientID,
                    DepartmentID,
                    DepartmentName
                ORDER BY TenantID, ClientID, DepartmentID";

            var parameters = new Dictionary<string, object>
            {
                ["StartDate"] = new DateTime(2024, 1, 1),
                ["EndDate"] = new DateTime(2024, 12, 31),
                ["TenantID"] = "TENANT_001"
            };

            var expectedResults = new List<PayrollAnalyticsSummaryData>
            {
                new PayrollAnalyticsSummaryData
                {
                    TenantID = "TENANT_001",
                    ClientID = "CLIENT_ABC",
                    DepartmentID = 10,
                    DepartmentName = "Engineering",
                    TotalEmployees = 50,
                    TotalGrossSalary = 4000000m,
                    AverageBaseSalary = 80000m
                },
                new PayrollAnalyticsSummaryData
                {
                    TenantID = "TENANT_001",
                    ClientID = "CLIENT_ABC",
                    DepartmentID = 20,
                    DepartmentName = "Sales",
                    TotalEmployees = 30,
                    TotalGrossSalary = 2400000m,
                    AverageBaseSalary = 80000m
                }
            };

            SetupMockDataReader(expectedResults);
            var queryExecutor = new SqlQueryExecutor(_mockConnection.Object, _mockLogger.Object);

            // Act
            var result = await queryExecutor.ExecuteQueryAsync<PayrollAnalyticsSummaryData>(query, parameters);

            // Assert
            Assert.IsNotNull(result);
            Assert.AreEqual(2);
            result.All(r => r.TenantID == "TENANT_001"), "All items should match condition");
            result.Sum(r => r.TotalEmployees)Assert.AreEqual(80);
            result.Sum(r => r.TotalGrossSalary)Assert.AreEqual(6400000m);
        }

        #endregion

        #region Employee Analytics Dashboard Tests

        /// <summary>
        /// Tests employee analytics dashboard query with rankings
        /// </summary>
        [TestMethod]
        public async Task ExecuteEmployeeAnalyticsDashboard_WithRankings_ReturnsExpectedResults()
        {
            // Arrange
            var query = @"
                WITH EmployeeMetrics AS (
                    SELECT 
                        TenantID,
                        ClientID,
                        EmployeeID,
                        EmployeeNumber,
                        FirstName,
                        LastName,
                        DepartmentID,
                        DepartmentName,
                        BaseSalary,
                        TotalAbsenceDays,
                        TenureMonths
                    FROM PayrollAnalyticsData
                    WHERE ExtractionDate >= @StartDate
                        AND ExtractionDate <= @EndDate
                ),
                EmployeeRankings AS (
                    SELECT 
                        *,
                        ROW_NUMBER() OVER (PARTITION BY TenantID, ClientID, DepartmentID ORDER BY BaseSalary DESC) as SalaryRank,
                        RANK() OVER (PARTITION BY TenantID, ClientID, DepartmentID ORDER BY BaseSalary DESC) as SalaryRankWithTies,
                        PERCENT_RANK() OVER (PARTITION BY TenantID, ClientID, DepartmentID ORDER BY BaseSalary) as SalaryPercentile
                    FROM EmployeeMetrics
                )
                SELECT 
                    TenantID,
                    ClientID,
                    EmployeeID,
                    EmployeeNumber,
                    FirstName,
                    LastName,
                    DepartmentID,
                    DepartmentName,
                    BaseSalary,
                    TotalAbsenceDays,
                    TenureMonths,
                    SalaryRank,
                    SalaryRankWithTies,
                    SalaryPercentile
                FROM EmployeeRankings
                ORDER BY TenantID, ClientID, DepartmentID, SalaryRank";

            var parameters = new Dictionary<string, object>
            {
                ["StartDate"] = new DateTime(2024, 1, 1),
                ["EndDate"] = new DateTime(2024, 12, 31)
            };

            var expectedResults = new List<EmployeeAnalyticsDashboardData>
            {
                new EmployeeAnalyticsDashboardData
                {
                    TenantID = "TENANT_001",
                    ClientID = "CLIENT_ABC",
                    EmployeeID = 1001,
                    EmployeeNumber = "EMP001",
                    FirstName = "John",
                    LastName = "Doe",
                    DepartmentID = 10,
                    DepartmentName = "Engineering",
                    BaseSalary = 100000m,
                    TotalAbsenceDays = 5,
                    TenureMonths = 24,
                    SalaryRank = 1,
                    SalaryRankWithTies = 1,
                    SalaryPercentile = 0.95
                },
                new EmployeeAnalyticsDashboardData
                {
                    TenantID = "TENANT_001",
                    ClientID = "CLIENT_ABC",
                    EmployeeID = 1002,
                    EmployeeNumber = "EMP002",
                    FirstName = "Jane",
                    LastName = "Smith",
                    DepartmentID = 10,
                    DepartmentName = "Engineering",
                    BaseSalary = 90000m,
                    TotalAbsenceDays = 3,
                    TenureMonths = 18,
                    SalaryRank = 2,
                    SalaryRankWithTies = 2,
                    SalaryPercentile = 0.85
                }
            };

            SetupMockDataReader(expectedResults);
            var queryExecutor = new SqlQueryExecutor(_mockConnection.Object, _mockLogger.Object);

            // Act
            var result = await queryExecutor.ExecuteQueryAsync<EmployeeAnalyticsDashboardData>(query, parameters);

            // Assert
            Assert.IsNotNull(result);
            Assert.AreEqual(2);
            result.First().SalaryRankAssert.AreEqual(1);
            result.First().SalaryPercentileAssert.AreEqual(0.95);
            result.Last().SalaryRankAssert.AreEqual(2);
            result.Last().SalaryPercentileAssert.AreEqual(0.85);
        }

        /// <summary>
        /// Tests employee analytics dashboard with department comparisons
        /// </summary>
        [TestMethod]
        public async Task ExecuteEmployeeAnalyticsDashboard_WithDepartmentComparisons_ReturnsExpectedResults()
        {
            // Arrange
            var query = @"
                WITH EmployeeComparisons AS (
                    SELECT 
                        TenantID,
                        ClientID,
                        EmployeeID,
                        EmployeeNumber,
                        FirstName,
                        LastName,
                        DepartmentID,
                        DepartmentName,
                        BaseSalary,
                        TotalAbsenceDays,
                        TenureMonths,
                        -- Department Averages
                        AVG(BaseSalary) OVER (PARTITION BY TenantID, ClientID, DepartmentID) as DepartmentAvgSalary,
                        AVG(TotalAbsenceDays) OVER (PARTITION BY TenantID, ClientID, DepartmentID) as DepartmentAvgAbsenceDays,
                        AVG(TenureMonths) OVER (PARTITION BY TenantID, ClientID, DepartmentID) as DepartmentAvgTenureMonths,
                        -- Company Averages
                        AVG(BaseSalary) OVER (PARTITION BY TenantID, ClientID) as CompanyAvgSalary,
                        AVG(TotalAbsenceDays) OVER (PARTITION BY TenantID, ClientID) as CompanyAvgAbsenceDays,
                        AVG(TenureMonths) OVER (PARTITION BY TenantID, ClientID) as CompanyAvgTenureMonths
                    FROM PayrollAnalyticsData
                    WHERE ExtractionDate >= @StartDate
                        AND ExtractionDate <= @EndDate
                )
                SELECT 
                    TenantID,
                    ClientID,
                    EmployeeID,
                    EmployeeNumber,
                    FirstName,
                    LastName,
                    DepartmentID,
                    DepartmentName,
                    BaseSalary,
                    TotalAbsenceDays,
                    TenureMonths,
                    DepartmentAvgSalary,
                    DepartmentAvgAbsenceDays,
                    DepartmentAvgTenureMonths,
                    CompanyAvgSalary,
                    CompanyAvgAbsenceDays,
                    CompanyAvgTenureMonths,
                    -- Calculated Comparisons
                    CASE 
                        WHEN DepartmentAvgSalary > 0 THEN ((BaseSalary - DepartmentAvgSalary) * 100.0 / DepartmentAvgSalary)
                        ELSE 0 
                    END as SalaryVsDepartmentPercent,
                    CASE 
                        WHEN CompanyAvgSalary > 0 THEN ((BaseSalary - CompanyAvgSalary) * 100.0 / CompanyAvgSalary)
                        ELSE 0 
                    END as SalaryVsCompanyPercent
                FROM EmployeeComparisons
                ORDER BY TenantID, ClientID, DepartmentID, EmployeeID";

            var parameters = new Dictionary<string, object>
            {
                ["StartDate"] = new DateTime(2024, 1, 1),
                ["EndDate"] = new DateTime(2024, 12, 31)
            };

            var expectedResults = new List<EmployeeAnalyticsDashboardData>
            {
                new EmployeeAnalyticsDashboardData
                {
                    TenantID = "TENANT_001",
                    ClientID = "CLIENT_ABC",
                    EmployeeID = 1001,
                    EmployeeNumber = "EMP001",
                    FirstName = "John",
                    LastName = "Doe",
                    DepartmentID = 10,
                    DepartmentName = "Engineering",
                    BaseSalary = 100000m,
                    TotalAbsenceDays = 5,
                    TenureMonths = 24,
                    DepartmentAvgSalary = 85000m,
                    DepartmentAvgAbsenceDays = 4,
                    DepartmentAvgTenureMonths = 20,
                    CompanyAvgSalary = 80000m,
                    CompanyAvgAbsenceDays = 6,
                    CompanyAvgTenureMonths = 18,
                    SalaryVsDepartmentPercent = 17.65m,
                    SalaryVsCompanyPercent = 25.0m
                }
            };

            SetupMockDataReader(expectedResults);
            var queryExecutor = new SqlQueryExecutor(_mockConnection.Object, _mockLogger.Object);

            // Act
            var result = await queryExecutor.ExecuteQueryAsync<EmployeeAnalyticsDashboardData>(query, parameters);

            // Assert
            Assert.IsNotNull(result);
            Assert.AreEqual(1);
            result.First().SalaryVsDepartmentPercentAssert.AreEqual(17.65m);
            result.First().SalaryVsCompanyPercentAssert.AreEqual(25.0m);
        }

        #endregion

        #region Multi-Tenant Analytics Tests

        /// <summary>
        /// Tests analytics queries across multiple tenants
        /// </summary>
        [TestMethod]
        public async Task ExecuteAnalytics_MultipleTenants_ReturnsTenantSpecificResults()
        {
            // Arrange
            var query = @"
                SELECT 
                    TenantID,
                    ClientID,
                    COUNT(DISTINCT EmployeeID) as TotalEmployees,
                    SUM(GrossSalary) as TotalGrossSalary,
                    AVG(GrossSalary) as AverageGrossSalary
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

            var expectedResults = new List<PayrollAnalyticsSummaryData>
            {
                new PayrollAnalyticsSummaryData
                {
                    TenantID = "TENANT_001",
                    ClientID = "CLIENT_ABC",
                    TotalEmployees = 100,
                    TotalGrossSalary = 8000000m,
                    AverageBaseSalary = 80000m
                },
                new PayrollAnalyticsSummaryData
                {
                    TenantID = "TENANT_002",
                    ClientID = "CLIENT_XYZ",
                    TotalEmployees = 75,
                    TotalGrossSalary = 6000000m,
                    AverageBaseSalary = 80000m
                }
            };

            SetupMockDataReader(expectedResults);
            var queryExecutor = new SqlQueryExecutor(_mockConnection.Object, _mockLogger.Object);

            // Act
            var result = await queryExecutor.ExecuteQueryAsync<PayrollAnalyticsSummaryData>(query, parameters);

            // Assert
            Assert.IsNotNull(result);
            Assert.AreEqual(2);
            result.First().TenantIDAssert.AreEqual("TENANT_001");
            result.First().TotalEmployeesAssert.AreEqual(100);
            result.Last().TenantIDAssert.AreEqual("TENANT_002");
            result.Last().TotalEmployeesAssert.AreEqual(75);
        }

        #endregion

        #region Performance Tests

        /// <summary>
        /// Tests analytics query performance with large datasets
        /// </summary>
        [TestMethod]
        public async Task ExecuteAnalytics_LargeDataset_PerformsWithinTimeLimit()
        {
            // Arrange
            var query = @"
                SELECT 
                    TenantID,
                    ClientID,
                    DepartmentID,
                    COUNT(DISTINCT EmployeeID) as TotalEmployees,
                    SUM(GrossSalary) as TotalGrossSalary
                FROM PayrollAnalyticsData
                WHERE ExtractionDate >= @StartDate
                    AND ExtractionDate <= @EndDate
                GROUP BY 
                    TenantID,
                    ClientID,
                    DepartmentID
                ORDER BY TenantID, ClientID, DepartmentID";

            var parameters = new Dictionary<string, object>
            {
                ["StartDate"] = new DateTime(2024, 1, 1),
                ["EndDate"] = new DateTime(2024, 12, 31)
            };

            var maxExecutionTime = TimeSpan.FromSeconds(10);
            var largeDataSet = SqlQueryTestUtilities.GenerateTestCustomers(50000);
            SetupMockDataReader(largeDataSet);
            var queryExecutor = new SqlQueryExecutor(_mockConnection.Object, _mockLogger.Object);

            // Act
            var executionTime = await SqlQueryTestUtilities.MeasureExecutionTime(async () =>
            {
                await queryExecutor.ExecuteQueryAsync<PayrollAnalyticsSummaryData>(query, parameters);
            });

            // Assert
            executionTimeAssert.IsTrue(executionTime < maxExecutionTime, 
                $"Analytics query should execute within {maxExecutionTime.TotalMilliseconds}ms");
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

    public class PayrollAnalyticsSummaryData
    {
        public string TenantID { get; set; }
        public string ClientID { get; set; }
        public int DepartmentID { get; set; }
        public string DepartmentName { get; set; }
        public string BusinessUnit { get; set; }
        public string Region { get; set; }
        public string Country { get; set; }
        public int TotalEmployees { get; set; }
        public int ActiveEmployees { get; set; }
        public decimal TotalBaseSalary { get; set; }
        public decimal AverageBaseSalary { get; set; }
        public decimal TotalAllowances { get; set; }
        public decimal TotalDeductions { get; set; }
        public decimal TotalGrossSalary { get; set; }
        public decimal TotalNetSalary { get; set; }
        public int TotalAbsenceDays { get; set; }
        public decimal AverageAbsenceDays { get; set; }
        public decimal CostPerEmployee { get; set; }
        public decimal DeductionPercentage { get; set; }
    }

    public class EmployeeAnalyticsDashboardData
    {
        public string TenantID { get; set; }
        public string ClientID { get; set; }
        public int EmployeeID { get; set; }
        public string EmployeeNumber { get; set; }
        public string FirstName { get; set; }
        public string LastName { get; set; }
        public int DepartmentID { get; set; }
        public string DepartmentName { get; set; }
        public decimal BaseSalary { get; set; }
        public int TotalAbsenceDays { get; set; }
        public int TenureMonths { get; set; }
        public int SalaryRank { get; set; }
        public int SalaryRankWithTies { get; set; }
        public double SalaryPercentile { get; set; }
        public decimal DepartmentAvgSalary { get; set; }
        public decimal DepartmentAvgAbsenceDays { get; set; }
        public decimal DepartmentAvgTenureMonths { get; set; }
        public decimal CompanyAvgSalary { get; set; }
        public decimal CompanyAvgAbsenceDays { get; set; }
        public decimal CompanyAvgTenureMonths { get; set; }
        public decimal SalaryVsDepartmentPercent { get; set; }
        public decimal SalaryVsCompanyPercent { get; set; }
    }

    #endregion
}
