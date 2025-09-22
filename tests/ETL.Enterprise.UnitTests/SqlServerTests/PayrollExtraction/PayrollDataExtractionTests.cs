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
    /// Comprehensive tests for payroll data extraction from client databases
    /// Tests multi-tenant payroll data extraction scenarios
    /// </summary>
    [TestClass]
    public class PayrollDataExtractionTests
    {
        private Mock<ILogger<PayrollDataExtractionTests>> _mockLogger;
        private Mock<IDbConnection> _mockConnection;
        private Mock<IDbCommand> _mockCommand;
        private Mock<IDataReader> _mockDataReader;
        private TestDatabaseContext _testContext;

        [TestInitialize]
        public void TestInitialize()
        {
            _mockLogger = new Mock<ILogger<PayrollDataExtractionTests>>();
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

        #region Employee Start Data Extraction Tests

        /// <summary>
        /// Tests extraction of employee start data for new hires
        /// </summary>
        [TestMethod]
        public async Task ExtractEmployeeStartData_NewHires_ReturnsExpectedResults()
        {
            // Arrange
            var query = @"
                SELECT 
                    e.EmployeeID,
                    e.EmployeeNumber,
                    e.FirstName,
                    e.LastName,
                    e.Email,
                    e.StartDate,
                    e.DepartmentID,
                    d.DepartmentName,
                    e.PositionID,
                    p.PositionTitle,
                    e.SalaryGrade,
                    e.EmploymentType,
                    @TenantID as TenantID,
                    @ClientID as ClientID,
                    @ExtractionDate as ExtractionDate,
                    @BatchID as BatchID
                FROM Employees e
                LEFT JOIN Departments d ON e.DepartmentID = d.DepartmentID
                LEFT JOIN Positions p ON e.PositionID = p.PositionID
                WHERE e.StartDate >= @StartDate
                    AND e.StartDate <= @EndDate
                    AND e.IsActive = 1";

            var parameters = new Dictionary<string, object>
            {
                ["StartDate"] = new DateTime(2024, 1, 1),
                ["EndDate"] = new DateTime(2024, 12, 31),
                ["TenantID"] = "TENANT_001",
                ["ClientID"] = "CLIENT_ABC",
                ["ExtractionDate"] = DateTime.Now,
                ["BatchID"] = "BATCH_20241201_001"
            };

            var expectedResults = new List<EmployeeStartData>
            {
                new EmployeeStartData
                {
                    EmployeeID = 1001,
                    EmployeeNumber = "EMP001",
                    FirstName = "John",
                    LastName = "Doe",
                    Email = "john.doe@company.com",
                    StartDate = new DateTime(2024, 6, 15),
                    DepartmentID = 10,
                    DepartmentName = "Engineering",
                    PositionID = 101,
                    PositionTitle = "Software Engineer",
                    SalaryGrade = "SE1",
                    EmploymentType = "Full-Time",
                    TenantID = "TENANT_001",
                    ClientID = "CLIENT_ABC"
                }
            };

            SetupMockDataReader(expectedResults);
            var queryExecutor = new SqlQueryExecutor(_mockConnection.Object, _mockLogger.Object);

            // Act
            var result = await queryExecutor.ExecuteQueryAsync<EmployeeStartData>(query, parameters);

            // Assert
            Assert.IsNotNull(result);
            Assert.AreEqual(1);
            result.First().EmployeeIDAssert.AreEqual(1001);
            result.First().TenantIDAssert.AreEqual("TENANT_001");
            result.First().ClientIDAssert.AreEqual("CLIENT_ABC");
        }

        /// <summary>
        /// Tests extraction of employee start data with department filtering
        /// </summary>
        [TestMethod]
        public async Task ExtractEmployeeStartData_WithDepartmentFilter_ReturnsFilteredResults()
        {
            // Arrange
            var query = @"
                SELECT 
                    e.EmployeeID,
                    e.EmployeeNumber,
                    e.FirstName,
                    e.LastName,
                    e.StartDate,
                    e.DepartmentID,
                    d.DepartmentName,
                    @TenantID as TenantID,
                    @ClientID as ClientID
                FROM Employees e
                LEFT JOIN Departments d ON e.DepartmentID = d.DepartmentID
                WHERE e.StartDate >= @StartDate
                    AND e.StartDate <= @EndDate
                    AND e.IsActive = 1
                    AND e.DepartmentID = @DepartmentID";

            var parameters = new Dictionary<string, object>
            {
                ["StartDate"] = new DateTime(2024, 1, 1),
                ["EndDate"] = new DateTime(2024, 12, 31),
                ["DepartmentID"] = 10,
                ["TenantID"] = "TENANT_001",
                ["ClientID"] = "CLIENT_ABC"
            };

            var expectedResults = new List<EmployeeStartData>
            {
                new EmployeeStartData
                {
                    EmployeeID = 1001,
                    EmployeeNumber = "EMP001",
                    FirstName = "John",
                    LastName = "Doe",
                    StartDate = new DateTime(2024, 6, 15),
                    DepartmentID = 10,
                    DepartmentName = "Engineering",
                    TenantID = "TENANT_001",
                    ClientID = "CLIENT_ABC"
                },
                new EmployeeStartData
                {
                    EmployeeID = 1002,
                    EmployeeNumber = "EMP002",
                    FirstName = "Jane",
                    LastName = "Smith",
                    StartDate = new DateTime(2024, 7, 1),
                    DepartmentID = 10,
                    DepartmentName = "Engineering",
                    TenantID = "TENANT_001",
                    ClientID = "CLIENT_ABC"
                }
            };

            SetupMockDataReader(expectedResults);
            var queryExecutor = new SqlQueryExecutor(_mockConnection.Object, _mockLogger.Object);

            // Act
            var result = await queryExecutor.ExecuteQueryAsync<EmployeeStartData>(query, parameters);

            // Assert
            Assert.IsNotNull(result);
            Assert.AreEqual(2);
            result.All(r => r.DepartmentID == 10), "All items should match condition");
            result.All(r => r.TenantID == "TENANT_001"), "All items should match condition");
        }

        #endregion

        #region Employee Exit Data Extraction Tests

        /// <summary>
        /// Tests extraction of employee exit data for terminated employees
        /// </summary>
        [TestMethod]
        public async Task ExtractEmployeeExitData_TerminatedEmployees_ReturnsExpectedResults()
        {
            // Arrange
            var query = @"
                SELECT 
                    e.EmployeeID,
                    e.EmployeeNumber,
                    e.FirstName,
                    e.LastName,
                    e.StartDate,
                    e.EndDate,
                    e.TerminationDate,
                    e.TerminationReason,
                    e.TerminationType,
                    e.DepartmentID,
                    d.DepartmentName,
                    @TenantID as TenantID,
                    @ClientID as ClientID
                FROM Employees e
                LEFT JOIN Departments d ON e.DepartmentID = d.DepartmentID
                WHERE e.EndDate >= @StartDate
                    AND e.EndDate <= @EndDate
                    AND e.IsActive = 0";

            var parameters = new Dictionary<string, object>
            {
                ["StartDate"] = new DateTime(2024, 1, 1),
                ["EndDate"] = new DateTime(2024, 12, 31),
                ["TenantID"] = "TENANT_001",
                ["ClientID"] = "CLIENT_ABC"
            };

            var expectedResults = new List<EmployeeExitData>
            {
                new EmployeeExitData
                {
                    EmployeeID = 2001,
                    EmployeeNumber = "EMP201",
                    FirstName = "Bob",
                    LastName = "Johnson",
                    StartDate = new DateTime(2020, 3, 1),
                    EndDate = new DateTime(2024, 8, 15),
                    TerminationDate = new DateTime(2024, 8, 15),
                    TerminationReason = "Resignation",
                    TerminationType = "Voluntary",
                    DepartmentID = 20,
                    DepartmentName = "Sales",
                    TenantID = "TENANT_001",
                    ClientID = "CLIENT_ABC"
                }
            };

            SetupMockDataReader(expectedResults);
            var queryExecutor = new SqlQueryExecutor(_mockConnection.Object, _mockLogger.Object);

            // Act
            var result = await queryExecutor.ExecuteQueryAsync<EmployeeExitData>(query, parameters);

            // Assert
            Assert.IsNotNull(result);
            Assert.AreEqual(1);
            result.First().EmployeeIDAssert.AreEqual(2001);
            result.First().TerminationReasonAssert.AreEqual("Resignation");
            result.First().TenantIDAssert.AreEqual("TENANT_001");
        }

        #endregion

        #region Employee Compensation Data Extraction Tests

        /// <summary>
        /// Tests extraction of employee compensation data
        /// </summary>
        [TestMethod]
        public async Task ExtractEmployeeCompensationData_CurrentCompensation_ReturnsExpectedResults()
        {
            // Arrange
            var query = @"
                SELECT 
                    e.EmployeeID,
                    e.EmployeeNumber,
                    e.FirstName,
                    e.LastName,
                    c.BaseSalary,
                    c.AnnualSalary,
                    c.Currency,
                    c.TotalAllowances,
                    c.TotalDeductions,
                    c.GrossSalary,
                    c.NetSalary,
                    c.EffectiveDate,
                    @TenantID as TenantID,
                    @ClientID as ClientID
                FROM Employees e
                INNER JOIN EmployeeCompensation c ON e.EmployeeID = c.EmployeeID
                WHERE c.EffectiveDate <= @ExtractionDate
                    AND (c.EndDate IS NULL OR c.EndDate >= @ExtractionDate)
                    AND e.IsActive = 1";

            var parameters = new Dictionary<string, object>
            {
                ["ExtractionDate"] = DateTime.Now,
                ["TenantID"] = "TENANT_001",
                ["ClientID"] = "CLIENT_ABC"
            };

            var expectedResults = new List<EmployeeCompensationData>
            {
                new EmployeeCompensationData
                {
                    EmployeeID = 1001,
                    EmployeeNumber = "EMP001",
                    FirstName = "John",
                    LastName = "Doe",
                    BaseSalary = 75000m,
                    AnnualSalary = 75000m,
                    Currency = "USD",
                    TotalAllowances = 5000m,
                    TotalDeductions = 15000m,
                    GrossSalary = 80000m,
                    NetSalary = 65000m,
                    EffectiveDate = new DateTime(2024, 1, 1),
                    TenantID = "TENANT_001",
                    ClientID = "CLIENT_ABC"
                }
            };

            SetupMockDataReader(expectedResults);
            var queryExecutor = new SqlQueryExecutor(_mockConnection.Object, _mockLogger.Object);

            // Act
            var result = await queryExecutor.ExecuteQueryAsync<EmployeeCompensationData>(query, parameters);

            // Assert
            Assert.IsNotNull(result);
            Assert.AreEqual(1);
            result.First().EmployeeIDAssert.AreEqual(1001);
            result.First().BaseSalaryAssert.AreEqual(75000m);
            result.First().NetSalaryAssert.AreEqual(65000m);
        }

        #endregion

        #region Employee PII Data Extraction Tests

        /// <summary>
        /// Tests extraction of employee PII data with security considerations
        /// </summary>
        [TestMethod]
        public async Task ExtractEmployeePIIData_WithSecurity_ReturnsExpectedResults()
        {
            // Arrange
            var query = @"
                SELECT 
                    e.EmployeeID,
                    e.EmployeeNumber,
                    e.FirstName,
                    e.LastName,
                    e.Email,
                    e.Phone,
                    e.DateOfBirth,
                    e.SocialSecurityNumber,
                    e.TaxID,
                    a.AddressLine1,
                    a.City,
                    a.State,
                    a.PostalCode,
                    a.Country,
                    b.BankName,
                    b.BankAccountNumber,
                    @TenantID as TenantID,
                    @ClientID as ClientID
                FROM Employees e
                LEFT JOIN EmployeeAddresses a ON e.EmployeeID = a.EmployeeID AND a.IsPrimary = 1
                LEFT JOIN EmployeeBankAccounts b ON e.EmployeeID = b.EmployeeID AND b.IsPrimary = 1
                WHERE e.IsActive = 1";

            var parameters = new Dictionary<string, object>
            {
                ["TenantID"] = "TENANT_001",
                ["ClientID"] = "CLIENT_ABC"
            };

            var expectedResults = new List<EmployeePIIData>
            {
                new EmployeePIIData
                {
                    EmployeeID = 1001,
                    EmployeeNumber = "EMP001",
                    FirstName = "John",
                    LastName = "Doe",
                    Email = "john.doe@company.com",
                    Phone = "555-1234",
                    DateOfBirth = new DateTime(1990, 5, 15),
                    SocialSecurityNumber = "123-45-6789",
                    TaxID = "TAX123456",
                    AddressLine1 = "123 Main St",
                    City = "New York",
                    State = "NY",
                    PostalCode = "10001",
                    Country = "USA",
                    BankName = "First National Bank",
                    BankAccountNumber = "1234567890",
                    TenantID = "TENANT_001",
                    ClientID = "CLIENT_ABC"
                }
            };

            SetupMockDataReader(expectedResults);
            var queryExecutor = new SqlQueryExecutor(_mockConnection.Object, _mockLogger.Object);

            // Act
            var result = await queryExecutor.ExecuteQueryAsync<EmployeePIIData>(query, parameters);

            // Assert
            Assert.IsNotNull(result);
            Assert.AreEqual(1);
            result.First().EmployeeIDAssert.AreEqual(1001);
            result.First().SocialSecurityNumberAssert.AreEqual("123-45-6789");
            result.First().BankAccountNumberAssert.AreEqual("1234567890");
        }

        #endregion

        #region Organizational Hierarchy Data Extraction Tests

        /// <summary>
        /// Tests extraction of organizational hierarchy data using CTE
        /// </summary>
        [TestMethod]
        public async Task ExtractOrganizationalHierarchyData_WithCTE_ReturnsExpectedResults()
        {
            // Arrange
            var query = @"
                WITH OrganizationalHierarchy AS (
                    SELECT 
                        e.EmployeeID,
                        e.EmployeeNumber,
                        e.FirstName,
                        e.LastName,
                        e.DepartmentID,
                        d.DepartmentName,
                        e.ManagerID,
                        e.HierarchyLevel,
                        @TenantID as TenantID,
                        @ClientID as ClientID
                    FROM Employees e
                    LEFT JOIN Departments d ON e.DepartmentID = d.DepartmentID
                    WHERE e.IsActive = 1
                )
                SELECT 
                    EmployeeID,
                    EmployeeNumber,
                    FirstName,
                    LastName,
                    DepartmentID,
                    DepartmentName,
                    ManagerID,
                    HierarchyLevel,
                    TenantID,
                    ClientID
                FROM OrganizationalHierarchy
                ORDER BY HierarchyLevel, DepartmentName, LastName";

            var parameters = new Dictionary<string, object>
            {
                ["TenantID"] = "TENANT_001",
                ["ClientID"] = "CLIENT_ABC"
            };

            var expectedResults = new List<OrganizationalHierarchyData>
            {
                new OrganizationalHierarchyData
                {
                    EmployeeID = 1001,
                    EmployeeNumber = "EMP001",
                    FirstName = "John",
                    LastName = "Doe",
                    DepartmentID = 10,
                    DepartmentName = "Engineering",
                    ManagerID = 1000,
                    HierarchyLevel = 2,
                    TenantID = "TENANT_001",
                    ClientID = "CLIENT_ABC"
                }
            };

            SetupMockDataReader(expectedResults);
            var queryExecutor = new SqlQueryExecutor(_mockConnection.Object, _mockLogger.Object);

            // Act
            var result = await queryExecutor.ExecuteQueryAsync<OrganizationalHierarchyData>(query, parameters);

            // Assert
            Assert.IsNotNull(result);
            Assert.AreEqual(1);
            result.First().EmployeeIDAssert.AreEqual(1001);
            result.First().HierarchyLevelAssert.AreEqual(2);
        }

        #endregion

        #region Employee Absence Data Extraction Tests

        /// <summary>
        /// Tests extraction of employee absence data
        /// </summary>
        [TestMethod]
        public async Task ExtractEmployeeAbsenceData_WithFilters_ReturnsExpectedResults()
        {
            // Arrange
            var query = @"
                SELECT 
                    a.AbsenceID,
                    a.EmployeeID,
                    e.EmployeeNumber,
                    e.FirstName,
                    e.LastName,
                    a.AbsenceType,
                    a.StartDate,
                    a.EndDate,
                    a.TotalDays,
                    a.IsApproved,
                    a.IsPaid,
                    @TenantID as TenantID,
                    @ClientID as ClientID
                FROM EmployeeAbsences a
                INNER JOIN Employees e ON a.EmployeeID = e.EmployeeID
                WHERE a.StartDate >= @StartDate
                    AND a.StartDate <= @EndDate
                    AND e.IsActive = 1";

            var parameters = new Dictionary<string, object>
            {
                ["StartDate"] = new DateTime(2024, 1, 1),
                ["EndDate"] = new DateTime(2024, 12, 31),
                ["TenantID"] = "TENANT_001",
                ["ClientID"] = "CLIENT_ABC"
            };

            var expectedResults = new List<EmployeeAbsenceData>
            {
                new EmployeeAbsenceData
                {
                    AbsenceID = 5001,
                    EmployeeID = 1001,
                    EmployeeNumber = "EMP001",
                    FirstName = "John",
                    LastName = "Doe",
                    AbsenceType = "Sick Leave",
                    StartDate = new DateTime(2024, 6, 10),
                    EndDate = new DateTime(2024, 6, 12),
                    TotalDays = 3,
                    IsApproved = true,
                    IsPaid = true,
                    TenantID = "TENANT_001",
                    ClientID = "CLIENT_ABC"
                }
            };

            SetupMockDataReader(expectedResults);
            var queryExecutor = new SqlQueryExecutor(_mockConnection.Object, _mockLogger.Object);

            // Act
            var result = await queryExecutor.ExecuteQueryAsync<EmployeeAbsenceData>(query, parameters);

            // Assert
            Assert.IsNotNull(result);
            Assert.AreEqual(1);
            result.First().AbsenceIDAssert.AreEqual(5001);
            result.First().AbsenceTypeAssert.AreEqual("Sick Leave");
            result.First().TotalDaysAssert.AreEqual(3);
        }

        #endregion

        #region Dynamic Security Data Extraction Tests

        /// <summary>
        /// Tests extraction of dynamic security data
        /// </summary>
        [TestMethod]
        public async Task ExtractDynamicSecurityData_WithAccessControls_ReturnsExpectedResults()
        {
            // Arrange
            var query = @"
                SELECT 
                    s.SecurityID,
                    s.EmployeeID,
                    e.EmployeeNumber,
                    e.FirstName,
                    e.LastName,
                    s.SecurityLevel,
                    s.AccessLevel,
                    s.CanViewPayroll,
                    s.CanViewPII,
                    s.IsActive,
                    @TenantID as TenantID,
                    @ClientID as ClientID
                FROM EmployeeSecurity s
                INNER JOIN Employees e ON s.EmployeeID = e.EmployeeID
                WHERE s.IsActive = 1
                    AND e.IsActive = 1";

            var parameters = new Dictionary<string, object>
            {
                ["TenantID"] = "TENANT_001",
                ["ClientID"] = "CLIENT_ABC"
            };

            var expectedResults = new List<DynamicSecurityData>
            {
                new DynamicSecurityData
                {
                    SecurityID = 3001,
                    EmployeeID = 1001,
                    EmployeeNumber = "EMP001",
                    FirstName = "John",
                    LastName = "Doe",
                    SecurityLevel = "High",
                    AccessLevel = "Manager",
                    CanViewPayroll = true,
                    CanViewPII = true,
                    IsActive = true,
                    TenantID = "TENANT_001",
                    ClientID = "CLIENT_ABC"
                }
            };

            SetupMockDataReader(expectedResults);
            var queryExecutor = new SqlQueryExecutor(_mockConnection.Object, _mockLogger.Object);

            // Act
            var result = await queryExecutor.ExecuteQueryAsync<DynamicSecurityData>(query, parameters);

            // Assert
            Assert.IsNotNull(result);
            Assert.AreEqual(1);
            result.First().SecurityIDAssert.AreEqual(3001);
            result.First().CanViewPayrollAssert.IsTrue();
            result.First().CanViewPIIAssert.IsTrue();
        }

        #endregion

        #region Multi-Tenant Data Extraction Tests

        /// <summary>
        /// Tests extraction of data from multiple tenants
        /// </summary>
        [TestMethod]
        public async Task ExtractData_MultipleTenants_ReturnsTenantSpecificResults()
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
                WHERE e.IsActive = 1";

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

            var tenant1Results = new List<EmployeeStartData>
            {
                new EmployeeStartData
                {
                    EmployeeID = 1001,
                    EmployeeNumber = "EMP001",
                    FirstName = "John",
                    LastName = "Doe",
                    TenantID = "TENANT_001",
                    ClientID = "CLIENT_ABC"
                }
            };

            var tenant2Results = new List<EmployeeStartData>
            {
                new EmployeeStartData
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
            var tenant1Result = await queryExecutor.ExecuteQueryAsync<EmployeeStartData>(query, tenant1Parameters);

            // Act - Extract from Tenant 2
            SetupMockDataReader(tenant2Results);
            var tenant2Result = await queryExecutor.ExecuteQueryAsync<EmployeeStartData>(query, tenant2Parameters);

            // Assert
            tenant1ResultAssert.IsNotNull();
            tenant1ResultAssert.AreEqual(1);
            tenant1Result.First().TenantIDAssert.AreEqual("TENANT_001");
            tenant1Result.First().ClientIDAssert.AreEqual("CLIENT_ABC");

            tenant2ResultAssert.IsNotNull();
            tenant2ResultAssert.AreEqual(1);
            tenant2Result.First().TenantIDAssert.AreEqual("TENANT_002");
            tenant2Result.First().ClientIDAssert.AreEqual("CLIENT_XYZ");
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

    public class EmployeeStartData
    {
        public int EmployeeID { get; set; }
        public string EmployeeNumber { get; set; }
        public string FirstName { get; set; }
        public string LastName { get; set; }
        public string Email { get; set; }
        public DateTime StartDate { get; set; }
        public int DepartmentID { get; set; }
        public string DepartmentName { get; set; }
        public int PositionID { get; set; }
        public string PositionTitle { get; set; }
        public string SalaryGrade { get; set; }
        public string EmploymentType { get; set; }
        public string TenantID { get; set; }
        public string ClientID { get; set; }
    }

    public class EmployeeExitData
    {
        public int EmployeeID { get; set; }
        public string EmployeeNumber { get; set; }
        public string FirstName { get; set; }
        public string LastName { get; set; }
        public DateTime StartDate { get; set; }
        public DateTime EndDate { get; set; }
        public DateTime TerminationDate { get; set; }
        public string TerminationReason { get; set; }
        public string TerminationType { get; set; }
        public int DepartmentID { get; set; }
        public string DepartmentName { get; set; }
        public string TenantID { get; set; }
        public string ClientID { get; set; }
    }

    public class EmployeeCompensationData
    {
        public int EmployeeID { get; set; }
        public string EmployeeNumber { get; set; }
        public string FirstName { get; set; }
        public string LastName { get; set; }
        public decimal BaseSalary { get; set; }
        public decimal AnnualSalary { get; set; }
        public string Currency { get; set; }
        public decimal TotalAllowances { get; set; }
        public decimal TotalDeductions { get; set; }
        public decimal GrossSalary { get; set; }
        public decimal NetSalary { get; set; }
        public DateTime EffectiveDate { get; set; }
        public string TenantID { get; set; }
        public string ClientID { get; set; }
    }

    public class EmployeePIIData
    {
        public int EmployeeID { get; set; }
        public string EmployeeNumber { get; set; }
        public string FirstName { get; set; }
        public string LastName { get; set; }
        public string Email { get; set; }
        public string Phone { get; set; }
        public DateTime DateOfBirth { get; set; }
        public string SocialSecurityNumber { get; set; }
        public string TaxID { get; set; }
        public string AddressLine1 { get; set; }
        public string City { get; set; }
        public string State { get; set; }
        public string PostalCode { get; set; }
        public string Country { get; set; }
        public string BankName { get; set; }
        public string BankAccountNumber { get; set; }
        public string TenantID { get; set; }
        public string ClientID { get; set; }
    }

    public class OrganizationalHierarchyData
    {
        public int EmployeeID { get; set; }
        public string EmployeeNumber { get; set; }
        public string FirstName { get; set; }
        public string LastName { get; set; }
        public int DepartmentID { get; set; }
        public string DepartmentName { get; set; }
        public int? ManagerID { get; set; }
        public int HierarchyLevel { get; set; }
        public string TenantID { get; set; }
        public string ClientID { get; set; }
    }

    public class EmployeeAbsenceData
    {
        public int AbsenceID { get; set; }
        public int EmployeeID { get; set; }
        public string EmployeeNumber { get; set; }
        public string FirstName { get; set; }
        public string LastName { get; set; }
        public string AbsenceType { get; set; }
        public DateTime StartDate { get; set; }
        public DateTime EndDate { get; set; }
        public int TotalDays { get; set; }
        public bool IsApproved { get; set; }
        public bool IsPaid { get; set; }
        public string TenantID { get; set; }
        public string ClientID { get; set; }
    }

    public class DynamicSecurityData
    {
        public int SecurityID { get; set; }
        public int EmployeeID { get; set; }
        public string EmployeeNumber { get; set; }
        public string FirstName { get; set; }
        public string LastName { get; set; }
        public string SecurityLevel { get; set; }
        public string AccessLevel { get; set; }
        public bool CanViewPayroll { get; set; }
        public bool CanViewPII { get; set; }
        public bool IsActive { get; set; }
        public string TenantID { get; set; }
        public string ClientID { get; set; }
    }

    #endregion
}
