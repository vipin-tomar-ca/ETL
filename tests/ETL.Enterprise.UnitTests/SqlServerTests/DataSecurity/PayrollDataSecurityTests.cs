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
    /// Comprehensive tests for payroll data security and access control
    /// Tests data encryption, access restrictions, and security compliance
    /// </summary>
    [TestClass]
    public class PayrollDataSecurityTests
    {
        private Mock<ILogger<PayrollDataSecurityTests>> _mockLogger;
        private Mock<IDbConnection> _mockConnection;
        private Mock<IDbCommand> _mockCommand;
        private Mock<IDataReader> _mockDataReader;
        private TestDatabaseContext _testContext;

        [TestInitialize]
        public void TestInitialize()
        {
            _mockLogger = new Mock<ILogger<PayrollDataSecurityTests>>();
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

        #region Data Encryption Tests

        /// <summary>
        /// Tests extraction of encrypted PII data
        /// </summary>
        [TestMethod]
        public async Task ExtractEncryptedPIIData_WithEncryption_ReturnsEncryptedResults()
        {
            // Arrange
            var query = @"
                SELECT 
                    e.EmployeeID,
                    e.EmployeeNumber,
                    e.FirstName,
                    e.LastName,
                    -- Encrypted PII fields
                    ENCRYPTBYKEY(KEY_GUID('PayrollPIIKey'), e.SocialSecurityNumber) as EncryptedSSN,
                    ENCRYPTBYKEY(KEY_GUID('PayrollPIIKey'), e.TaxID) as EncryptedTaxID,
                    ENCRYPTBYKEY(KEY_GUID('PayrollPIIKey'), e.PassportNumber) as EncryptedPassport,
                    ENCRYPTBYKEY(KEY_GUID('PayrollPIIKey'), b.BankAccountNumber) as EncryptedBankAccount,
                    -- Non-encrypted fields
                    e.Email,
                    e.Phone,
                    e.DateOfBirth,
                    @TenantID as TenantID,
                    @ClientID as ClientID
                FROM Employees e
                LEFT JOIN EmployeeBankAccounts b ON e.EmployeeID = b.EmployeeID AND b.IsPrimary = 1
                WHERE e.IsActive = 1";

            var parameters = new Dictionary<string, object>
            {
                ["TenantID"] = "TENANT_001",
                ["ClientID"] = "CLIENT_ABC"
            };

            var expectedResults = new List<EncryptedPIIData>
            {
                new EncryptedPIIData
                {
                    EmployeeID = 1001,
                    EmployeeNumber = "EMP001",
                    FirstName = "John",
                    LastName = "Doe",
                    EncryptedSSN = new byte[] { 0x01, 0x02, 0x03, 0x04 },
                    EncryptedTaxID = new byte[] { 0x05, 0x06, 0x07, 0x08 },
                    EncryptedPassport = new byte[] { 0x09, 0x0A, 0x0B, 0x0C },
                    EncryptedBankAccount = new byte[] { 0x0D, 0x0E, 0x0F, 0x10 },
                    Email = "john.doe@company.com",
                    Phone = "555-1234",
                    DateOfBirth = new DateTime(1990, 5, 15),
                    TenantID = "TENANT_001",
                    ClientID = "CLIENT_ABC"
                }
            };

            SetupMockDataReader(expectedResults);
            var queryExecutor = new SqlQueryExecutor(_mockConnection.Object, _mockLogger.Object);

            // Act
            var result = await queryExecutor.ExecuteQueryAsync<EncryptedPIIData>(query, parameters);

            // Assert
            Assert.IsNotNull(result);
            Assert.AreEqual(1);
            result.First().EmployeeIDAssert.AreEqual(1001);
            result.First().EncryptedSSNAssert.IsNotNull();
            result.First().EncryptedTaxIDAssert.IsNotNull();
            result.First().EncryptedPassportAssert.IsNotNull();
            result.First().EncryptedBankAccountAssert.IsNotNull();
        }

        /// <summary>
        /// Tests decryption of PII data for authorized users
        /// </summary>
        [TestMethod]
        public async Task DecryptPIIData_AuthorizedUser_ReturnsDecryptedResults()
        {
            // Arrange
            var query = @"
                SELECT 
                    e.EmployeeID,
                    e.EmployeeNumber,
                    e.FirstName,
                    e.LastName,
                    -- Decrypted PII fields (only for authorized users)
                    DECRYPTBYKEY(e.EncryptedSSN) as DecryptedSSN,
                    DECRYPTBYKEY(e.EncryptedTaxID) as DecryptedTaxID,
                    DECRYPTBYKEY(e.EncryptedPassport) as DecryptedPassport,
                    DECRYPTBYKEY(e.EncryptedBankAccount) as DecryptedBankAccount,
                    @TenantID as TenantID,
                    @ClientID as ClientID
                FROM Employees e
                WHERE e.IsActive = 1
                    AND @UserID IN (SELECT UserID FROM AuthorizedUsers WHERE CanViewPII = 1)";

            var parameters = new Dictionary<string, object>
            {
                ["UserID"] = "USER_001",
                ["TenantID"] = "TENANT_001",
                ["ClientID"] = "CLIENT_ABC"
            };

            var expectedResults = new List<DecryptedPIIData>
            {
                new DecryptedPIIData
                {
                    EmployeeID = 1001,
                    EmployeeNumber = "EMP001",
                    FirstName = "John",
                    LastName = "Doe",
                    DecryptedSSN = "123-45-6789",
                    DecryptedTaxID = "TAX123456",
                    DecryptedPassport = "PASS123456",
                    DecryptedBankAccount = "1234567890",
                    TenantID = "TENANT_001",
                    ClientID = "CLIENT_ABC"
                }
            };

            SetupMockDataReader(expectedResults);
            var queryExecutor = new SqlQueryExecutor(_mockConnection.Object, _mockLogger.Object);

            // Act
            var result = await queryExecutor.ExecuteQueryAsync<DecryptedPIIData>(query, parameters);

            // Assert
            Assert.IsNotNull(result);
            Assert.AreEqual(1);
            result.First().EmployeeIDAssert.AreEqual(1001);
            result.First().DecryptedSSNAssert.AreEqual("123-45-6789");
            result.First().DecryptedTaxIDAssert.AreEqual("TAX123456");
            result.First().DecryptedPassportAssert.AreEqual("PASS123456");
            result.First().DecryptedBankAccountAssert.AreEqual("1234567890");
        }

        #endregion

        #region Access Control Tests

        /// <summary>
        /// Tests access control for payroll data based on user permissions
        /// </summary>
        [TestMethod]
        public async Task ExtractPayrollData_WithAccessControl_ReturnsAuthorizedData()
        {
            // Arrange
            var query = @"
                SELECT 
                    e.EmployeeID,
                    e.EmployeeNumber,
                    e.FirstName,
                    e.LastName,
                    c.BaseSalary,
                    c.GrossSalary,
                    c.NetSalary,
                    @TenantID as TenantID,
                    @ClientID as ClientID
                FROM Employees e
                INNER JOIN EmployeeCompensation c ON e.EmployeeID = c.EmployeeID
                WHERE e.IsActive = 1
                    AND (@UserID IN (SELECT UserID FROM AuthorizedUsers WHERE CanViewPayroll = 1)
                         OR e.EmployeeID IN (SELECT EmployeeID FROM EmployeeManagers WHERE ManagerID = @UserID))";

            var parameters = new Dictionary<string, object>
            {
                ["UserID"] = "USER_001",
                ["TenantID"] = "TENANT_001",
                ["ClientID"] = "CLIENT_ABC"
            };

            var expectedResults = new List<PayrollAccessData>
            {
                new PayrollAccessData
                {
                    EmployeeID = 1001,
                    EmployeeNumber = "EMP001",
                    FirstName = "John",
                    LastName = "Doe",
                    BaseSalary = 75000m,
                    GrossSalary = 80000m,
                    NetSalary = 65000m,
                    TenantID = "TENANT_001",
                    ClientID = "CLIENT_ABC"
                }
            };

            SetupMockDataReader(expectedResults);
            var queryExecutor = new SqlQueryExecutor(_mockConnection.Object, _mockLogger.Object);

            // Act
            var result = await queryExecutor.ExecuteQueryAsync<PayrollAccessData>(query, parameters);

            // Assert
            Assert.IsNotNull(result);
            Assert.AreEqual(1);
            result.First().EmployeeIDAssert.AreEqual(1001);
            result.First().BaseSalaryAssert.AreEqual(75000m);
        }

        /// <summary>
        /// Tests access control for unauthorized users
        /// </summary>
        [TestMethod]
        public async Task ExtractPayrollData_UnauthorizedUser_ReturnsEmptyResults()
        {
            // Arrange
            var query = @"
                SELECT 
                    e.EmployeeID,
                    e.EmployeeNumber,
                    e.FirstName,
                    e.LastName,
                    c.BaseSalary,
                    c.GrossSalary,
                    c.NetSalary
                FROM Employees e
                INNER JOIN EmployeeCompensation c ON e.EmployeeID = c.EmployeeID
                WHERE e.IsActive = 1
                    AND @UserID IN (SELECT UserID FROM AuthorizedUsers WHERE CanViewPayroll = 1)";

            var parameters = new Dictionary<string, object>
            {
                ["UserID"] = "UNAUTHORIZED_USER"
            };

            SetupMockDataReader(new List<PayrollAccessData>());
            var queryExecutor = new SqlQueryExecutor(_mockConnection.Object, _mockLogger.Object);

            // Act
            var result = await queryExecutor.ExecuteQueryAsync<PayrollAccessData>(query, parameters);

            // Assert
            Assert.IsNotNull(result);
            Assert.AreEqual(0, result.Count, );
        }

        #endregion

        #region Data Masking Tests

        /// <summary>
        /// Tests data masking for sensitive information
        /// </summary>
        [TestMethod]
        public async Task ExtractData_WithDataMasking_ReturnsMaskedResults()
        {
            // Arrange
            var query = @"
                SELECT 
                    e.EmployeeID,
                    e.EmployeeNumber,
                    e.FirstName,
                    e.LastName,
                    -- Masked sensitive data
                    CASE 
                        WHEN @UserID IN (SELECT UserID FROM AuthorizedUsers WHERE CanViewPII = 1)
                        THEN e.SocialSecurityNumber
                        ELSE 'XXX-XX-' + RIGHT(e.SocialSecurityNumber, 4)
                    END as MaskedSSN,
                    CASE 
                        WHEN @UserID IN (SELECT UserID FROM AuthorizedUsers WHERE CanViewPII = 1)
                        THEN e.Email
                        ELSE LEFT(e.Email, 3) + '***@' + RIGHT(e.Email, CHARINDEX('@', REVERSE(e.Email)) - 1)
                    END as MaskedEmail,
                    CASE 
                        WHEN @UserID IN (SELECT UserID FROM AuthorizedUsers WHERE CanViewPII = 1)
                        THEN b.BankAccountNumber
                        ELSE '****' + RIGHT(b.BankAccountNumber, 4)
                    END as MaskedBankAccount,
                    @TenantID as TenantID,
                    @ClientID as ClientID
                FROM Employees e
                LEFT JOIN EmployeeBankAccounts b ON e.EmployeeID = b.EmployeeID AND b.IsPrimary = 1
                WHERE e.IsActive = 1";

            var parameters = new Dictionary<string, object>
            {
                ["UserID"] = "USER_002", // User without PII access
                ["TenantID"] = "TENANT_001",
                ["ClientID"] = "CLIENT_ABC"
            };

            var expectedResults = new List<MaskedData>
            {
                new MaskedData
                {
                    EmployeeID = 1001,
                    EmployeeNumber = "EMP001",
                    FirstName = "John",
                    LastName = "Doe",
                    MaskedSSN = "XXX-XX-6789",
                    MaskedEmail = "joh***@company.com",
                    MaskedBankAccount = "****7890",
                    TenantID = "TENANT_001",
                    ClientID = "CLIENT_ABC"
                }
            };

            SetupMockDataReader(expectedResults);
            var queryExecutor = new SqlQueryExecutor(_mockConnection.Object, _mockLogger.Object);

            // Act
            var result = await queryExecutor.ExecuteQueryAsync<MaskedData>(query, parameters);

            // Assert
            Assert.IsNotNull(result);
            Assert.AreEqual(1);
            result.First().EmployeeIDAssert.AreEqual(1001);
            result.First().MaskedSSNAssert.AreEqual("XXX-XX-6789");
            result.First().MaskedEmailAssert.AreEqual("joh***@company.com");
            result.First().MaskedBankAccountAssert.AreEqual("****7890");
        }

        #endregion

        #region Audit Trail Tests

        /// <summary>
        /// Tests audit trail for data access
        /// </summary>
        [TestMethod]
        public async Task ExtractData_WithAuditTrail_LogsAccess()
        {
            // Arrange
            var query = @"
                -- Insert audit trail
                INSERT INTO DataAccessAudit (UserID, TableName, AccessType, AccessDate, TenantID, ClientID)
                VALUES (@UserID, 'Employees', 'SELECT', GETDATE(), @TenantID, @ClientID);
                
                -- Extract data
                SELECT 
                    e.EmployeeID,
                    e.EmployeeNumber,
                    e.FirstName,
                    e.LastName,
                    @TenantID as TenantID,
                    @ClientID as ClientID
                FROM Employees e
                WHERE e.IsActive = 1";

            var parameters = new Dictionary<string, object>
            {
                ["UserID"] = "USER_001",
                ["TenantID"] = "TENANT_001",
                ["ClientID"] = "CLIENT_ABC"
            };

            var expectedResults = new List<AuditTrailData>
            {
                new AuditTrailData
                {
                    EmployeeID = 1001,
                    EmployeeNumber = "EMP001",
                    FirstName = "John",
                    LastName = "Doe",
                    TenantID = "TENANT_001",
                    ClientID = "CLIENT_ABC"
                }
            };

            SetupMockDataReader(expectedResults);
            _mockCommand.Setup(cmd => cmd.ExecuteNonQuery()).Returns(1);
            var queryExecutor = new SqlQueryExecutor(_mockConnection.Object, _mockLogger.Object);

            // Act
            var result = await queryExecutor.ExecuteQueryAsync<AuditTrailData>(query, parameters);

            // Assert
            Assert.IsNotNull(result);
            Assert.AreEqual(1);
            result.First().EmployeeIDAssert.AreEqual(1001);
        }

        #endregion

        #region Compliance Tests

        /// <summary>
        /// Tests GDPR compliance for data extraction
        /// </summary>
        [TestMethod]
        public async Task ExtractData_GDPRCompliance_ReturnsCompliantResults()
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
                    e.ConsentGiven,
                    e.ConsentDate,
                    e.DataRetentionDate,
                    @TenantID as TenantID,
                    @ClientID as ClientID
                FROM Employees e
                WHERE e.IsActive = 1
                    AND e.ConsentGiven = 1
                    AND (e.DataRetentionDate IS NULL OR e.DataRetentionDate > GETDATE())";

            var parameters = new Dictionary<string, object>
            {
                ["TenantID"] = "TENANT_001",
                ["ClientID"] = "CLIENT_ABC"
            };

            var expectedResults = new List<GDPRCompliantData>
            {
                new GDPRCompliantData
                {
                    EmployeeID = 1001,
                    EmployeeNumber = "EMP001",
                    FirstName = "John",
                    LastName = "Doe",
                    Email = "john.doe@company.com",
                    Phone = "555-1234",
                    DateOfBirth = new DateTime(1990, 5, 15),
                    ConsentGiven = true,
                    ConsentDate = new DateTime(2024, 1, 1),
                    DataRetentionDate = new DateTime(2025, 12, 31),
                    TenantID = "TENANT_001",
                    ClientID = "CLIENT_ABC"
                }
            };

            SetupMockDataReader(expectedResults);
            var queryExecutor = new SqlQueryExecutor(_mockConnection.Object, _mockLogger.Object);

            // Act
            var result = await queryExecutor.ExecuteQueryAsync<GDPRCompliantData>(query, parameters);

            // Assert
            Assert.IsNotNull(result);
            Assert.AreEqual(1);
            result.First().EmployeeIDAssert.AreEqual(1001);
            Assert.IsTrue(result.First().ConsentGiven);
            result.First().DataRetentionDateAssert.IsTrue(result.First().DataRetentionDate > DateTime.Now, DateTime.Now);
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

    public class EncryptedPIIData
    {
        public int EmployeeID { get; set; }
        public string EmployeeNumber { get; set; }
        public string FirstName { get; set; }
        public string LastName { get; set; }
        public byte[] EncryptedSSN { get; set; }
        public byte[] EncryptedTaxID { get; set; }
        public byte[] EncryptedPassport { get; set; }
        public byte[] EncryptedBankAccount { get; set; }
        public string Email { get; set; }
        public string Phone { get; set; }
        public DateTime DateOfBirth { get; set; }
        public string TenantID { get; set; }
        public string ClientID { get; set; }
    }

    public class DecryptedPIIData
    {
        public int EmployeeID { get; set; }
        public string EmployeeNumber { get; set; }
        public string FirstName { get; set; }
        public string LastName { get; set; }
        public string DecryptedSSN { get; set; }
        public string DecryptedTaxID { get; set; }
        public string DecryptedPassport { get; set; }
        public string DecryptedBankAccount { get; set; }
        public string TenantID { get; set; }
        public string ClientID { get; set; }
    }

    public class PayrollAccessData
    {
        public int EmployeeID { get; set; }
        public string EmployeeNumber { get; set; }
        public string FirstName { get; set; }
        public string LastName { get; set; }
        public decimal BaseSalary { get; set; }
        public decimal GrossSalary { get; set; }
        public decimal NetSalary { get; set; }
        public string TenantID { get; set; }
        public string ClientID { get; set; }
    }

    public class MaskedData
    {
        public int EmployeeID { get; set; }
        public string EmployeeNumber { get; set; }
        public string FirstName { get; set; }
        public string LastName { get; set; }
        public string MaskedSSN { get; set; }
        public string MaskedEmail { get; set; }
        public string MaskedBankAccount { get; set; }
        public string TenantID { get; set; }
        public string ClientID { get; set; }
    }

    public class AuditTrailData
    {
        public int EmployeeID { get; set; }
        public string EmployeeNumber { get; set; }
        public string FirstName { get; set; }
        public string LastName { get; set; }
        public string TenantID { get; set; }
        public string ClientID { get; set; }
    }

    public class GDPRCompliantData
    {
        public int EmployeeID { get; set; }
        public string EmployeeNumber { get; set; }
        public string FirstName { get; set; }
        public string LastName { get; set; }
        public string Email { get; set; }
        public string Phone { get; set; }
        public DateTime DateOfBirth { get; set; }
        public bool ConsentGiven { get; set; }
        public DateTime ConsentDate { get; set; }
        public DateTime DataRetentionDate { get; set; }
        public string TenantID { get; set; }
        public string ClientID { get; set; }
    }

    #endregion
}
