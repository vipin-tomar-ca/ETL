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
    /// Tests for data cleanup, teardown procedures, and resource management
    /// This class ensures proper cleanup of test data and database state reset
    /// </summary>
    [TestClass]
    public class DataCleanupTests
    {
        private Mock<ILogger<DataCleanupTests>> _mockLogger;
        private string _connectionString;
        private string _sqlFilesDirectory;

        [TestInitialize]
        public void TestInitialize()
        {
            _mockLogger = new Mock<ILogger<DataCleanupTests>>();
            _connectionString = Environment.GetEnvironmentVariable("PAYROLL_TEST_CONNECTION_STRING") 
                ?? "Server=localhost;Database=PayrollTestDB;Integrated Security=true;TrustServerCertificate=true;";
            
            _sqlFilesDirectory = Path.Combine(AppDomain.CurrentDomain.BaseDirectory, "SqlServerTests", "SqlFiles");
        }

        #region Test Data Cleanup Tests

        /// <summary>
        /// Tests cleanup of test data after test execution
        /// </summary>
        [TestMethod]
        public async Task CleanupTestData_AfterTestExecution_RemovesAllTestData()
        {
            // Arrange
            var queryExecutor = new SqlQueryExecutor(CreateConnection(), _mockLogger.Object);
            
            // Insert test data
            await queryExecutor.ExecuteNonQueryAsync(@"
                INSERT INTO Employees (
                    EmployeeNumber, FirstName, LastName, Email, DateOfBirth, Gender,
                    StartDate, DepartmentID, PositionID, SalaryGrade, JobLevel, EmploymentStatus,
                    BusinessUnit, Region, Country, ContractType, EmploymentType
                ) VALUES (
                    'CLEANUP_TEST_001', 'Cleanup', 'Test', 'cleanup.test@company.com', 
                    '1990-01-01', 'Male', GETDATE(), 1, 1, 'SE1', 'L3', 'Active',
                    'Technology', 'North America', 'USA', 'Full-Time', 'Permanent'
                )");

            // Act
            await CleanupTestData(queryExecutor);

            // Assert
            var testEmployeeCount = await queryExecutor.ExecuteScalarAsync<int>(
                "SELECT COUNT(*) FROM Employees WHERE EmployeeNumber LIKE 'CLEANUP_TEST_%'");
            testEmployeeCount.Should().Be(0, "All test employees should be cleaned up");
        }

        /// <summary>
        /// Tests cleanup of test data with foreign key constraints
        /// </summary>
        [TestMethod]
        public async Task CleanupTestData_WithForeignKeyConstraints_HandlesCorrectly()
        {
            // Arrange
            var queryExecutor = new SqlQueryExecutor(CreateConnection(), _mockLogger.Object);
            
            // Insert test data with relationships
            await queryExecutor.ExecuteNonQueryAsync(@"
                INSERT INTO Employees (
                    EmployeeNumber, FirstName, LastName, Email, DateOfBirth, Gender,
                    StartDate, DepartmentID, PositionID, SalaryGrade, JobLevel, EmploymentStatus,
                    BusinessUnit, Region, Country, ContractType, EmploymentType
                ) VALUES (
                    'CLEANUP_FK_001', 'CleanupFK', 'Test', 'cleanup.fk@company.com', 
                    '1990-01-01', 'Male', GETDATE(), 1, 1, 'SE1', 'L3', 'Active',
                    'Technology', 'North America', 'USA', 'Full-Time', 'Permanent'
                )");

            var employeeId = await queryExecutor.ExecuteScalarAsync<int>(
                "SELECT EmployeeID FROM Employees WHERE EmployeeNumber = 'CLEANUP_FK_001'");

            // Insert related data
            await queryExecutor.ExecuteNonQueryAsync($@"
                INSERT INTO EmployeeAddresses (EmployeeID, AddressType, AddressLine1, City, State, PostalCode, Country, IsPrimary)
                VALUES ({employeeId}, 'Home', '123 Test St', 'Test City', 'Test State', '12345', 'USA', 1)");

            await queryExecutor.ExecuteNonQueryAsync($@"
                INSERT INTO EmployeeBankAccounts (EmployeeID, BankName, BankAccountNumber, AccountHolderName, AccountType, IsPrimary)
                VALUES ({employeeId}, 'Test Bank', '123456789', 'CleanupFK Test', 'Checking', 1)");

            // Act
            await CleanupTestData(queryExecutor);

            // Assert
            var testEmployeeCount = await queryExecutor.ExecuteScalarAsync<int>(
                "SELECT COUNT(*) FROM Employees WHERE EmployeeNumber LIKE 'CLEANUP_FK_%'");
            var testAddressCount = await queryExecutor.ExecuteScalarAsync<int>(
                "SELECT COUNT(*) FROM EmployeeAddresses ea INNER JOIN Employees e ON ea.EmployeeID = e.EmployeeID WHERE e.EmployeeNumber LIKE 'CLEANUP_FK_%'");
            var testBankCount = await queryExecutor.ExecuteScalarAsync<int>(
                "SELECT COUNT(*) FROM EmployeeBankAccounts eba INNER JOIN Employees e ON eba.EmployeeID = e.EmployeeID WHERE e.EmployeeNumber LIKE 'CLEANUP_FK_%'");
            
            testEmployeeCount.Should().Be(0, "Test employees should be cleaned up");
            testAddressCount.Should().Be(0, "Test addresses should be cleaned up");
            testBankCount.Should().Be(0, "Test bank accounts should be cleaned up");
        }

        /// <summary>
        /// Tests cleanup of test data with transactions
        /// </summary>
        [TestMethod]
        public async Task CleanupTestData_WithTransactions_HandlesCorrectly()
        {
            // Arrange
            var queryExecutor = new SqlQueryExecutor(CreateConnection(), _mockLogger.Object);
            
            // Insert test data in transaction
            await queryExecutor.ExecuteNonQueryAsync(@"
                BEGIN TRANSACTION;
                INSERT INTO Employees (
                    EmployeeNumber, FirstName, LastName, Email, DateOfBirth, Gender,
                    StartDate, DepartmentID, PositionID, SalaryGrade, JobLevel, EmploymentStatus,
                    BusinessUnit, Region, Country, ContractType, EmploymentType
                ) VALUES (
                    'CLEANUP_TXN_001', 'CleanupTxn', 'Test', 'cleanup.txn@company.com', 
                    '1990-01-01', 'Male', GETDATE(), 1, 1, 'SE1', 'L3', 'Active',
                    'Technology', 'North America', 'USA', 'Full-Time', 'Permanent'
                );
                COMMIT TRANSACTION;");

            // Act
            await CleanupTestData(queryExecutor);

            // Assert
            var testEmployeeCount = await queryExecutor.ExecuteScalarAsync<int>(
                "SELECT COUNT(*) FROM Employees WHERE EmployeeNumber LIKE 'CLEANUP_TXN_%'");
            testEmployeeCount.Should().Be(0, "Test employees should be cleaned up");
        }

        #endregion

        #region Database State Reset Tests

        /// <summary>
        /// Tests reset of database state to clean state
        /// </summary>
        [TestMethod]
        public async Task ResetDatabaseState_ToCleanState_ResetsCorrectly()
        {
            // Arrange
            var queryExecutor = new SqlQueryExecutor(CreateConnection(), _mockLogger.Object);
            
            // Modify database state
            await queryExecutor.ExecuteNonQueryAsync(@"
                INSERT INTO Employees (
                    EmployeeNumber, FirstName, LastName, Email, DateOfBirth, Gender,
                    StartDate, DepartmentID, PositionID, SalaryGrade, JobLevel, EmploymentStatus,
                    BusinessUnit, Region, Country, ContractType, EmploymentType
                ) VALUES (
                    'RESET_TEST_001', 'Reset', 'Test', 'reset.test@company.com', 
                    '1990-01-01', 'Male', GETDATE(), 1, 1, 'SE1', 'L3', 'Active',
                    'Technology', 'North America', 'USA', 'Full-Time', 'Permanent'
                )");

            // Act
            await ResetDatabaseState(queryExecutor);

            // Assert
            var testEmployeeCount = await queryExecutor.ExecuteScalarAsync<int>(
                "SELECT COUNT(*) FROM Employees WHERE EmployeeNumber LIKE 'RESET_TEST_%'");
            testEmployeeCount.Should().Be(0, "Database should be reset to clean state");
        }

        /// <summary>
        /// Tests reset of identity columns
        /// </summary>
        [TestMethod]
        public async Task ResetIdentityColumns_AfterDataCleanup_ResetsCorrectly()
        {
            // Arrange
            var queryExecutor = new SqlQueryExecutor(CreateConnection(), _mockLogger.Object);
            
            // Get current identity values
            var originalEmployeeIdentity = await queryExecutor.ExecuteScalarAsync<int>("SELECT IDENT_CURRENT('Employees')");
            var originalDepartmentIdentity = await queryExecutor.ExecuteScalarAsync<int>("SELECT IDENT_CURRENT('Departments')");

            // Insert test data
            await queryExecutor.ExecuteNonQueryAsync(@"
                INSERT INTO Employees (
                    EmployeeNumber, FirstName, LastName, Email, DateOfBirth, Gender,
                    StartDate, DepartmentID, PositionID, SalaryGrade, JobLevel, EmploymentStatus,
                    BusinessUnit, Region, Country, ContractType, EmploymentType
                ) VALUES (
                    'IDENTITY_TEST_001', 'Identity', 'Test', 'identity.test@company.com', 
                    '1990-01-01', 'Male', GETDATE(), 1, 1, 'SE1', 'L3', 'Active',
                    'Technology', 'North America', 'USA', 'Full-Time', 'Permanent'
                )");

            // Act
            await ResetIdentityColumns(queryExecutor);

            // Assert
            var newEmployeeIdentity = await queryExecutor.ExecuteScalarAsync<int>("SELECT IDENT_CURRENT('Employees')");
            var newDepartmentIdentity = await queryExecutor.ExecuteScalarAsync<int>("SELECT IDENT_CURRENT('Departments')");
            
            newEmployeeIdentity.Should().Be(originalEmployeeIdentity, "Employee identity should be reset");
            newDepartmentIdentity.Should().Be(originalDepartmentIdentity, "Department identity should be reset");
        }

        #endregion

        #region Resource Cleanup Tests

        /// <summary>
        /// Tests cleanup of database connections
        /// </summary>
        [TestMethod]
        public async Task CleanupDatabaseConnections_AfterTestExecution_ClosesAllConnections()
        {
            // Arrange
            var connections = new List<IDbConnection>();
            var connectionCount = 5;

            // Create multiple connections
            for (int i = 0; i < connectionCount; i++)
            {
                var connection = new SqlConnection(_connectionString);
                connection.Open();
                connections.Add(connection);
            }

            // Act
            await CleanupDatabaseConnections(connections);

            // Assert
            connections.Should().AllSatisfy(conn => 
                conn.State.Should().Be(ConnectionState.Closed, "All connections should be closed"));
        }

        /// <summary>
        /// Tests cleanup of database transactions
        /// </summary>
        [TestMethod]
        public async Task CleanupDatabaseTransactions_AfterTestExecution_CommitsOrRollsBackAllTransactions()
        {
            // Arrange
            var queryExecutor = new SqlQueryExecutor(CreateConnection(), _mockLogger.Object);
            
            // Start transaction
            await queryExecutor.ExecuteNonQueryAsync("BEGIN TRANSACTION");
            await queryExecutor.ExecuteNonQueryAsync("UPDATE Employees SET ModifiedDate = GETDATE() WHERE EmployeeID = 1");

            // Act
            await CleanupDatabaseTransactions(queryExecutor);

            // Assert
            // Transaction should be committed or rolled back
            var transactionCount = await queryExecutor.ExecuteScalarAsync<int>(
                "SELECT COUNT(*) FROM sys.dm_tran_active_transactions");
            transactionCount.Should().Be(0, "No active transactions should remain");
        }

        /// <summary>
        /// Tests cleanup of temporary objects
        /// </summary>
        [TestMethod]
        public async Task CleanupTemporaryObjects_AfterTestExecution_RemovesAllTempObjects()
        {
            // Arrange
            var queryExecutor = new SqlQueryExecutor(CreateConnection(), _mockLogger.Object);
            
            // Create temporary objects
            await queryExecutor.ExecuteNonQueryAsync(@"
                CREATE TABLE #TempTestTable (
                    ID INT IDENTITY(1,1) PRIMARY KEY,
                    Name NVARCHAR(100)
                )");

            await queryExecutor.ExecuteNonQueryAsync(@"
                CREATE PROCEDURE #TempTestProc
                AS
                BEGIN
                    SELECT 'Test' as Result
                END");

            // Act
            await CleanupTemporaryObjects(queryExecutor);

            // Assert
            var tempTableCount = await queryExecutor.ExecuteScalarAsync<int>(
                "SELECT COUNT(*) FROM tempdb.sys.tables WHERE name LIKE '#TempTestTable%'");
            var tempProcCount = await queryExecutor.ExecuteScalarAsync<int>(
                "SELECT COUNT(*) FROM tempdb.sys.procedures WHERE name LIKE '#TempTestProc%'");
            
            tempTableCount.Should().Be(0, "Temporary tables should be cleaned up");
            tempProcCount.Should().Be(0, "Temporary procedures should be cleaned up");
        }

        #endregion

        #region Data Integrity After Cleanup Tests

        /// <summary>
        /// Tests data integrity after cleanup
        /// </summary>
        [TestMethod]
        public async Task ValidateDataIntegrity_AfterCleanup_MaintainsIntegrity()
        {
            // Arrange
            var queryExecutor = new SqlQueryExecutor(CreateConnection(), _mockLogger.Object);
            
            // Insert test data
            await queryExecutor.ExecuteNonQueryAsync(@"
                INSERT INTO Employees (
                    EmployeeNumber, FirstName, LastName, Email, DateOfBirth, Gender,
                    StartDate, DepartmentID, PositionID, SalaryGrade, JobLevel, EmploymentStatus,
                    BusinessUnit, Region, Country, ContractType, EmploymentType
                ) VALUES (
                    'INTEGRITY_TEST_001', 'Integrity', 'Test', 'integrity.test@company.com', 
                    '1990-01-01', 'Male', GETDATE(), 1, 1, 'SE1', 'L3', 'Active',
                    'Technology', 'North America', 'USA', 'Full-Time', 'Permanent'
                )");

            // Act
            await CleanupTestData(queryExecutor);
            var integrityIssues = await ValidateDataIntegrity(queryExecutor);

            // Assert
            integrityIssues.Should().BeEmpty("No data integrity issues should remain after cleanup");
        }

        /// <summary>
        /// Tests foreign key constraint validation after cleanup
        /// </summary>
        [TestMethod]
        public async Task ValidateForeignKeyConstraints_AfterCleanup_MaintainsConstraints()
        {
            // Arrange
            var queryExecutor = new SqlQueryExecutor(CreateConnection(), _mockLogger.Object);
            
            // Insert test data with relationships
            await queryExecutor.ExecuteNonQueryAsync(@"
                INSERT INTO Employees (
                    EmployeeNumber, FirstName, LastName, Email, DateOfBirth, Gender,
                    StartDate, DepartmentID, PositionID, SalaryGrade, JobLevel, EmploymentStatus,
                    BusinessUnit, Region, Country, ContractType, EmploymentType
                ) VALUES (
                    'FK_TEST_001', 'FK', 'Test', 'fk.test@company.com', 
                    '1990-01-01', 'Male', GETDATE(), 1, 1, 'SE1', 'L3', 'Active',
                    'Technology', 'North America', 'USA', 'Full-Time', 'Permanent'
                )");

            var employeeId = await queryExecutor.ExecuteScalarAsync<int>(
                "SELECT EmployeeID FROM Employees WHERE EmployeeNumber = 'FK_TEST_001'");

            await queryExecutor.ExecuteNonQueryAsync($@"
                INSERT INTO EmployeeAddresses (EmployeeID, AddressType, AddressLine1, City, State, PostalCode, Country, IsPrimary)
                VALUES ({employeeId}, 'Home', '123 FK St', 'FK City', 'FK State', '12345', 'USA', 1)");

            // Act
            await CleanupTestData(queryExecutor);
            var constraintViolations = await ValidateForeignKeyConstraints(queryExecutor);

            // Assert
            constraintViolations.Should().BeEmpty("No foreign key constraint violations should remain after cleanup");
        }

        #endregion

        #region Helper Methods

        private IDbConnection CreateConnection()
        {
            var connection = new SqlConnection(_connectionString);
            connection.Open();
            return connection;
        }

        private async Task CleanupTestData(SqlQueryExecutor queryExecutor)
        {
            // Clean up test data in reverse order of dependencies
            await queryExecutor.ExecuteNonQueryAsync(@"
                DELETE FROM EmployeeSecurity WHERE EmployeeID IN (
                    SELECT EmployeeID FROM Employees WHERE EmployeeNumber LIKE 'CLEANUP_%' 
                    OR EmployeeNumber LIKE 'RESET_%' 
                    OR EmployeeNumber LIKE 'IDENTITY_%'
                    OR EmployeeNumber LIKE 'INTEGRITY_%'
                    OR EmployeeNumber LIKE 'FK_%'
                )");

            await queryExecutor.ExecuteNonQueryAsync(@"
                DELETE FROM EmployeeAbsences WHERE EmployeeID IN (
                    SELECT EmployeeID FROM Employees WHERE EmployeeNumber LIKE 'CLEANUP_%' 
                    OR EmployeeNumber LIKE 'RESET_%' 
                    OR EmployeeNumber LIKE 'IDENTITY_%'
                    OR EmployeeNumber LIKE 'INTEGRITY_%'
                    OR EmployeeNumber LIKE 'FK_%'
                )");

            await queryExecutor.ExecuteNonQueryAsync(@"
                DELETE FROM EmployeeCompensation WHERE EmployeeID IN (
                    SELECT EmployeeID FROM Employees WHERE EmployeeNumber LIKE 'CLEANUP_%' 
                    OR EmployeeNumber LIKE 'RESET_%' 
                    OR EmployeeNumber LIKE 'IDENTITY_%'
                    OR EmployeeNumber LIKE 'INTEGRITY_%'
                    OR EmployeeNumber LIKE 'FK_%'
                )");

            await queryExecutor.ExecuteNonQueryAsync(@"
                DELETE FROM EmployeeBankAccounts WHERE EmployeeID IN (
                    SELECT EmployeeID FROM Employees WHERE EmployeeNumber LIKE 'CLEANUP_%' 
                    OR EmployeeNumber LIKE 'RESET_%' 
                    OR EmployeeNumber LIKE 'IDENTITY_%'
                    OR EmployeeNumber LIKE 'INTEGRITY_%'
                    OR EmployeeNumber LIKE 'FK_%'
                )");

            await queryExecutor.ExecuteNonQueryAsync(@"
                DELETE FROM EmployeeAddresses WHERE EmployeeID IN (
                    SELECT EmployeeID FROM Employees WHERE EmployeeNumber LIKE 'CLEANUP_%' 
                    OR EmployeeNumber LIKE 'RESET_%' 
                    OR EmployeeNumber LIKE 'IDENTITY_%'
                    OR EmployeeNumber LIKE 'INTEGRITY_%'
                    OR EmployeeNumber LIKE 'FK_%'
                )");

            await queryExecutor.ExecuteNonQueryAsync(@"
                DELETE FROM Employees WHERE EmployeeNumber LIKE 'CLEANUP_%' 
                OR EmployeeNumber LIKE 'RESET_%' 
                OR EmployeeNumber LIKE 'IDENTITY_%'
                OR EmployeeNumber LIKE 'INTEGRITY_%'
                OR EmployeeNumber LIKE 'FK_%'");

            // Clean up audit logs
            await queryExecutor.ExecuteNonQueryAsync(@"
                DELETE FROM EmployeeAuditLog WHERE ActionBy LIKE '%TEST%' 
                OR ActionBy LIKE '%Cleanup%' 
                OR ActionBy LIKE '%Reset%'");
        }

        private async Task ResetDatabaseState(SqlQueryExecutor queryExecutor)
        {
            await CleanupTestData(queryExecutor);
            await ResetIdentityColumns(queryExecutor);
        }

        private async Task ResetIdentityColumns(SqlQueryExecutor queryExecutor)
        {
            // Reset identity columns to their original values
            await queryExecutor.ExecuteNonQueryAsync("DBCC CHECKIDENT ('Employees', RESEED, 1002)");
            await queryExecutor.ExecuteNonQueryAsync("DBCC CHECKIDENT ('Departments', RESEED, 10)");
            await queryExecutor.ExecuteNonQueryAsync("DBCC CHECKIDENT ('Positions', RESEED, 28)");
        }

        private async Task CleanupDatabaseConnections(List<IDbConnection> connections)
        {
            foreach (var connection in connections)
            {
                if (connection.State == ConnectionState.Open)
                {
                    connection.Close();
                }
                connection.Dispose();
            }
        }

        private async Task CleanupDatabaseTransactions(SqlQueryExecutor queryExecutor)
        {
            // Commit or rollback any active transactions
            await queryExecutor.ExecuteNonQueryAsync(@"
                IF @@TRANCOUNT > 0
                    ROLLBACK TRANSACTION");
        }

        private async Task CleanupTemporaryObjects(SqlQueryExecutor queryExecutor)
        {
            // Clean up temporary objects
            await queryExecutor.ExecuteNonQueryAsync(@"
                IF OBJECT_ID('tempdb..#TempTestTable') IS NOT NULL
                    DROP TABLE #TempTestTable");

            await queryExecutor.ExecuteNonQueryAsync(@"
                IF OBJECT_ID('tempdb..#TempTestProc') IS NOT NULL
                    DROP PROCEDURE #TempTestProc");
        }

        private async Task<List<string>> ValidateDataIntegrity(SqlQueryExecutor queryExecutor)
        {
            var issues = new List<string>();

            // Check for orphaned records
            var orphanedAddresses = await queryExecutor.ExecuteScalarAsync<int>(@"
                SELECT COUNT(*) FROM EmployeeAddresses ea 
                LEFT JOIN Employees e ON ea.EmployeeID = e.EmployeeID 
                WHERE e.EmployeeID IS NULL");

            if (orphanedAddresses > 0)
                issues.Add($"Found {orphanedAddresses} orphaned address records");

            var orphanedBankAccounts = await queryExecutor.ExecuteScalarAsync<int>(@"
                SELECT COUNT(*) FROM EmployeeBankAccounts eba 
                LEFT JOIN Employees e ON eba.EmployeeID = e.EmployeeID 
                WHERE e.EmployeeID IS NULL");

            if (orphanedBankAccounts > 0)
                issues.Add($"Found {orphanedBankAccounts} orphaned bank account records");

            return issues;
        }

        private async Task<List<string>> ValidateForeignKeyConstraints(SqlQueryExecutor queryExecutor)
        {
            var violations = new List<string>();

            // Check foreign key constraints
            var invalidDepartmentRefs = await queryExecutor.ExecuteScalarAsync<int>(@"
                SELECT COUNT(*) FROM Employees e 
                LEFT JOIN Departments d ON e.DepartmentID = d.DepartmentID 
                WHERE d.DepartmentID IS NULL");

            if (invalidDepartmentRefs > 0)
                violations.Add($"Found {invalidDepartmentRefs} invalid department references");

            var invalidPositionRefs = await queryExecutor.ExecuteScalarAsync<int>(@"
                SELECT COUNT(*) FROM Employees e 
                LEFT JOIN Positions p ON e.PositionID = p.PositionID 
                WHERE p.PositionID IS NULL");

            if (invalidPositionRefs > 0)
                violations.Add($"Found {invalidPositionRefs} invalid position references");

            return violations;
        }

        #endregion
    }
}
