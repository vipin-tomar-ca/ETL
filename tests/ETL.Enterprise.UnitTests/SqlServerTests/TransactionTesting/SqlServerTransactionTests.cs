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
    /// Tests for transaction management, concurrency control, and isolation levels in SQL Server
    /// This class covers transaction scenarios, concurrency testing, and isolation level validation
    /// </summary>
    [TestClass]
    public class SqlServerTransactionTests
    {
        private Mock<ILogger<SqlServerTransactionTests>> _mockLogger;
        private string _connectionString;
        private string _sqlFilesDirectory;

        [TestInitialize]
        public void TestInitialize()
        {
            _mockLogger = new Mock<ILogger<SqlServerTransactionTests>>();
            _connectionString = Environment.GetEnvironmentVariable("PAYROLL_TEST_CONNECTION_STRING") 
                ?? "Server=localhost;Database=PayrollTestDB;Integrated Security=true;TrustServerCertificate=true;";
            
            _sqlFilesDirectory = Path.Combine(AppDomain.CurrentDomain.BaseDirectory, "SqlServerTests", "SqlFiles");
        }

        #region Transaction Isolation Level Tests

        /// <summary>
        /// Tests READ UNCOMMITTED isolation level behavior
        /// </summary>
        [TestMethod]
        public async Task ExecuteTransaction_WithReadUncommitted_AllowsDirtyReads()
        {
            // Arrange
            var connection1 = new SqlConnection(_connectionString);
            var connection2 = new SqlConnection(_connectionString);
            connection1.Open();
            connection2.Open();

            var queryExecutor1 = new SqlQueryExecutor(connection1, _mockLogger.Object);
            var queryExecutor2 = new SqlQueryExecutor(connection2, _mockLogger.Object);

            // Act
            var task1 = Task.Run(async () =>
            {
                // Start transaction and update data
                await queryExecutor1.ExecuteNonQueryAsync("BEGIN TRANSACTION");
                await queryExecutor1.ExecuteNonQueryAsync("UPDATE Employees SET FirstName = 'DirtyRead' WHERE EmployeeID = 1");
                await Task.Delay(2000); // Keep transaction open
                await queryExecutor1.ExecuteNonQueryAsync("ROLLBACK TRANSACTION");
            });

            var task2 = Task.Run(async () =>
            {
                await Task.Delay(500); // Wait for first transaction to start
                
                // Read with READ UNCOMMITTED
                var result = await queryExecutor2.ExecuteScalarAsync<string>(
                    "SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED; SELECT FirstName FROM Employees WHERE EmployeeID = 1");
                
                return result;
            });

            await Task.WhenAll(task1, task2);

            // Assert
            var result = await task2;
            Assert.AreEqual("DirtyRead", "READ UNCOMMITTED should allow dirty reads");
        }

        /// <summary>
        /// Tests READ COMMITTED isolation level behavior
        /// </summary>
        [TestMethod]
        public async Task ExecuteTransaction_WithReadCommitted_PreventsDirtyReads()
        {
            // Arrange
            var connection1 = new SqlConnection(_connectionString);
            var connection2 = new SqlConnection(_connectionString);
            connection1.Open();
            connection2.Open();

            var queryExecutor1 = new SqlQueryExecutor(connection1, _mockLogger.Object);
            var queryExecutor2 = new SqlQueryExecutor(connection2, _mockLogger.Object);

            // Act
            var task1 = Task.Run(async () =>
            {
                // Start transaction and update data
                await queryExecutor1.ExecuteNonQueryAsync("BEGIN TRANSACTION");
                await queryExecutor1.ExecuteNonQueryAsync("UPDATE Employees SET FirstName = 'ReadCommitted' WHERE EmployeeID = 1");
                await Task.Delay(2000); // Keep transaction open
                await queryExecutor1.ExecuteNonQueryAsync("ROLLBACK TRANSACTION");
            });

            var task2 = Task.Run(async () =>
            {
                await Task.Delay(500); // Wait for first transaction to start
                
                // Read with READ COMMITTED (default)
                var result = await queryExecutor2.ExecuteScalarAsync<string>(
                    "SELECT FirstName FROM Employees WHERE EmployeeID = 1");
                
                return result;
            });

            await Task.WhenAll(task1, task2);

            // Assert
            var result = await task2;
            resultAssert.AreNotEqual("ReadCommitted", "READ COMMITTED should prevent dirty reads");
        }

        /// <summary>
        /// Tests REPEATABLE READ isolation level behavior
        /// </summary>
        [TestMethod]
        public async Task ExecuteTransaction_WithRepeatableRead_PreventsNonRepeatableReads()
        {
            // Arrange
            var connection1 = new SqlConnection(_connectionString);
            var connection2 = new SqlConnection(_connectionString);
            connection1.Open();
            connection2.Open();

            var queryExecutor1 = new SqlQueryExecutor(connection1, _mockLogger.Object);
            var queryExecutor2 = new SqlQueryExecutor(connection2, _mockLogger.Object);

            // Act
            var task1 = Task.Run(async () =>
            {
                // Start transaction and read data
                await queryExecutor1.ExecuteNonQueryAsync("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ");
                await queryExecutor1.ExecuteNonQueryAsync("BEGIN TRANSACTION");
                
                var firstRead = await queryExecutor1.ExecuteScalarAsync<string>("SELECT FirstName FROM Employees WHERE EmployeeID = 1");
                await Task.Delay(2000); // Wait for other transaction to update
                var secondRead = await queryExecutor1.ExecuteScalarAsync<string>("SELECT FirstName FROM Employees WHERE EmployeeID = 1");
                
                await queryExecutor1.ExecuteNonQueryAsync("COMMIT TRANSACTION");
                
                return new { FirstRead = firstRead, SecondRead = secondRead };
            });

            var task2 = Task.Run(async () =>
            {
                await Task.Delay(500); // Wait for first transaction to start
                
                // Update data
                await queryExecutor2.ExecuteNonQueryAsync("UPDATE Employees SET FirstName = 'Updated' WHERE EmployeeID = 1");
            });

            await Task.WhenAll(task1, task2);

            // Assert
            var result = await task1;
            result.FirstReadAssert.AreEqual(result.SecondRead, "REPEATABLE READ should prevent non-repeatable reads");
        }

        /// <summary>
        /// Tests SERIALIZABLE isolation level behavior
        /// </summary>
        [TestMethod]
        public async Task ExecuteTransaction_WithSerializable_PreventsPhantomReads()
        {
            // Arrange
            var connection1 = new SqlConnection(_connectionString);
            var connection2 = new SqlConnection(_connectionString);
            connection1.Open();
            connection2.Open();

            var queryExecutor1 = new SqlQueryExecutor(connection1, _mockLogger.Object);
            var queryExecutor2 = new SqlQueryExecutor(connection2, _mockLogger.Object);

            // Act
            var task1 = Task.Run(async () =>
            {
                // Start transaction and read data
                await queryExecutor1.ExecuteNonQueryAsync("SET TRANSACTION ISOLATION LEVEL SERIALIZABLE");
                await queryExecutor1.ExecuteNonQueryAsync("BEGIN TRANSACTION");
                
                var firstCount = await queryExecutor1.ExecuteScalarAsync<int>("SELECT COUNT(*) FROM Employees WHERE DepartmentID = 1");
                await Task.Delay(2000); // Wait for other transaction to insert
                var secondCount = await queryExecutor1.ExecuteScalarAsync<int>("SELECT COUNT(*) FROM Employees WHERE DepartmentID = 1");
                
                await queryExecutor1.ExecuteNonQueryAsync("COMMIT TRANSACTION");
                
                return new { FirstCount = firstCount, SecondCount = secondCount };
            });

            var task2 = Task.Run(async () =>
            {
                await Task.Delay(500); // Wait for first transaction to start
                
                // Insert new employee
                await queryExecutor2.ExecuteNonQueryAsync(@"
                    INSERT INTO Employees (
                        EmployeeNumber, FirstName, LastName, Email, DateOfBirth, Gender,
                        StartDate, DepartmentID, PositionID, SalaryGrade, JobLevel, EmploymentStatus,
                        BusinessUnit, Region, Country, ContractType, EmploymentType
                    ) VALUES (
                        'TEST_SERIALIZABLE', 'Test', 'Serializable', 'test.serializable@company.com', 
                        '1990-01-01', 'Male', GETDATE(), 1, 1, 'SE1', 'L3', 'Active',
                        'Technology', 'North America', 'USA', 'Full-Time', 'Permanent'
                    )");
            });

            await Task.WhenAll(task1, task2);

            // Assert
            var result = await task1;
            result.FirstCountAssert.AreEqual(result.SecondCount, "SERIALIZABLE should prevent phantom reads");
        }

        #endregion

        #region Concurrent Access Tests

        /// <summary>
        /// Tests concurrent read operations
        /// </summary>
        [TestMethod]
        public async Task ExecuteConcurrentReads_MultipleConnections_HandlesCorrectly()
        {
            // Arrange
            var tasks = new List<Task<int>>();
            var connectionCount = 10;

            // Act
            for (int i = 0; i < connectionCount; i++)
            {
                tasks.Add(Task.Run(async () =>
                {
                    var connection = new SqlConnection(_connectionString);
                    connection.Open();
                    var queryExecutor = new SqlQueryExecutor(connection, _mockLogger.Object);
                    
                    var result = await queryExecutor.ExecuteScalarAsync<int>("SELECT COUNT(*) FROM Employees");
                    
                    connection.Close();
                    return result;
                }));
            }

            var results = await Task.WhenAll(tasks);

            // Assert
            resultsAssert.IsTrue(results.All(r => r.Equals(results[0])), results[0], "All concurrent reads should return same result");
        }

        /// <summary>
        /// Tests concurrent write operations with locking
        /// </summary>
        [TestMethod]
        public async Task ExecuteConcurrentWrites_WithLocking_HandlesCorrectly()
        {
            // Arrange
            var tasks = new List<Task>();
            var connectionCount = 5;

            // Act
            for (int i = 0; i < connectionCount; i++)
            {
                int index = i; // Capture loop variable
                tasks.Add(Task.Run(async () =>
                {
                    var connection = new SqlConnection(_connectionString);
                    connection.Open();
                    var queryExecutor = new SqlQueryExecutor(connection, _mockLogger.Object);
                    
                    try
                    {
                        await queryExecutor.ExecuteNonQueryAsync(@"
                            UPDATE Employees 
                            SET ModifiedDate = GETDATE(), ModifiedBy = @ModifiedBy 
                            WHERE EmployeeID = @EmployeeID", 
                            new Dictionary<string, object>
                            {
                                ["EmployeeID"] = 1,
                                ["ModifiedBy"] = $"User_{index}"
                            });
                    }
                    catch (SqlException ex) when (ex.Number == 1205) // Deadlock
                    {
                        // Expected for some concurrent updates
                    }
                    finally
                    {
                        connection.Close();
                    }
                }));
            }

            // Assert
            await Task.WhenAll(tasks);
            // Test passes if no exceptions are thrown (deadlocks are handled gracefully)
        }

        /// <summary>
        /// Tests lock escalation behavior
        /// </summary>
        [TestMethod]
        public async Task ExecuteBulkUpdate_WithLockEscalation_HandlesCorrectly()
        {
            // Arrange
            var queryExecutor = new SqlQueryExecutor(CreateConnection(), _mockLogger.Object);
            var bulkUpdateQuery = @"
                UPDATE Employees 
                SET ModifiedDate = GETDATE(), ModifiedBy = 'BulkUpdate'
                WHERE DepartmentID = 1";

            // Act
            var result = await queryExecutor.ExecuteNonQueryAsync(bulkUpdateQuery);

            // Assert
            resultAssert.IsTrue(result > 0, "Bulk update should succeed");
        }

        #endregion

        #region Transaction Rollback Tests

        /// <summary>
        /// Tests explicit transaction rollback
        /// </summary>
        [TestMethod]
        public async Task ExecuteTransaction_WithExplicitRollback_RollsBackCorrectly()
        {
            // Arrange
            var queryExecutor = new SqlQueryExecutor(CreateConnection(), _mockLogger.Object);
            var originalFirstName = await queryExecutor.ExecuteScalarAsync<string>("SELECT FirstName FROM Employees WHERE EmployeeID = 1");

            // Act
            await queryExecutor.ExecuteNonQueryAsync(@"
                BEGIN TRANSACTION;
                UPDATE Employees SET FirstName = 'RollbackTest' WHERE EmployeeID = 1;
                ROLLBACK TRANSACTION;");

            // Assert
            var currentFirstName = await queryExecutor.ExecuteScalarAsync<string>("SELECT FirstName FROM Employees WHERE EmployeeID = 1");
            currentFirstNameAssert.AreEqual(originalFirstName, "Rollback should restore original value");
        }

        /// <summary>
        /// Tests implicit transaction rollback on error
        /// </summary>
        [TestMethod]
        public async Task ExecuteTransaction_WithError_ImplicitlyRollsBack()
        {
            // Arrange
            var queryExecutor = new SqlQueryExecutor(CreateConnection(), _mockLogger.Object);
            var originalFirstName = await queryExecutor.ExecuteScalarAsync<string>("SELECT FirstName FROM Employees WHERE EmployeeID = 1");

            // Act & Assert
            await Assert.ThrowsExceptionAsync<SqlException>(async () =>
            {
                await queryExecutor.ExecuteNonQueryAsync(@"
                    BEGIN TRANSACTION;
                    UPDATE Employees SET FirstName = 'ErrorTest' WHERE EmployeeID = 1;
                    UPDATE NonExistentTable SET Column1 = 'Value' WHERE ID = 1;
                    COMMIT TRANSACTION;");
            });

            // Assert
            var currentFirstName = await queryExecutor.ExecuteScalarAsync<string>("SELECT FirstName FROM Employees WHERE EmployeeID = 1");
            currentFirstNameAssert.AreEqual(originalFirstName, "Error should cause implicit rollback");
        }

        /// <summary>
        /// Tests savepoint and partial rollback
        /// </summary>
        [TestMethod]
        public async Task ExecuteTransaction_WithSavepoint_PartialRollbackWorks()
        {
            // Arrange
            var queryExecutor = new SqlQueryExecutor(CreateConnection(), _mockLogger.Object);
            var originalFirstName = await queryExecutor.ExecuteScalarAsync<string>("SELECT FirstName FROM Employees WHERE EmployeeID = 1");
            var originalLastName = await queryExecutor.ExecuteScalarAsync<string>("SELECT LastName FROM Employees WHERE EmployeeID = 1");

            // Act
            await queryExecutor.ExecuteNonQueryAsync(@"
                BEGIN TRANSACTION;
                UPDATE Employees SET FirstName = 'SavepointTest' WHERE EmployeeID = 1;
                SAVE TRANSACTION Savepoint1;
                UPDATE Employees SET LastName = 'SavepointTest' WHERE EmployeeID = 1;
                ROLLBACK TRANSACTION Savepoint1;
                COMMIT TRANSACTION;");

            // Assert
            var currentFirstName = await queryExecutor.ExecuteScalarAsync<string>("SELECT FirstName FROM Employees WHERE EmployeeID = 1");
            var currentLastName = await queryExecutor.ExecuteScalarAsync<string>("SELECT LastName FROM Employees WHERE EmployeeID = 1");
            
            currentFirstNameAssert.AreEqual("SavepointTest", "First update should be committed");
            currentLastNameAssert.AreEqual(originalLastName, "Second update should be rolled back to savepoint");
        }

        #endregion

        #region Distributed Transaction Tests

        /// <summary>
        /// Tests distributed transaction coordination
        /// </summary>
        [TestMethod]
        public async Task ExecuteDistributedTransaction_WithMultipleOperations_CoordinatesCorrectly()
        {
            // Arrange
            var queryExecutor = new SqlQueryExecutor(CreateConnection(), _mockLogger.Object);

            // Act
            await queryExecutor.ExecuteNonQueryAsync(@"
                BEGIN DISTRIBUTED TRANSACTION;
                
                -- Update employee
                UPDATE Employees SET ModifiedDate = GETDATE() WHERE EmployeeID = 1;
                
                -- Update compensation
                UPDATE EmployeeCompensation SET ModifiedDate = GETDATE() WHERE EmployeeID = 1;
                
                -- Update address
                UPDATE EmployeeAddresses SET ModifiedDate = GETDATE() WHERE EmployeeID = 1;
                
                COMMIT TRANSACTION;");

            // Assert
            var employeeModified = await queryExecutor.ExecuteScalarAsync<DateTime?>("SELECT ModifiedDate FROM Employees WHERE EmployeeID = 1");
            var compensationModified = await queryExecutor.ExecuteScalarAsync<DateTime?>("SELECT ModifiedDate FROM EmployeeCompensation WHERE EmployeeID = 1");
            var addressModified = await queryExecutor.ExecuteScalarAsync<DateTime?>("SELECT ModifiedDate FROM EmployeeAddresses WHERE EmployeeID = 1");
            
            employeeModifiedAssert.IsNotNull("Employee should be updated");
            compensationModifiedAssert.IsNotNull("Compensation should be updated");
            addressModifiedAssert.IsNotNull("Address should be updated");
        }

        #endregion

        #region Long-Running Transaction Tests

        /// <summary>
        /// Tests long-running transaction behavior
        /// </summary>
        [TestMethod]
        public async Task ExecuteLongRunningTransaction_WithTimeout_HandlesCorrectly()
        {
            // Arrange
            var queryExecutor = new SqlQueryExecutor(CreateConnection(), _mockLogger.Object);

            // Act & Assert
            await Assert.ThrowsExceptionAsync<SqlException>(async () =>
            {
                await queryExecutor.ExecuteNonQueryAsync(@"
                    BEGIN TRANSACTION;
                    UPDATE Employees SET FirstName = 'LongRunning' WHERE EmployeeID = 1;
                    WAITFOR DELAY '00:00:10'; -- 10 second delay
                    COMMIT TRANSACTION;", timeoutSeconds: 5); // 5 second timeout
            });
        }

        /// <summary>
        /// Tests transaction with large data operations
        /// </summary>
        [TestMethod]
        public async Task ExecuteTransaction_WithLargeDataOperations_HandlesCorrectly()
        {
            // Arrange
            var queryExecutor = new SqlQueryExecutor(CreateConnection(), _mockLogger.Object);

            // Act
            await queryExecutor.ExecuteNonQueryAsync(@"
                BEGIN TRANSACTION;
                
                -- Large update operation
                UPDATE Employees 
                SET ModifiedDate = GETDATE(), ModifiedBy = 'LargeTransaction'
                WHERE EmployeeID IN (SELECT TOP 100 EmployeeID FROM Employees);
                
                -- Large insert operation
                INSERT INTO EmployeeAuditLog (EmployeeID, Action, ActionDate, ActionBy)
                SELECT TOP 100 EmployeeID, 'UPDATE', GETDATE(), 'LargeTransaction'
                FROM Employees;
                
                COMMIT TRANSACTION;");

            // Assert
            var auditCount = await queryExecutor.ExecuteScalarAsync<int>("SELECT COUNT(*) FROM EmployeeAuditLog WHERE ActionBy = 'LargeTransaction'");
            auditCountAssert.AreEqual(100, "Large transaction should complete successfully");
        }

        #endregion

        #region Helper Methods

        private IDbConnection CreateConnection()
        {
            var connection = new SqlConnection(_connectionString);
            connection.Open();
            return connection;
        }

        #endregion
    }
}
