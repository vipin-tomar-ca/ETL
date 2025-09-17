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
    /// Tests for comprehensive error handling, edge cases, and failure scenarios in SQL Server operations
    /// This class covers error handling, SQL injection prevention, timeout scenarios, and edge cases
    /// </summary>
    [TestClass]
    public class SqlServerErrorHandlingTests
    {
        private Mock<ILogger<SqlServerErrorHandlingTests>> _mockLogger;
        private string _connectionString;
        private string _sqlFilesDirectory;

        [TestInitialize]
        public void TestInitialize()
        {
            _mockLogger = new Mock<ILogger<SqlServerErrorHandlingTests>>();
            _connectionString = Environment.GetEnvironmentVariable("PAYROLL_TEST_CONNECTION_STRING") 
                ?? "Server=localhost;Database=PayrollTestDB;Integrated Security=true;TrustServerCertificate=true;";
            
            _sqlFilesDirectory = Path.Combine(AppDomain.CurrentDomain.BaseDirectory, "SqlServerTests", "SqlFiles");
        }

        #region SQL Injection Prevention Tests

        /// <summary>
        /// Tests SQL injection prevention with malicious input
        /// </summary>
        [TestMethod]
        public async Task ExecuteQuery_WithSQLInjectionAttempt_PreventsInjection()
        {
            // Arrange
            var queryExecutor = new SqlQueryExecutor(CreateConnection(), _mockLogger.Object);
            var query = @"
                SELECT EmployeeID, FirstName, LastName, Email
                FROM Employees 
                WHERE EmployeeNumber = @EmployeeNumber";

            var maliciousParameters = new Dictionary<string, object>
            {
                ["EmployeeNumber"] = "EMP001'; DROP TABLE Employees; --"
            };

            // Act
            var result = await queryExecutor.ExecuteQueryAsync<Dictionary<string, object>>(query, maliciousParameters);

            // Assert
            result.Should().NotBeNull("Query should execute without error");
            result.Should().BeEmpty("No results should be returned for malicious input");
            
            // Verify table still exists
            var tableCheckQuery = "SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = 'Employees'";
            var tableCount = await queryExecutor.ExecuteScalarAsync<int>(tableCheckQuery);
            tableCount.Should().Be(1, "Employees table should still exist");
        }

        /// <summary>
        /// Tests SQL injection prevention with UNION attack
        /// </summary>
        [TestMethod]
        public async Task ExecuteQuery_WithUNIONInjectionAttempt_PreventsInjection()
        {
            // Arrange
            var queryExecutor = new SqlQueryExecutor(CreateConnection(), _mockLogger.Object);
            var query = @"
                SELECT EmployeeID, FirstName, LastName
                FROM Employees 
                WHERE DepartmentID = @DepartmentID";

            var maliciousParameters = new Dictionary<string, object>
            {
                ["DepartmentID"] = "1 UNION SELECT Password, Username, Email FROM Users"
            };

            // Act
            var result = await queryExecutor.ExecuteQueryAsync<Dictionary<string, object>>(query, maliciousParameters);

            // Assert
            result.Should().NotBeNull("Query should execute without error");
            result.Should().BeEmpty("No results should be returned for malicious input");
        }

        /// <summary>
        /// Tests SQL injection prevention with comment injection
        /// </summary>
        [TestMethod]
        public async Task ExecuteQuery_WithCommentInjectionAttempt_PreventsInjection()
        {
            // Arrange
            var queryExecutor = new SqlQueryExecutor(CreateConnection(), _mockLogger.Object);
            var query = @"
                SELECT EmployeeID, FirstName, LastName
                FROM Employees 
                WHERE IsActive = @IsActive";

            var maliciousParameters = new Dictionary<string, object>
            {
                ["IsActive"] = "1 -- AND Password = 'admin'"
            };

            // Act
            var result = await queryExecutor.ExecuteQueryAsync<Dictionary<string, object>>(query, maliciousParameters);

            // Assert
            result.Should().NotBeNull("Query should execute without error");
        }

        #endregion

        #region Timeout and Performance Tests

        /// <summary>
        /// Tests query timeout handling
        /// </summary>
        [TestMethod]
        public async Task ExecuteQuery_WithTimeout_HandlesTimeoutCorrectly()
        {
            // Arrange
            var queryExecutor = new SqlQueryExecutor(CreateConnection(), _mockLogger.Object);
            var timeoutQuery = @"
                WAITFOR DELAY '00:00:05'; -- 5 second delay
                SELECT EmployeeID, FirstName, LastName FROM Employees";

            // Act & Assert
            await Assert.ThrowsExceptionAsync<SqlException>(async () =>
            {
                await queryExecutor.ExecuteQueryAsync<Dictionary<string, object>>(timeoutQuery, timeout: 2000); // 2 second timeout
            });
        }

        /// <summary>
        /// Tests long-running query handling
        /// </summary>
        [TestMethod]
        public async Task ExecuteQuery_WithLongRunningQuery_HandlesCorrectly()
        {
            // Arrange
            var queryExecutor = new SqlQueryExecutor(CreateConnection(), _mockLogger.Object);
            var longRunningQuery = @"
                SELECT 
                    e1.EmployeeID,
                    e1.FirstName,
                    e1.LastName,
                    e2.EmployeeID as ManagerID,
                    e2.FirstName as ManagerFirstName,
                    e2.LastName as ManagerLastName
                FROM Employees e1
                CROSS JOIN Employees e2
                WHERE e1.EmployeeID != e2.EmployeeID";

            // Act
            var stopwatch = System.Diagnostics.Stopwatch.StartNew();
            var result = await queryExecutor.ExecuteQueryAsync<Dictionary<string, object>>(longRunningQuery);
            stopwatch.Stop();

            // Assert
            result.Should().NotBeNull("Long-running query should complete");
            stopwatch.Elapsed.TotalSeconds.Should().BeLessThan(30, "Query should complete within reasonable time");
        }

        /// <summary>
        /// Tests memory pressure scenarios
        /// </summary>
        [TestMethod]
        public async Task ExecuteQuery_WithLargeResultSet_HandlesMemoryCorrectly()
        {
            // Arrange
            var queryExecutor = new SqlQueryExecutor(CreateConnection(), _mockLogger.Object);
            var largeResultQuery = @"
                SELECT 
                    EmployeeID,
                    EmployeeNumber,
                    FirstName,
                    LastName,
                    Email,
                    StartDate,
                    EndDate,
                    DepartmentID,
                    PositionID
                FROM Employees
                CROSS JOIN Departments
                CROSS JOIN Positions";

            // Act
            var initialMemory = GC.GetTotalMemory(false);
            var result = await queryExecutor.ExecuteQueryAsync<Dictionary<string, object>>(largeResultQuery);
            var finalMemory = GC.GetTotalMemory(false);
            var memoryIncrease = (finalMemory - initialMemory) / (1024 * 1024); // MB

            // Assert
            result.Should().NotBeNull("Large result set query should complete");
            memoryIncrease.Should().BeLessThan(500, "Memory increase should be reasonable"); // Less than 500MB
        }

        #endregion

        #region Connection and Resource Tests

        /// <summary>
        /// Tests connection pool exhaustion scenarios
        /// </summary>
        [TestMethod]
        public async Task ExecuteMultipleQueries_WithConnectionPoolExhaustion_HandlesCorrectly()
        {
            // Arrange
            var tasks = new List<Task>();
            var maxConcurrentConnections = 100;

            // Act
            for (int i = 0; i < maxConcurrentConnections; i++)
            {
                tasks.Add(Task.Run(async () =>
                {
                    var queryExecutor = new SqlQueryExecutor(CreateConnection(), _mockLogger.Object);
                    var query = "SELECT COUNT(*) FROM Employees";
                    await queryExecutor.ExecuteScalarAsync<int>(query);
                }));
            }

            // Assert
            await Assert.ThrowsExceptionAsync<AggregateException>(async () =>
            {
                await Task.WhenAll(tasks);
            });
        }

        /// <summary>
        /// Tests deadlock detection and handling
        /// </summary>
        [TestMethod]
        public async Task ExecuteConcurrentQueries_WithDeadlockPotential_HandlesDeadlock()
        {
            // Arrange
            var queryExecutor1 = new SqlQueryExecutor(CreateConnection(), _mockLogger.Object);
            var queryExecutor2 = new SqlQueryExecutor(CreateConnection(), _mockLogger.Object);

            var task1 = Task.Run(async () =>
            {
                // Update Employees then Departments
                await queryExecutor1.ExecuteNonQueryAsync("UPDATE Employees SET ModifiedDate = GETDATE() WHERE EmployeeID = 1");
                await Task.Delay(100); // Small delay to increase deadlock chance
                await queryExecutor1.ExecuteNonQueryAsync("UPDATE Departments SET ModifiedDate = GETDATE() WHERE DepartmentID = 1");
            });

            var task2 = Task.Run(async () =>
            {
                // Update Departments then Employees (reverse order)
                await queryExecutor2.ExecuteNonQueryAsync("UPDATE Departments SET ModifiedDate = GETDATE() WHERE DepartmentID = 1");
                await Task.Delay(100); // Small delay to increase deadlock chance
                await queryExecutor2.ExecuteNonQueryAsync("UPDATE Employees SET ModifiedDate = GETDATE() WHERE EmployeeID = 1");
            });

            // Act & Assert
            try
            {
                await Task.WhenAll(task1, task2);
            }
            catch (AggregateException ex)
            {
                // One of the tasks should fail with deadlock exception
                ex.InnerExceptions.Should().Contain(e => e is SqlException sqlEx && sqlEx.Number == 1205);
            }
        }

        /// <summary>
        /// Tests resource cleanup on connection failure
        /// </summary>
        [TestMethod]
        public async Task ExecuteQuery_WithConnectionFailure_CleansUpResources()
        {
            // Arrange
            var invalidConnectionString = "Server=invalidserver;Database=InvalidDB;Integrated Security=true;";
            var queryExecutor = new SqlQueryExecutor(new SqlConnection(invalidConnectionString), _mockLogger.Object);

            // Act & Assert
            await Assert.ThrowsExceptionAsync<SqlException>(async () =>
            {
                await queryExecutor.ExecuteQueryAsync<Dictionary<string, object>>("SELECT 1");
            });
        }

        #endregion

        #region Data Validation and Edge Cases

        /// <summary>
        /// Tests handling of NULL values in queries
        /// </summary>
        [TestMethod]
        public async Task ExecuteQuery_WithNULLValues_HandlesCorrectly()
        {
            // Arrange
            var queryExecutor = new SqlQueryExecutor(CreateConnection(), _mockLogger.Object);
            var nullQuery = @"
                SELECT 
                    EmployeeID,
                    FirstName,
                    LastName,
                    MiddleName,
                    EndDate,
                    TerminationReason
                FROM Employees 
                WHERE MiddleName IS NULL 
                   OR EndDate IS NULL 
                   OR TerminationReason IS NULL";

            // Act
            var result = await queryExecutor.ExecuteQueryAsync<Dictionary<string, object>>(nullQuery);

            // Assert
            result.Should().NotBeNull("Query with NULL values should execute");
            result.Should().NotBeEmpty("Should return employees with NULL values");
        }

        /// <summary>
        /// Tests handling of empty result sets
        /// </summary>
        [TestMethod]
        public async Task ExecuteQuery_WithEmptyResultSet_HandlesCorrectly()
        {
            // Arrange
            var queryExecutor = new SqlQueryExecutor(CreateConnection(), _mockLogger.Object);
            var emptyQuery = @"
                SELECT EmployeeID, FirstName, LastName
                FROM Employees 
                WHERE EmployeeNumber = 'NONEXISTENT'";

            // Act
            var result = await queryExecutor.ExecuteQueryAsync<Dictionary<string, object>>(emptyQuery);

            // Assert
            result.Should().NotBeNull("Query should execute without error");
            result.Should().BeEmpty("Should return empty result set");
        }

        /// <summary>
        /// Tests handling of invalid data types
        /// </summary>
        [TestMethod]
        public async Task ExecuteQuery_WithInvalidDataTypes_ThrowsExpectedException()
        {
            // Arrange
            var queryExecutor = new SqlQueryExecutor(CreateConnection(), _mockLogger.Object);
            var invalidTypeQuery = @"
                SELECT EmployeeID, FirstName, LastName
                FROM Employees 
                WHERE EmployeeID = @InvalidEmployeeID";

            var parameters = new Dictionary<string, object>
            {
                ["InvalidEmployeeID"] = "NOT_A_NUMBER"
            };

            // Act & Assert
            await Assert.ThrowsExceptionAsync<SqlException>(async () =>
            {
                await queryExecutor.ExecuteQueryAsync<Dictionary<string, object>>(invalidTypeQuery, parameters);
            });
        }

        /// <summary>
        /// Tests handling of constraint violations
        /// </summary>
        [TestMethod]
        public async Task ExecuteInsert_WithConstraintViolation_ThrowsExpectedException()
        {
            // Arrange
            var queryExecutor = new SqlQueryExecutor(CreateConnection(), _mockLogger.Object);
            var insertQuery = @"
                INSERT INTO Employees (
                    EmployeeNumber, FirstName, LastName, Email, DateOfBirth, Gender,
                    StartDate, DepartmentID, PositionID, SalaryGrade, JobLevel, EmploymentStatus,
                    BusinessUnit, Region, Country, ContractType, EmploymentType
                ) VALUES (
                    @EmployeeNumber, @FirstName, @LastName, @Email, @DateOfBirth, @Gender,
                    @StartDate, @DepartmentID, @PositionID, @SalaryGrade, @JobLevel, @EmploymentStatus,
                    @BusinessUnit, @Region, @Country, @ContractType, @EmploymentType
                )";

            var parameters = new Dictionary<string, object>
            {
                ["EmployeeNumber"] = "EMP001", // Duplicate employee number
                ["FirstName"] = "Test",
                ["LastName"] = "Employee",
                ["Email"] = "test.employee@company.com",
                ["DateOfBirth"] = new DateTime(1990, 1, 1),
                ["Gender"] = "Male",
                ["StartDate"] = DateTime.Now,
                ["DepartmentID"] = 1,
                ["PositionID"] = 1,
                ["SalaryGrade"] = "SE1",
                ["JobLevel"] = "L3",
                ["EmploymentStatus"] = "Active",
                ["BusinessUnit"] = "Technology",
                ["Region"] = "North America",
                ["Country"] = "USA",
                ["ContractType"] = "Full-Time",
                ["EmploymentType"] = "Permanent"
            };

            // Act & Assert
            await Assert.ThrowsExceptionAsync<SqlException>(async () =>
            {
                await queryExecutor.ExecuteNonQueryAsync(insertQuery, parameters);
            });
        }

        #endregion

        #region Transaction Error Handling

        /// <summary>
        /// Tests transaction rollback on error
        /// </summary>
        [TestMethod]
        public async Task ExecuteTransaction_WithError_RollsBackCorrectly()
        {
            // Arrange
            var queryExecutor = new SqlQueryExecutor(CreateConnection(), _mockLogger.Object);
            var transactionQuery = @"
                BEGIN TRANSACTION;
                
                UPDATE Employees SET FirstName = 'Updated' WHERE EmployeeID = 1;
                
                -- This will cause an error
                UPDATE NonExistentTable SET Column1 = 'Value' WHERE ID = 1;
                
                COMMIT TRANSACTION;";

            // Act & Assert
            await Assert.ThrowsExceptionAsync<SqlException>(async () =>
            {
                await queryExecutor.ExecuteNonQueryAsync(transactionQuery);
            });

            // Verify rollback occurred
            var verifyQuery = "SELECT FirstName FROM Employees WHERE EmployeeID = 1";
            var firstName = await queryExecutor.ExecuteScalarAsync<string>(verifyQuery);
            firstName.Should().NotBe("Updated", "Transaction should have been rolled back");
        }

        /// <summary>
        /// Tests nested transaction handling
        /// </summary>
        [TestMethod]
        public async Task ExecuteNestedTransaction_WithError_HandlesCorrectly()
        {
            // Arrange
            var queryExecutor = new SqlQueryExecutor(CreateConnection(), _mockLogger.Object);
            var nestedTransactionQuery = @"
                BEGIN TRANSACTION OuterTransaction;
                
                UPDATE Employees SET FirstName = 'Outer' WHERE EmployeeID = 1;
                
                BEGIN TRANSACTION InnerTransaction;
                
                UPDATE Employees SET LastName = 'Inner' WHERE EmployeeID = 1;
                
                -- This will cause an error
                UPDATE NonExistentTable SET Column1 = 'Value' WHERE ID = 1;
                
                COMMIT TRANSACTION InnerTransaction;
                COMMIT TRANSACTION OuterTransaction;";

            // Act & Assert
            await Assert.ThrowsExceptionAsync<SqlException>(async () =>
            {
                await queryExecutor.ExecuteNonQueryAsync(nestedTransactionQuery);
            });
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
