using Microsoft.VisualStudio.TestTools.UnitTesting;
using Microsoft.Extensions.Logging;
using Moq;
using System;
using System.Collections.Generic;
using System.Data;
using System.Threading.Tasks;
using System.Diagnostics;
using System.Linq;
using ETL.Enterprise.Domain.Entities;
using ETL.Enterprise.Domain.Enums;

namespace ETL.Tests.Unit
{
    /// <summary>
    /// Performance and load testing for SQL queries
    /// Tests query execution times, memory usage, and scalability
    /// </summary>
    [TestClass]
    public class SqlQueryPerformanceTests
    {
        private Mock<ILogger<SqlQueryPerformanceTests>> _mockLogger;
        private DatabaseTestSetup _testSetup;
        private SqlQueryExecutor _queryExecutor;

        [TestInitialize]
        public void TestInitialize()
        {
            _mockLogger = new Mock<ILogger<SqlQueryPerformanceTests>>();
            _testSetup = new DatabaseTestSetup();
        }

        [TestCleanup]
        public void TestCleanup()
        {
            _testSetup?.Dispose();
        }

        #region Query Execution Time Tests

        /// <summary>
        /// Tests that simple SELECT queries execute within acceptable time limits
        /// </summary>
        [TestMethod]
        public async Task ExecuteQuery_SimpleSelect_PerformsWithinTimeLimit()
        {
            // Arrange
            var query = "SELECT CustomerID, CustomerName, Email FROM Customers WHERE IsActive = 1";
            var maxExecutionTime = TimeSpan.FromSeconds(1);
            var mockConnection = _testSetup.CreateMockConnection();
            _queryExecutor = new SqlQueryExecutor(mockConnection.Object, _mockLogger.Object);

            // Act
            var executionTime = await SqlQueryTestUtilities.MeasureExecutionTime(async () =>
            {
                await _queryExecutor.ExecuteQueryAsync<CustomerData>(query);
            });

            // Assert
            Assert.IsTrue(executionTime < maxExecutionTime, 
                $"Simple SELECT query should execute within {maxExecutionTime.TotalMilliseconds}ms");
        }

        /// <summary>
        /// Tests that complex JOIN queries execute within acceptable time limits
        /// </summary>
        [TestMethod]
        public async Task ExecuteQuery_ComplexJoins_PerformsWithinTimeLimit()
        {
            // Arrange
            var query = SqlQueryTestUtilities.QueryTemplates.ComplexJoinQuery;
            var maxExecutionTime = TimeSpan.FromSeconds(5);
            var parameters = new Dictionary<string, object>
            {
                ["StartDate"] = new DateTime(2024, 1, 1)
            };

            var mockConnection = _testSetup.CreateMockConnection();
            _queryExecutor = new SqlQueryExecutor(mockConnection.Object, _mockLogger.Object);

            // Act
            var executionTime = await SqlQueryTestUtilities.MeasureExecutionTime(async () =>
            {
                await _queryExecutor.ExecuteQueryAsync<OrderDetailData>(query, parameters);
            });

            // Assert
            Assert.IsTrue(executionTime < maxExecutionTime, 
                $"Complex JOIN query should execute within {maxExecutionTime.TotalMilliseconds}ms");
        }

        /// <summary>
        /// Tests that aggregate queries execute within acceptable time limits
        /// </summary>
        [TestMethod]
        public async Task ExecuteQuery_AggregateQueries_PerformsWithinTimeLimit()
        {
            // Arrange
            var query = SqlQueryTestUtilities.QueryTemplates.AggregateQuery;
            var maxExecutionTime = TimeSpan.FromSeconds(3);
            var parameters = new Dictionary<string, object>
            {
                ["StartDate"] = new DateTime(2024, 1, 1),
                ["MinOrderCount"] = 1
            };

            var mockConnection = _testSetup.CreateMockConnection();
            _queryExecutor = new SqlQueryExecutor(mockConnection.Object, _mockLogger.Object);

            // Act
            var executionTime = await SqlQueryTestUtilities.MeasureExecutionTime(async () =>
            {
                await _queryExecutor.ExecuteQueryAsync<CustomerAggregateData>(query, parameters);
            });

            // Assert
            Assert.IsTrue(executionTime < maxExecutionTime, 
                $"Aggregate query should execute within {maxExecutionTime.TotalMilliseconds}ms");
        }

        #endregion

        #region Large Dataset Performance Tests

        /// <summary>
        /// Tests query performance with large result sets
        /// </summary>
        [TestMethod]
        public async Task ExecuteQuery_LargeResultSet_PerformsWithinTimeLimit()
        {
            // Arrange
            var query = "SELECT * FROM Customers ORDER BY CustomerID";
            var maxExecutionTime = TimeSpan.FromSeconds(10);
            var largeDataSet = SqlQueryTestUtilities.GenerateTestCustomers(10000);

            var mockConnection = _testSetup.CreateMockConnection();
            var mockDataReader = _testSetup.CreateMockDataReader(largeDataSet);
            var mockCommand = _testSetup.CreateMockCommand();
            mockCommand.Setup(cmd => cmd.ExecuteReader()).Returns(mockDataReader.Object);
            mockConnection.Setup(conn => conn.CreateCommand()).Returns(mockCommand.Object);

            _queryExecutor = new SqlQueryExecutor(mockConnection.Object, _mockLogger.Object);

            // Act
            var executionTime = await SqlQueryTestUtilities.MeasureExecutionTime(async () =>
            {
                await _queryExecutor.ExecuteQueryAsync<CustomerData>(query);
            });

            // Assert
            Assert.IsTrue(executionTime < maxExecutionTime, 
                $"Large result set query should execute within {maxExecutionTime.TotalMilliseconds}ms");
        }

        /// <summary>
        /// Tests query performance with large parameter sets
        /// </summary>
        [TestMethod]
        public async Task ExecuteQuery_LargeParameterSet_PerformsWithinTimeLimit()
        {
            // Arrange
            var query = "SELECT * FROM Customers WHERE CustomerID IN (@CustomerIDs)";
            var maxExecutionTime = TimeSpan.FromSeconds(5);
            var customerIds = Enumerable.Range(1, 1000).ToList();
            var parameters = new Dictionary<string, object>
            {
                ["CustomerIDs"] = string.Join(",", customerIds)
            };

            var mockConnection = _testSetup.CreateMockConnection();
            _queryExecutor = new SqlQueryExecutor(mockConnection.Object, _mockLogger.Object);

            // Act
            var executionTime = await SqlQueryTestUtilities.MeasureExecutionTime(async () =>
            {
                await _queryExecutor.ExecuteQueryAsync<CustomerData>(query, parameters);
            });

            // Assert
            Assert.IsTrue(executionTime < maxExecutionTime, 
                $"Query with large parameter set should execute within {maxExecutionTime.TotalMilliseconds}ms");
        }

        #endregion

        #region Memory Usage Tests

        /// <summary>
        /// Tests memory usage during query execution
        /// </summary>
        [TestMethod]
        public async Task ExecuteQuery_MemoryUsage_StaysWithinLimits()
        {
            // Arrange
            var query = "SELECT * FROM LargeTable";
            var maxMemoryIncreaseMB = 50; // 50MB limit
            var largeDataSet = SqlQueryTestUtilities.GenerateTestProducts(50000);

            var mockConnection = _testSetup.CreateMockConnection();
            var mockDataReader = _testSetup.CreateMockDataReader(largeDataSet);
            var mockCommand = _testSetup.CreateMockCommand();
            mockCommand.Setup(cmd => cmd.ExecuteReader()).Returns(mockDataReader.Object);
            mockConnection.Setup(conn => conn.CreateCommand()).Returns(mockCommand.Object);

            _queryExecutor = new SqlQueryExecutor(mockConnection.Object, _mockLogger.Object);

            // Act
            var initialMemory = GC.GetTotalMemory(false);
            await _queryExecutor.ExecuteQueryAsync<ProductData>(query);
            var finalMemory = GC.GetTotalMemory(false);
            var memoryIncrease = (finalMemory - initialMemory) / (1024 * 1024); // Convert to MB

            // Assert
            Assert.IsTrue(memoryIncrease < maxMemoryIncreaseMB, 
                $"Memory usage should not increase by more than {maxMemoryIncreaseMB}MB");
        }

        /// <summary>
        /// Tests memory usage with multiple concurrent queries
        /// </summary>
        [TestMethod]
        public async Task ExecuteQuery_ConcurrentQueries_MemoryUsageStaysWithinLimits()
        {
            // Arrange
            var query = "SELECT * FROM Customers";
            var maxMemoryIncreaseMB = 100; // 100MB limit for concurrent queries
            var concurrentTasks = 10;

            var mockConnection = _testSetup.CreateMockConnection();
            _queryExecutor = new SqlQueryExecutor(mockConnection.Object, _mockLogger.Object);

            // Act
            var initialMemory = GC.GetTotalMemory(false);
            
            var tasks = new List<Task>();
            for (int i = 0; i < concurrentTasks; i++)
            {
                tasks.Add(_queryExecutor.ExecuteQueryAsync<CustomerData>(query));
            }
            
            await Task.WhenAll(tasks);
            
            var finalMemory = GC.GetTotalMemory(false);
            var memoryIncrease = (finalMemory - initialMemory) / (1024 * 1024); // Convert to MB

            // Assert
            Assert.IsTrue(memoryIncrease < maxMemoryIncreaseMB, 
                $"Memory usage with {concurrentTasks} concurrent queries should not increase by more than {maxMemoryIncreaseMB}MB");
        }

        #endregion

        #region Timeout Tests

        /// <summary>
        /// Tests query timeout handling
        /// </summary>
        [TestMethod]
        public async Task ExecuteQuery_WithTimeout_ThrowsTimeoutException()
        {
            // Arrange
            var query = "SELECT * FROM LargeTable";
            var timeoutSeconds = 1;

            var mockConnection = _testSetup.CreateMockConnection();
            var mockCommand = _testSetup.CreateMockCommand();
            mockCommand.Setup(cmd => cmd.CommandTimeout).Returns(timeoutSeconds);
            mockCommand.Setup(cmd => cmd.ExecuteReader())
                .Throws(new TimeoutException("Query execution timeout"));
            mockConnection.Setup(conn => conn.CreateCommand()).Returns(mockCommand.Object);

            _queryExecutor = new SqlQueryExecutor(mockConnection.Object, _mockLogger.Object);

            // Act & Assert
            await Assert.ThrowsExceptionAsync<TimeoutException>(
                () => _queryExecutor.ExecuteQueryAsync<ProductData>(query, timeoutSeconds: timeoutSeconds));
        }

        /// <summary>
        /// Tests that queries complete before timeout
        /// </summary>
        [TestMethod]
        public async Task ExecuteQuery_WithinTimeout_CompletesSuccessfully()
        {
            // Arrange
            var query = "SELECT CustomerID, CustomerName FROM Customers WHERE IsActive = 1";
            var timeoutSeconds = 30; // Generous timeout

            var mockConnection = _testSetup.CreateMockConnection();
            _queryExecutor = new SqlQueryExecutor(mockConnection.Object, _mockLogger.Object);

            // Act
            var result = await _queryExecutor.ExecuteQueryAsync<CustomerData>(query, timeoutSeconds: timeoutSeconds);

            // Assert
            Assert.IsNotNull(result, "Query should complete successfully within timeout");
        }

        #endregion

        #region Batch Operation Performance Tests

        /// <summary>
        /// Tests batch operation performance
        /// </summary>
        [TestMethod]
        public async Task ExecuteBatch_LargeBatch_PerformsWithinTimeLimit()
        {
            // Arrange
            var queries = new List<string>();
            for (int i = 1; i <= 1000; i++)
            {
                queries.Add($"INSERT INTO Customers (CustomerID, CustomerName, Email) VALUES ({i}, 'Customer {i}', 'customer{i}@example.com')");
            }

            var maxExecutionTime = TimeSpan.FromSeconds(15);
            var mockConnection = _testSetup.CreateMockConnection();
            _queryExecutor = new SqlQueryExecutor(mockConnection.Object, _mockLogger.Object);

            // Act
            var executionTime = await SqlQueryTestUtilities.MeasureExecutionTime(async () =>
            {
                await _queryExecutor.ExecuteBatchAsync(queries);
            });

            // Assert
            Assert.IsTrue(executionTime < maxExecutionTime, 
                $"Batch operation with {queries.Count} queries should execute within {maxExecutionTime.TotalMilliseconds}ms");
        }

        /// <summary>
        /// Tests batch operation with transaction performance
        /// </summary>
        [TestMethod]
        public async Task ExecuteBatch_WithTransaction_PerformsWithinTimeLimit()
        {
            // Arrange
            var queries = new List<string>();
            for (int i = 1; i <= 500; i++)
            {
                queries.Add($"UPDATE Customers SET ModifiedDate = GETDATE() WHERE CustomerID = {i}");
            }

            var maxExecutionTime = TimeSpan.FromSeconds(10);
            var mockConnection = _testSetup.CreateMockConnection();
            _queryExecutor = new SqlQueryExecutor(mockConnection.Object, _mockLogger.Object);

            // Act
            var executionTime = await SqlQueryTestUtilities.MeasureExecutionTime(async () =>
            {
                await _queryExecutor.ExecuteBatchAsync(queries, useTransaction: true);
            });

            // Assert
            Assert.IsTrue(executionTime < maxExecutionTime, 
                $"Batch operation with transaction should execute within {maxExecutionTime.TotalMilliseconds}ms");
        }

        #endregion

        #region Stress Tests

        /// <summary>
        /// Tests system under high query load
        /// </summary>
        [TestMethod]
        public async Task ExecuteQuery_HighLoad_SystemRemainsStable()
        {
            // Arrange
            var query = "SELECT * FROM Customers WHERE IsActive = 1";
            var concurrentQueries = 50;
            var maxExecutionTime = TimeSpan.FromSeconds(30);

            var mockConnection = _testSetup.CreateMockConnection();
            _queryExecutor = new SqlQueryExecutor(mockConnection.Object, _mockLogger.Object);

            // Act
            var startTime = DateTime.UtcNow;
            
            var tasks = new List<Task<List<CustomerData>>>();
            for (int i = 0; i < concurrentQueries; i++)
            {
                tasks.Add(_queryExecutor.ExecuteQueryAsync<CustomerData>(query));
            }
            
            var results = await Task.WhenAll(tasks);
            var endTime = DateTime.UtcNow;
            var totalExecutionTime = endTime - startTime;

            // Assert
            Assert.IsTrue(totalExecutionTime < maxExecutionTime, 
                $"System should handle {concurrentQueries} concurrent queries within {maxExecutionTime.TotalSeconds} seconds");
            
            Assert.AreEqual(concurrentQueries, results.Length, "All queries should complete");
            Assert.IsTrue(results.All(result => result != null), "All results should be non-null");
        }

        /// <summary>
        /// Tests system stability with mixed query types
        /// </summary>
        [TestMethod]
        public async Task ExecuteQuery_MixedQueryTypes_SystemRemainsStable()
        {
            // Arrange
            var queries = new List<(string query, Dictionary<string, object> parameters)>
            {
                ("SELECT * FROM Customers WHERE IsActive = 1", null),
                ("SELECT * FROM Orders WHERE OrderDate >= @StartDate", new Dictionary<string, object> { ["StartDate"] = DateTime.Now.AddDays(-30) }),
                ("SELECT COUNT(*) FROM Products WHERE Discontinued = 0", null),
                ("INSERT INTO Customers (CustomerName, Email) VALUES (@Name, @Email)", new Dictionary<string, object> { ["Name"] = "Test", ["Email"] = "test@example.com" }),
                ("UPDATE Customers SET ModifiedDate = GETDATE() WHERE CustomerID = @ID", new Dictionary<string, object> { ["ID"] = 1 })
            };

            var maxExecutionTime = TimeSpan.FromSeconds(20);
            var mockConnection = _testSetup.CreateMockConnection();
            _queryExecutor = new SqlQueryExecutor(mockConnection.Object, _mockLogger.Object);

            // Act
            var startTime = DateTime.UtcNow;
            
            var tasks = new List<Task>();
            foreach (var (query, parameters) in queries)
            {
                if (query.StartsWith("SELECT"))
                {
                    tasks.Add(_queryExecutor.ExecuteQueryAsync<CustomerData>(query, parameters));
                }
                else
                {
                    tasks.Add(_queryExecutor.ExecuteNonQueryAsync(query, parameters));
                }
            }
            
            await Task.WhenAll(tasks);
            var endTime = DateTime.UtcNow;
            var totalExecutionTime = endTime - startTime;

            // Assert
            Assert.IsTrue(totalExecutionTime < maxExecutionTime, 
                $"System should handle mixed query types within {maxExecutionTime.TotalSeconds} seconds");
        }

        #endregion

        #region Performance Benchmarking

        /// <summary>
        /// Benchmarks query execution performance
        /// </summary>
        [TestMethod]
        public async Task ExecuteQuery_PerformanceBenchmark_MeetsBenchmarkCriteria()
        {
            // Arrange
            var query = "SELECT CustomerID, CustomerName, Email FROM Customers WHERE IsActive = 1";
            var iterations = 100;
            var maxAverageExecutionTime = TimeSpan.FromMilliseconds(100);

            var mockConnection = _testSetup.CreateMockConnection();
            _queryExecutor = new SqlQueryExecutor(mockConnection.Object, _mockLogger.Object);

            // Act
            var executionTimes = new List<TimeSpan>();
            
            for (int i = 0; i < iterations; i++)
            {
                var executionTime = await SqlQueryTestUtilities.MeasureExecutionTime(async () =>
                {
                    await _queryExecutor.ExecuteQueryAsync<CustomerData>(query);
                });
                executionTimes.Add(executionTime);
            }

            var averageExecutionTime = TimeSpan.FromMilliseconds(executionTimes.Average(et => et.TotalMilliseconds));

            // Assert
            Assert.IsTrue(averageExecutionTime < maxAverageExecutionTime, 
                $"Average execution time over {iterations} iterations should be less than {maxAverageExecutionTime.TotalMilliseconds}ms");
        }

        /// <summary>
        /// Benchmarks memory usage during repeated query execution
        /// </summary>
        [TestMethod]
        public async Task ExecuteQuery_MemoryBenchmark_MemoryUsageIsStable()
        {
            // Arrange
            var query = "SELECT * FROM Customers";
            var iterations = 1000;
            var maxMemoryIncreaseMB = 20;

            var mockConnection = _testSetup.CreateMockConnection();
            _queryExecutor = new SqlQueryExecutor(mockConnection.Object, _mockLogger.Object);

            // Act
            var initialMemory = GC.GetTotalMemory(false);
            
            for (int i = 0; i < iterations; i++)
            {
                await _queryExecutor.ExecuteQueryAsync<CustomerData>(query);
                
                // Force garbage collection every 100 iterations
                if (i % 100 == 0)
                {
                    GC.Collect();
                    GC.WaitForPendingFinalizers();
                }
            }
            
            var finalMemory = GC.GetTotalMemory(false);
            var memoryIncrease = (finalMemory - initialMemory) / (1024 * 1024); // Convert to MB

            // Assert
            Assert.IsTrue(memoryIncrease < maxMemoryIncreaseMB, 
                $"Memory usage after {iterations} iterations should not increase by more than {maxMemoryIncreaseMB}MB");
        }

        #endregion
    }

    #region Test Data Classes

    public class CustomerAggregateData
    {
        public int CustomerID { get; set; }
        public string CustomerName { get; set; }
        public int OrderCount { get; set; }
        public decimal TotalSpent { get; set; }
        public decimal AverageOrderValue { get; set; }
    }

    #endregion
}
