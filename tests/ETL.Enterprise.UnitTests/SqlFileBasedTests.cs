using Microsoft.VisualStudio.TestTools.UnitTesting;
using Microsoft.Extensions.Logging;
using Moq;
using System;
using System.Collections.Generic;
using System.Data;
using System.Threading.Tasks;
using System.IO;
using ETL.Enterprise.Domain.Entities;
using ETL.Enterprise.Domain.Enums;

namespace ETL.Tests.Unit
{
    /// <summary>
    /// Unit tests that demonstrate how to use SQL queries from external files
    /// Supports both inline SQL and file-based SQL queries
    /// </summary>
    [TestClass]
    public class SqlFileBasedTests
    {
        private Mock<ILogger<SqlFileBasedTests>> _mockLogger;
        private Mock<ILogger<SqlFileLoader>> _mockFileLoaderLogger;
        private SqlFileLoader _sqlFileLoader;
        private DatabaseTestSetup _testSetup;
        private SqlQueryExecutor _queryExecutor;

        [TestInitialize]
        public void TestInitialize()
        {
            _mockLogger = new Mock<ILogger<SqlFileBasedTests>>();
            _mockFileLoaderLogger = new Mock<ILogger<SqlFileLoader>>();
            _testSetup = new DatabaseTestSetup();
            
            // Initialize SQL file loader with test SQL files directory
            var sqlFilesDirectory = Path.Combine(AppDomain.CurrentDomain.BaseDirectory, "SqlFiles");
            _sqlFileLoader = new SqlFileLoader(_mockFileLoaderLogger.Object, sqlFilesDirectory);
        }

        [TestCleanup]
        public void TestCleanup()
        {
            _testSetup?.Dispose();
        }

        #region File-Based Query Execution Tests

        /// <summary>
        /// Tests executing a simple SELECT query loaded from a SQL file
        /// </summary>
        [TestMethod]
        public async Task ExecuteQuery_FromSqlFile_SimpleSelect_ReturnsExpectedResults()
        {
            // Arrange
            var mockConnection = _testSetup.CreateMockConnection();
            _queryExecutor = new SqlQueryExecutor(mockConnection.Object, _mockLogger.Object);
            
            // Load SQL query from file
            var sqlQuery = _sqlFileLoader.LoadSqlQuery("SimpleSelectCustomers");
            var expectedResults = new List<CustomerData>
            {
                new CustomerData { CustomerID = 1, CustomerName = "John Doe", Email = "john@example.com" }
            };

            SqlQueryTestUtilities.SetupMockDataReader(_testSetup.CreateMockDataReader(expectedResults), expectedResults);

            // Act
            var result = await _queryExecutor.ExecuteQueryAsync<CustomerData>(sqlQuery);

            // Assert
            Assert.IsNotNull(result);
            Assert.AreEqual(1);
            result.First().CustomerNameAssert.AreEqual("John Doe");
        }

        /// <summary>
        /// Tests executing a parameterized query loaded from a SQL file
        /// </summary>
        [TestMethod]
        public async Task ExecuteQuery_FromSqlFile_WithParameters_ReturnsFilteredResults()
        {
            // Arrange
            var mockConnection = _testSetup.CreateMockConnection();
            _queryExecutor = new SqlQueryExecutor(mockConnection.Object, _mockLogger.Object);
            
            // Load parameterized SQL query from file
            var parameterizedQuery = _sqlFileLoader.LoadParameterizedSqlQuery("SelectOrdersByDateRange");
            var parameters = new Dictionary<string, object>
            {
                ["StartDate"] = new DateTime(2024, 1, 1),
                ["EndDate"] = new DateTime(2024, 12, 31)
            };

            var expectedResults = new List<OrderData>
            {
                new OrderData { OrderID = 1, CustomerID = 123, OrderDate = new DateTime(2024, 6, 15), TotalAmount = 199.98m }
            };

            SqlQueryTestUtilities.SetupMockDataReader(_testSetup.CreateMockDataReader(expectedResults), expectedResults);

            // Act
            var result = await _queryExecutor.ExecuteQueryAsync<OrderData>(parameterizedQuery.Query, parameters);

            // Assert
            Assert.IsNotNull(result);
            Assert.AreEqual(1);
            result.First().OrderIDAssert.AreEqual(1);
            
            // Verify that the query contains the expected parameters
            parameterizedQuery.ParametersAssert.IsTrue(parameterizedQuery.Parameters.Contains("StartDate");
            parameterizedQuery.ParametersAssert.IsTrue(parameterizedQuery.Parameters.Contains("EndDate");
        }

        /// <summary>
        /// Tests executing a complex JOIN query loaded from a SQL file
        /// </summary>
        [TestMethod]
        public async Task ExecuteQuery_FromSqlFile_ComplexJoin_ReturnsExpectedResults()
        {
            // Arrange
            var mockConnection = _testSetup.CreateMockConnection();
            _queryExecutor = new SqlQueryExecutor(mockConnection.Object, _mockLogger.Object);
            
            // Load complex query from file
            var sqlQuery = _sqlFileLoader.LoadSqlQuery("ComplexOrderDetailsJoin");
            var parameters = new Dictionary<string, object>
            {
                ["StartDate"] = new DateTime(2024, 1, 1)
            };

            var expectedResults = new List<OrderDetailData>
            {
                new OrderDetailData
                {
                    CustomerID = 1,
                    CustomerName = "John Doe",
                    OrderID = 100,
                    OrderDate = new DateTime(2024, 6, 15),
                    TotalAmount = 199.98m,
                    ProductName = "Test Product",
                    Quantity = 2,
                    UnitPrice = 99.99m
                }
            };

            SqlQueryTestUtilities.SetupMockDataReader(_testSetup.CreateMockDataReader(expectedResults), expectedResults);

            // Act
            var result = await _queryExecutor.ExecuteQueryAsync<OrderDetailData>(sqlQuery, parameters);

            // Assert
            Assert.IsNotNull(result);
            Assert.AreEqual(1);
            result.First().CustomerNameAssert.AreEqual("John Doe");
        }

        /// <summary>
        /// Tests executing multiple queries from a single SQL file
        /// </summary>
        [TestMethod]
        public async Task ExecuteQuery_FromSqlFile_MultipleQueries_ExecutesAllSuccessfully()
        {
            // Arrange
            var mockConnection = _testSetup.CreateMockConnection();
            _queryExecutor = new SqlQueryExecutor(mockConnection.Object, _mockLogger.Object);
            
            // Load multiple queries from file
            var queries = _sqlFileLoader.LoadMultipleSqlQueries("MultipleQueries", "GO");
            
            // Act & Assert
            queriesAssert.IsTrue(result.Count > 0, );
            
            foreach (var queryInfo in queries)
            {
                var result = await _queryExecutor.ExecuteQueryAsync<CustomerData>(queryInfo.Query);
                resultAssert.IsNotNull($"Query {queryInfo.Index} should execute successfully");
            }
        }

        #endregion

        #region Metadata-Based Query Tests

        /// <summary>
        /// Tests executing a query with metadata loaded from a SQL file
        /// </summary>
        [TestMethod]
        public async Task ExecuteQuery_FromSqlFile_WithMetadata_ExecutesWithCorrectConfiguration()
        {
            // Arrange
            var mockConnection = _testSetup.CreateMockConnection();
            _queryExecutor = new SqlQueryExecutor(mockConnection.Object, _mockLogger.Object);
            
            // Load query with metadata from file
            var queryWithMetadata = _sqlFileLoader.LoadSqlQueryWithMetadata("CustomerAggregateQuery");
            
            var expectedResults = new List<CustomerAggregateData>
            {
                new CustomerAggregateData
                {
                    CustomerID = 1,
                    CustomerName = "John Doe",
                    OrderCount = 5,
                    TotalSpent = 1500.00m,
                    AverageOrderValue = 300.00m
                }
            };

            SqlQueryTestUtilities.SetupMockDataReader(_testSetup.CreateMockDataReader(expectedResults), expectedResults);

            // Act
            var result = await _queryExecutor.ExecuteQueryAsync<CustomerAggregateData>(
                queryWithMetadata.Query, 
                timeoutSeconds: queryWithMetadata.Metadata.TimeoutSeconds ?? 30);

            // Assert
            Assert.IsNotNull(result);
            Assert.AreEqual(1);
            
            // Verify metadata
            Assert.IsNotNull(queryWithMetadata.Metadata);
            queryWithMetadata.Metadata.NameAssert.IsFalse(string.IsNullOrEmpty(queryWithMetadata.Metadata.Name), );
            queryWithMetadata.Metadata.DescriptionAssert.IsFalse(string.IsNullOrEmpty(queryWithMetadata.Metadata.Name), );
            Assert.AreEqual("SELECT");
        }

        /// <summary>
        /// Tests executing queries from a directory of SQL files
        /// </summary>
        [TestMethod]
        public async Task ExecuteQuery_FromSqlDirectory_ExecutesAllQueriesSuccessfully()
        {
            // Arrange
            var mockConnection = _testSetup.CreateMockConnection();
            _queryExecutor = new SqlQueryExecutor(mockConnection.Object, _mockLogger.Object);
            
            // Load all queries from a directory
            var queries = _sqlFileLoader.LoadSqlQueriesFromDirectory("CustomerQueries");
            
            // Act & Assert
            queriesAssert.IsTrue(result.Count > 0, );
            
            foreach (var kvp in queries)
            {
                var fileName = kvp.Key;
                var sqlQuery = kvp.Value;
                
                var result = await _queryExecutor.ExecuteQueryAsync<CustomerData>(sqlQuery);
                resultAssert.IsNotNull($"Query from file {fileName} should execute successfully");
            }
        }

        #endregion

        #region Mixed Inline and File-Based Tests

        /// <summary>
        /// Tests combining inline SQL with file-based SQL queries
        /// </summary>
        [TestMethod]
        public async Task ExecuteQuery_MixedInlineAndFileBased_ExecutesBothSuccessfully()
        {
            // Arrange
            var mockConnection = _testSetup.CreateMockConnection();
            _queryExecutor = new SqlQueryExecutor(mockConnection.Object, _mockLogger.Object);
            
            // Inline SQL query
            var inlineQuery = "SELECT CustomerID, CustomerName FROM Customers WHERE IsActive = 1";
            
            // File-based SQL query
            var fileQuery = _sqlFileLoader.LoadSqlQuery("SelectActiveCustomers");
            
            var expectedResults = new List<CustomerData>
            {
                new CustomerData { CustomerID = 1, CustomerName = "John Doe" }
            };

            SqlQueryTestUtilities.SetupMockDataReader(_testSetup.CreateMockDataReader(expectedResults), expectedResults);

            // Act
            var inlineResult = await _queryExecutor.ExecuteQueryAsync<CustomerData>(inlineQuery);
            var fileResult = await _queryExecutor.ExecuteQueryAsync<CustomerData>(fileQuery);

            // Assert
            inlineResultAssert.IsNotNull("Inline query should execute successfully");
            fileResultAssert.IsNotNull("File-based query should execute successfully");
            
            inlineResultAssert.AreEqual(1);
            fileResultAssert.AreEqual(1);
        }

        /// <summary>
        /// Tests using file-based queries with inline parameters
        /// </summary>
        [TestMethod]
        public async Task ExecuteQuery_FileBasedWithInlineParameters_ExecutesSuccessfully()
        {
            // Arrange
            var mockConnection = _testSetup.CreateMockConnection();
            _queryExecutor = new SqlQueryExecutor(mockConnection.Object, _mockLogger.Object);
            
            // Load query from file
            var sqlQuery = _sqlFileLoader.LoadSqlQuery("SelectOrdersByCustomer");
            
            // Define parameters inline
            var parameters = new Dictionary<string, object>
            {
                ["CustomerID"] = 123,
                ["OrderStatus"] = "Completed"
            };

            var expectedResults = new List<OrderData>
            {
                new OrderData { OrderID = 1, CustomerID = 123, Status = "Completed" }
            };

            SqlQueryTestUtilities.SetupMockDataReader(_testSetup.CreateMockDataReader(expectedResults), expectedResults);

            // Act
            var result = await _queryExecutor.ExecuteQueryAsync<OrderData>(sqlQuery, parameters);

            // Assert
            Assert.IsNotNull(result);
            Assert.AreEqual(1);
            result.First().CustomerIDAssert.AreEqual(123);
        }

        #endregion

        #region SQL File Validation Tests

        /// <summary>
        /// Tests SQL file validation
        /// </summary>
        [TestMethod]
        public void ValidateSqlFile_ValidFile_ReturnsValidResult()
        {
            // Act
            var validationResult = _sqlFileLoader.ValidateSqlFile("SimpleSelectCustomers");

            // Assert
            validationResultAssert.IsNotNull();
            validationResult.IsValidAssert.IsTrue();
            validationResult.ErrorsAssert.AreEqual(0, result.Count, );
        }

        /// <summary>
        /// Tests SQL file validation with invalid syntax
        /// </summary>
        [TestMethod]
        public void ValidateSqlFile_InvalidSyntax_ReturnsInvalidResult()
        {
            // Act
            var validationResult = _sqlFileLoader.ValidateSqlFile("InvalidSyntaxQuery");

            // Assert
            validationResultAssert.IsNotNull();
            validationResult.IsValidAssert.IsFalse();
            validationResult.ErrorsAssert.IsTrue(result.Count > 0, );
        }

        #endregion

        #region Performance Tests with File-Based Queries

        /// <summary>
        /// Tests performance of file-based queries
        /// </summary>
        [TestMethod]
        public async Task ExecuteQuery_FileBased_PerformsWithinTimeLimit()
        {
            // Arrange
            var mockConnection = _testSetup.CreateMockConnection();
            _queryExecutor = new SqlQueryExecutor(mockConnection.Object, _mockLogger.Object);
            
            var sqlQuery = _sqlFileLoader.LoadSqlQuery("PerformanceTestQuery");
            var maxExecutionTime = TimeSpan.FromSeconds(5);

            // Act
            var executionTime = await SqlQueryTestUtilities.MeasureExecutionTime(async () =>
            {
                await _queryExecutor.ExecuteQueryAsync<CustomerData>(sqlQuery);
            });

            // Assert
            executionTimeAssert.IsTrue(executionTime < maxExecutionTime, 
                "File-based query should execute within time limit");
        }

        #endregion

        #region Error Handling Tests

        /// <summary>
        /// Tests error handling when SQL file is not found
        /// </summary>
        [TestMethod]
        public void LoadSqlQuery_FileNotFound_ThrowsFileNotFoundException()
        {
            // Act & Assert
            Assert.ThrowsException<FileNotFoundException>(() =>
            {
                _sqlFileLoader.LoadSqlQuery("NonExistentFile");
            });
        }

        /// <summary>
        /// Tests error handling when SQL directory is not found
        /// </summary>
        [TestMethod]
        public void LoadSqlQueriesFromDirectory_DirectoryNotFound_ThrowsDirectoryNotFoundException()
        {
            // Act & Assert
            Assert.ThrowsException<DirectoryNotFoundException>(() =>
            {
                _sqlFileLoader.LoadSqlQueriesFromDirectory("NonExistentDirectory");
            });
        }

        #endregion
    }
}
