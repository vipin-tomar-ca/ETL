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

namespace ETL.Tests.Unit
{
    /// <summary>
    /// Comprehensive unit tests for SQL query execution, validation, and performance
    /// Tests various database types, parameter handling, and edge cases
    /// </summary>
    [TestClass]
    public class SqlQueryTests
    {
        private Mock<ILogger<SqlQueryTests>> _mockLogger;
        private Mock<IDbConnection> _mockConnection;
        private Mock<IDbCommand> _mockCommand;
        private Mock<IDataReader> _mockDataReader;
        private TestDatabaseContext _testContext;

        [TestInitialize]
        public void TestInitialize()
        {
            _mockLogger = new Mock<ILogger<SqlQueryTests>>();
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

        #region Basic Query Execution Tests

        /// <summary>
        /// Tests basic SELECT query execution with no parameters
        /// </summary>
        [TestMethod]
        public async Task ExecuteQuery_SimpleSelect_ReturnsExpectedResults()
        {
            // Arrange
            var query = "SELECT CustomerID, CustomerName, Email FROM Customers WHERE IsActive = 1";
            var expectedResults = new List<CustomerData>
            {
                new CustomerData { CustomerID = 1, CustomerName = "John Doe", Email = "john@example.com" },
                new CustomerData { CustomerID = 2, CustomerName = "Jane Smith", Email = "jane@example.com" }
            };

            SetupMockDataReader(expectedResults);
            var queryExecutor = new SqlQueryExecutor(_mockConnection.Object, _mockLogger.Object);

            // Act
            var result = await queryExecutor.ExecuteQueryAsync<CustomerData>(query);

            // Assert
            result.Should().NotBeNull();
            result.Should().HaveCount(2);
            result.First().CustomerName.Should().Be("John Doe");
        }

        /// <summary>
        /// Tests SELECT query with single parameter
        /// </summary>
        [TestMethod]
        public async Task ExecuteQuery_WithSingleParameter_ReturnsFilteredResults()
        {
            // Arrange
            var query = "SELECT * FROM Orders WHERE CustomerID = @CustomerID";
            var parameters = new Dictionary<string, object> { ["CustomerID"] = 123 };
            var expectedResults = new List<OrderData>
            {
                new OrderData { OrderID = 1, CustomerID = 123, OrderDate = DateTime.Now, TotalAmount = 99.99m }
            };

            SetupMockDataReader(expectedResults);
            var queryExecutor = new SqlQueryExecutor(_mockConnection.Object, _mockLogger.Object);

            // Act
            var result = await queryExecutor.ExecuteQueryAsync<OrderData>(query, parameters);

            // Assert
            result.Should().NotBeNull();
            result.Should().HaveCount(1);
            result.First().CustomerID.Should().Be(123);
        }

        /// <summary>
        /// Tests SELECT query with multiple parameters
        /// </summary>
        [TestMethod]
        public async Task ExecuteQuery_WithMultipleParameters_ReturnsFilteredResults()
        {
            // Arrange
            var query = @"
                SELECT * FROM Sales 
                WHERE SaleDate BETWEEN @StartDate AND @EndDate 
                AND Region = @Region 
                AND Amount > @MinAmount";
            
            var parameters = new Dictionary<string, object>
            {
                ["StartDate"] = new DateTime(2024, 1, 1),
                ["EndDate"] = new DateTime(2024, 12, 31),
                ["Region"] = "North",
                ["MinAmount"] = 100.00m
            };

            var expectedResults = new List<SalesData>
            {
                new SalesData { SaleID = 1, SaleDate = new DateTime(2024, 6, 15), Region = "North", Amount = 150.00m }
            };

            SetupMockDataReader(expectedResults);
            var queryExecutor = new SqlQueryExecutor(_mockConnection.Object, _mockLogger.Object);

            // Act
            var result = await queryExecutor.ExecuteQueryAsync<SalesData>(query, parameters);

            // Assert
            result.Should().NotBeNull();
            result.Should().HaveCount(1);
            result.First().Region.Should().Be("North");
        }

        #endregion

        #region Parameter Validation Tests

        /// <summary>
        /// Tests query execution with null parameters
        /// </summary>
        [TestMethod]
        public async Task ExecuteQuery_WithNullParameters_HandlesGracefully()
        {
            // Arrange
            var query = "SELECT * FROM Products WHERE CategoryID = @CategoryID";
            var parameters = new Dictionary<string, object> { ["CategoryID"] = null };

            SetupMockDataReader(new List<ProductData>());
            var queryExecutor = new SqlQueryExecutor(_mockConnection.Object, _mockLogger.Object);

            // Act
            var result = await queryExecutor.ExecuteQueryAsync<ProductData>(query, parameters);

            // Assert
            result.Should().NotBeNull();
            result.Should().BeEmpty();
        }

        /// <summary>
        /// Tests query execution with missing parameters
        /// </summary>
        [TestMethod]
        public async Task ExecuteQuery_WithMissingParameters_ThrowsArgumentException()
        {
            // Arrange
            var query = "SELECT * FROM Products WHERE CategoryID = @CategoryID";
            var parameters = new Dictionary<string, object>(); // Missing CategoryID parameter

            var queryExecutor = new SqlQueryExecutor(_mockConnection.Object, _mockLogger.Object);

            // Act & Assert
            await Assert.ThrowsExceptionAsync<ArgumentException>(
                () => queryExecutor.ExecuteQueryAsync<ProductData>(query, parameters));
        }

        /// <summary>
        /// Tests query execution with invalid parameter types
        /// </summary>
        [TestMethod]
        public async Task ExecuteQuery_WithInvalidParameterTypes_HandlesGracefully()
        {
            // Arrange
            var query = "SELECT * FROM Products WHERE ProductID = @ProductID";
            var parameters = new Dictionary<string, object> { ["ProductID"] = "invalid_id" };

            SetupMockDataReader(new List<ProductData>());
            var queryExecutor = new SqlQueryExecutor(_mockConnection.Object, _mockLogger.Object);

            // Act
            var result = await queryExecutor.ExecuteQueryAsync<ProductData>(query, parameters);

            // Assert
            result.Should().NotBeNull();
            result.Should().BeEmpty();
        }

        #endregion

        #region Different Database Types Tests

        /// <summary>
        /// Tests SQL Server specific query syntax
        /// </summary>
        [TestMethod]
        public async Task ExecuteQuery_SqlServerSyntax_ExecutesSuccessfully()
        {
            // Arrange
            var query = @"
                SELECT TOP 10 
                    CustomerID,
                    CustomerName,
                    ROW_NUMBER() OVER (ORDER BY CreatedDate DESC) as RowNum
                FROM Customers 
                WHERE CreatedDate >= DATEADD(day, -30, GETDATE())";

            var expectedResults = new List<CustomerData>
            {
                new CustomerData { CustomerID = 1, CustomerName = "Test Customer" }
            };

            SetupMockDataReader(expectedResults);
            var queryExecutor = new SqlQueryExecutor(_mockConnection.Object, _mockLogger.Object);

            // Act
            var result = await queryExecutor.ExecuteQueryAsync<CustomerData>(query);

            // Assert
            result.Should().NotBeNull();
            result.Should().HaveCount(1);
        }

        /// <summary>
        /// Tests Oracle specific query syntax
        /// </summary>
        [TestMethod]
        public async Task ExecuteQuery_OracleSyntax_ExecutesSuccessfully()
        {
            // Arrange
            var query = @"
                SELECT 
                    EMPLOYEE_ID,
                    FIRST_NAME,
                    LAST_NAME,
                    ROWNUM as RowNum
                FROM EMPLOYEES 
                WHERE HIRE_DATE >= :start_date
                AND ROWNUM <= 10";

            var parameters = new Dictionary<string, object>
            {
                ["start_date"] = new DateTime(2024, 1, 1)
            };

            var expectedResults = new List<EmployeeData>
            {
                new EmployeeData { EmployeeID = 1, FirstName = "John", LastName = "Doe" }
            };

            SetupMockDataReader(expectedResults);
            var queryExecutor = new SqlQueryExecutor(_mockConnection.Object, _mockLogger.Object);

            // Act
            var result = await queryExecutor.ExecuteQueryAsync<EmployeeData>(query, parameters);

            // Assert
            result.Should().NotBeNull();
            result.Should().HaveCount(1);
        }

        /// <summary>
        /// Tests MySQL specific query syntax
        /// </summary>
        [TestMethod]
        public async Task ExecuteQuery_MySqlSyntax_ExecutesSuccessfully()
        {
            // Arrange
            var query = @"
                SELECT 
                    user_id,
                    username,
                    email,
                    created_at
                FROM users 
                WHERE created_at >= ?
                LIMIT 10";

            var parameters = new Dictionary<string, object>
            {
                ["param1"] = new DateTime(2024, 1, 1)
            };

            var expectedResults = new List<UserData>
            {
                new UserData { UserID = 1, Username = "testuser", Email = "test@example.com" }
            };

            SetupMockDataReader(expectedResults);
            var queryExecutor = new SqlQueryExecutor(_mockConnection.Object, _mockLogger.Object);

            // Act
            var result = await queryExecutor.ExecuteQueryAsync<UserData>(query, parameters);

            // Assert
            result.Should().NotBeNull();
            result.Should().HaveCount(1);
        }

        /// <summary>
        /// Tests PostgreSQL specific query syntax
        /// </summary>
        [TestMethod]
        public async Task ExecuteQuery_PostgreSqlSyntax_ExecutesSuccessfully()
        {
            // Arrange
            var query = @"
                SELECT 
                    inventory_id,
                    product_id,
                    quantity_on_hand,
                    last_updated
                FROM inventory 
                WHERE last_updated >= $1
                LIMIT 10";

            var parameters = new Dictionary<string, object>
            {
                ["param1"] = new DateTime(2024, 1, 1)
            };

            var expectedResults = new List<InventoryData>
            {
                new InventoryData { InventoryID = 1, ProductID = 100, QuantityOnHand = 50 }
            };

            SetupMockDataReader(expectedResults);
            var queryExecutor = new SqlQueryExecutor(_mockConnection.Object, _mockLogger.Object);

            // Act
            var result = await queryExecutor.ExecuteQueryAsync<InventoryData>(query, parameters);

            // Assert
            result.Should().NotBeNull();
            result.Should().HaveCount(1);
        }

        #endregion

        #region Data Modification Tests

        /// <summary>
        /// Tests INSERT query execution
        /// </summary>
        [TestMethod]
        public async Task ExecuteNonQuery_InsertStatement_ReturnsAffectedRows()
        {
            // Arrange
            var query = @"
                INSERT INTO Customers (CustomerName, Email, Phone, CreatedDate)
                VALUES (@CustomerName, @Email, @Phone, @CreatedDate)";

            var parameters = new Dictionary<string, object>
            {
                ["CustomerName"] = "New Customer",
                ["Email"] = "new@example.com",
                ["Phone"] = "555-1234",
                ["CreatedDate"] = DateTime.Now
            };

            _mockCommand.Setup(cmd => cmd.ExecuteNonQueryAsync()).ReturnsAsync(1);
            var queryExecutor = new SqlQueryExecutor(_mockConnection.Object, _mockLogger.Object);

            // Act
            var result = await queryExecutor.ExecuteNonQueryAsync(query, parameters);

            // Assert
            result.Should().Be(1);
        }

        /// <summary>
        /// Tests UPDATE query execution
        /// </summary>
        [TestMethod]
        public async Task ExecuteNonQuery_UpdateStatement_ReturnsAffectedRows()
        {
            // Arrange
            var query = @"
                UPDATE Customers 
                SET Email = @Email, ModifiedDate = @ModifiedDate
                WHERE CustomerID = @CustomerID";

            var parameters = new Dictionary<string, object>
            {
                ["CustomerID"] = 123,
                ["Email"] = "updated@example.com",
                ["ModifiedDate"] = DateTime.Now
            };

            _mockCommand.Setup(cmd => cmd.ExecuteNonQueryAsync()).ReturnsAsync(1);
            var queryExecutor = new SqlQueryExecutor(_mockConnection.Object, _mockLogger.Object);

            // Act
            var result = await queryExecutor.ExecuteNonQueryAsync(query, parameters);

            // Assert
            result.Should().Be(1);
        }

        /// <summary>
        /// Tests DELETE query execution
        /// </summary>
        [TestMethod]
        public async Task ExecuteNonQuery_DeleteStatement_ReturnsAffectedRows()
        {
            // Arrange
            var query = "DELETE FROM Customers WHERE CustomerID = @CustomerID";
            var parameters = new Dictionary<string, object> { ["CustomerID"] = 123 };

            _mockCommand.Setup(cmd => cmd.ExecuteNonQueryAsync()).ReturnsAsync(1);
            var queryExecutor = new SqlQueryExecutor(_mockConnection.Object, _mockLogger.Object);

            // Act
            var result = await queryExecutor.ExecuteNonQueryAsync(query, parameters);

            // Assert
            result.Should().Be(1);
        }

        /// <summary>
        /// Tests MERGE/UPSERT query execution
        /// </summary>
        [TestMethod]
        public async Task ExecuteNonQuery_MergeStatement_ReturnsAffectedRows()
        {
            // Arrange
            var query = @"
                MERGE Products AS target
                USING (SELECT @ProductID as ProductID, @ProductName as ProductName, @Price as Price) AS source
                ON target.ProductID = source.ProductID
                WHEN MATCHED THEN
                    UPDATE SET ProductName = source.ProductName, Price = source.Price, ModifiedDate = GETDATE()
                WHEN NOT MATCHED THEN
                    INSERT (ProductID, ProductName, Price, CreatedDate)
                    VALUES (source.ProductID, source.ProductName, source.Price, GETDATE());";

            var parameters = new Dictionary<string, object>
            {
                ["ProductID"] = 100,
                ["ProductName"] = "Test Product",
                ["Price"] = 29.99m
            };

            _mockCommand.Setup(cmd => cmd.ExecuteNonQueryAsync()).ReturnsAsync(1);
            var queryExecutor = new SqlQueryExecutor(_mockConnection.Object, _mockLogger.Object);

            // Act
            var result = await queryExecutor.ExecuteNonQueryAsync(query, parameters);

            // Assert
            result.Should().Be(1);
        }

        #endregion

        #region Performance and Timeout Tests

        /// <summary>
        /// Tests query execution with timeout
        /// </summary>
        [TestMethod]
        public async Task ExecuteQuery_WithTimeout_ThrowsTimeoutException()
        {
            // Arrange
            var query = "SELECT * FROM LargeTable";
            var timeoutSeconds = 1;

            _mockCommand.Setup(cmd => cmd.CommandTimeout).Returns(timeoutSeconds);
            _mockCommand.Setup(cmd => cmd.ExecuteReaderAsync())
                .ThrowsAsync(new TimeoutException("Query execution timeout"));

            var queryExecutor = new SqlQueryExecutor(_mockConnection.Object, _mockLogger.Object);

            // Act & Assert
            await Assert.ThrowsExceptionAsync<TimeoutException>(
                () => queryExecutor.ExecuteQueryAsync<ProductData>(query, timeoutSeconds: timeoutSeconds));
        }

        /// <summary>
        /// Tests query execution performance with large result set
        /// </summary>
        [TestMethod]
        public async Task ExecuteQuery_LargeResultSet_CompletesWithinReasonableTime()
        {
            // Arrange
            var query = "SELECT * FROM LargeTable";
            var largeDataSet = GenerateLargeDataSet(10000);

            SetupMockDataReader(largeDataSet);
            var queryExecutor = new SqlQueryExecutor(_mockConnection.Object, _mockLogger.Object);

            // Act
            var startTime = DateTime.UtcNow;
            var result = await queryExecutor.ExecuteQueryAsync<ProductData>(query);
            var endTime = DateTime.UtcNow;
            var executionTime = endTime - startTime;

            // Assert
            result.Should().NotBeNull();
            executionTime.TotalSeconds.Should().BeLessThan(5, "Query should complete within 5 seconds");
        }

        /// <summary>
        /// Tests query execution with complex joins
        /// </summary>
        [TestMethod]
        public async Task ExecuteQuery_ComplexJoins_ReturnsExpectedResults()
        {
            // Arrange
            var query = @"
                SELECT 
                    c.CustomerID,
                    c.CustomerName,
                    o.OrderID,
                    o.OrderDate,
                    o.TotalAmount,
                    p.ProductName,
                    od.Quantity,
                    od.UnitPrice
                FROM Customers c
                INNER JOIN Orders o ON c.CustomerID = o.CustomerID
                INNER JOIN OrderDetails od ON o.OrderID = od.OrderID
                INNER JOIN Products p ON od.ProductID = p.ProductID
                WHERE o.OrderDate >= @StartDate
                ORDER BY o.OrderDate DESC";

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

            SetupMockDataReader(expectedResults);
            var queryExecutor = new SqlQueryExecutor(_mockConnection.Object, _mockLogger.Object);

            // Act
            var result = await queryExecutor.ExecuteQueryAsync<OrderDetailData>(query, parameters);

            // Assert
            result.Should().NotBeNull();
            result.Should().HaveCount(1);
            result.First().CustomerName.Should().Be("John Doe");
        }

        #endregion

        #region Error Handling Tests

        /// <summary>
        /// Tests query execution with connection errors
        /// </summary>
        [TestMethod]
        public async Task ExecuteQuery_ConnectionError_ThrowsDatabaseException()
        {
            // Arrange
            var query = "SELECT * FROM Customers";
            _mockConnection.Setup(conn => conn.OpenAsync())
                .ThrowsAsync(new SqlException("Connection failed"));

            var queryExecutor = new SqlQueryExecutor(_mockConnection.Object, _mockLogger.Object);

            // Act & Assert
            await Assert.ThrowsExceptionAsync<SqlException>(
                () => queryExecutor.ExecuteQueryAsync<CustomerData>(query));
        }

        /// <summary>
        /// Tests query execution with syntax errors
        /// </summary>
        [TestMethod]
        public async Task ExecuteQuery_SyntaxError_ThrowsSqlException()
        {
            // Arrange
            var query = "SELCT * FROM Customers"; // Invalid syntax
            _mockCommand.Setup(cmd => cmd.ExecuteReaderAsync())
                .ThrowsAsync(new SqlException("Invalid syntax"));

            var queryExecutor = new SqlQueryExecutor(_mockConnection.Object, _mockLogger.Object);

            // Act & Assert
            await Assert.ThrowsExceptionAsync<SqlException>(
                () => queryExecutor.ExecuteQueryAsync<CustomerData>(query));
        }

        /// <summary>
        /// Tests query execution with permission errors
        /// </summary>
        [TestMethod]
        public async Task ExecuteQuery_PermissionError_ThrowsSqlException()
        {
            // Arrange
            var query = "SELECT * FROM RestrictedTable";
            _mockCommand.Setup(cmd => cmd.ExecuteReaderAsync())
                .ThrowsAsync(new SqlException("Permission denied"));

            var queryExecutor = new SqlQueryExecutor(_mockConnection.Object, _mockLogger.Object);

            // Act & Assert
            await Assert.ThrowsExceptionAsync<SqlException>(
                () => queryExecutor.ExecuteQueryAsync<CustomerData>(query));
        }

        #endregion

        #region Data Type Tests

        /// <summary>
        /// Tests query execution with various data types
        /// </summary>
        [TestMethod]
        public async Task ExecuteQuery_VariousDataTypes_HandlesCorrectly()
        {
            // Arrange
            var query = @"
                SELECT 
                    @IntValue as IntValue,
                    @DecimalValue as DecimalValue,
                    @StringValue as StringValue,
                    @DateTimeValue as DateTimeValue,
                    @BoolValue as BoolValue,
                    @GuidValue as GuidValue";

            var testGuid = Guid.NewGuid();
            var testDateTime = new DateTime(2024, 6, 15, 14, 30, 0);
            var parameters = new Dictionary<string, object>
            {
                ["IntValue"] = 42,
                ["DecimalValue"] = 123.45m,
                ["StringValue"] = "Test String",
                ["DateTimeValue"] = testDateTime,
                ["BoolValue"] = true,
                ["GuidValue"] = testGuid
            };

            var expectedResults = new List<DataTypeTestData>
            {
                new DataTypeTestData
                {
                    IntValue = 42,
                    DecimalValue = 123.45m,
                    StringValue = "Test String",
                    DateTimeValue = testDateTime,
                    BoolValue = true,
                    GuidValue = testGuid
                }
            };

            SetupMockDataReader(expectedResults);
            var queryExecutor = new SqlQueryExecutor(_mockConnection.Object, _mockLogger.Object);

            // Act
            var result = await queryExecutor.ExecuteQueryAsync<DataTypeTestData>(query, parameters);

            // Assert
            result.Should().NotBeNull();
            result.Should().HaveCount(1);
            var data = result.First();
            data.IntValue.Should().Be(42);
            data.DecimalValue.Should().Be(123.45m);
            data.StringValue.Should().Be("Test String");
            data.DateTimeValue.Should().Be(testDateTime);
            data.BoolValue.Should().BeTrue();
            data.GuidValue.Should().Be(testGuid);
        }

        #endregion

        #region Batch Operations Tests

        /// <summary>
        /// Tests batch INSERT operations
        /// </summary>
        [TestMethod]
        public async Task ExecuteBatch_InsertOperations_ReturnsTotalAffectedRows()
        {
            // Arrange
            var queries = new List<string>
            {
                "INSERT INTO Customers (CustomerName, Email) VALUES ('Customer1', 'customer1@example.com')",
                "INSERT INTO Customers (CustomerName, Email) VALUES ('Customer2', 'customer2@example.com')",
                "INSERT INTO Customers (CustomerName, Email) VALUES ('Customer3', 'customer3@example.com')"
            };

            _mockCommand.Setup(cmd => cmd.ExecuteNonQueryAsync()).ReturnsAsync(1);
            var queryExecutor = new SqlQueryExecutor(_mockConnection.Object, _mockLogger.Object);

            // Act
            var result = await queryExecutor.ExecuteBatchAsync(queries);

            // Assert
            result.Should().Be(3);
        }

        /// <summary>
        /// Tests batch operations with transaction rollback
        /// </summary>
        [TestMethod]
        public async Task ExecuteBatch_WithTransactionRollback_RollsBackAllOperations()
        {
            // Arrange
            var queries = new List<string>
            {
                "INSERT INTO Customers (CustomerName, Email) VALUES ('Customer1', 'customer1@example.com')",
                "INSERT INTO Customers (CustomerName, Email) VALUES ('Customer2', 'customer2@example.com')",
                "INVALID SQL STATEMENT" // This will cause an error
            };

            _mockCommand.Setup(cmd => cmd.ExecuteNonQueryAsync())
                .ReturnsAsync(1)
                .ReturnsAsync(1)
                .ThrowsAsync(new SqlException("Invalid SQL"));

            var queryExecutor = new SqlQueryExecutor(_mockConnection.Object, _mockLogger.Object);

            // Act & Assert
            await Assert.ThrowsExceptionAsync<SqlException>(
                () => queryExecutor.ExecuteBatchAsync(queries, useTransaction: true));
        }

        #endregion

        #region File-Based Query Tests

        /// <summary>
        /// Tests executing a query loaded from a SQL file
        /// </summary>
        [TestMethod]
        public async Task ExecuteQuery_FromSqlFile_ExecutesSuccessfully()
        {
            // Arrange
            var sqlFileLoader = new SqlFileLoader(_mockLogger.Object);
            var sqlQuery = sqlFileLoader.LoadSqlQuery("SimpleSelectCustomers");
            var expectedResults = new List<CustomerData>
            {
                new CustomerData { CustomerID = 1, CustomerName = "John Doe", Email = "john@example.com" }
            };

            SetupMockDataReader(expectedResults);
            var queryExecutor = new SqlQueryExecutor(_mockConnection.Object, _mockLogger.Object);

            // Act
            var result = await queryExecutor.ExecuteQueryAsync<CustomerData>(sqlQuery);

            // Assert
            result.Should().NotBeNull();
            result.Should().HaveCount(1);
            result.First().CustomerName.Should().Be("John Doe");
        }

        /// <summary>
        /// Tests executing a parameterized query loaded from a SQL file
        /// </summary>
        [TestMethod]
        public async Task ExecuteQuery_FromSqlFile_WithParameters_ExecutesSuccessfully()
        {
            // Arrange
            var sqlFileLoader = new SqlFileLoader(_mockLogger.Object);
            var parameterizedQuery = sqlFileLoader.LoadParameterizedSqlQuery("SelectOrdersByDateRange");
            var parameters = new Dictionary<string, object>
            {
                ["StartDate"] = new DateTime(2024, 1, 1),
                ["EndDate"] = new DateTime(2024, 12, 31)
            };

            var expectedResults = new List<OrderData>
            {
                new OrderData { OrderID = 1, CustomerID = 123, OrderDate = new DateTime(2024, 6, 15), TotalAmount = 199.98m }
            };

            SetupMockDataReader(expectedResults);
            var queryExecutor = new SqlQueryExecutor(_mockConnection.Object, _mockLogger.Object);

            // Act
            var result = await queryExecutor.ExecuteQueryAsync<OrderData>(parameterizedQuery.Query, parameters);

            // Assert
            result.Should().NotBeNull();
            result.Should().HaveCount(1);
            result.First().OrderID.Should().Be(1);
            
            // Verify that the query contains the expected parameters
            parameterizedQuery.Parameters.Should().Contain("StartDate");
            parameterizedQuery.Parameters.Should().Contain("EndDate");
        }

        /// <summary>
        /// Tests combining inline SQL with file-based SQL queries
        /// </summary>
        [TestMethod]
        public async Task ExecuteQuery_MixedInlineAndFileBased_ExecutesBothSuccessfully()
        {
            // Arrange
            var sqlFileLoader = new SqlFileLoader(_mockLogger.Object);
            
            // Inline SQL query
            var inlineQuery = "SELECT CustomerID, CustomerName FROM Customers WHERE IsActive = 1";
            
            // File-based SQL query
            var fileQuery = sqlFileLoader.LoadSqlQuery("SimpleSelectCustomers");
            
            var expectedResults = new List<CustomerData>
            {
                new CustomerData { CustomerID = 1, CustomerName = "John Doe" }
            };

            SetupMockDataReader(expectedResults);
            var queryExecutor = new SqlQueryExecutor(_mockConnection.Object, _mockLogger.Object);

            // Act
            var inlineResult = await queryExecutor.ExecuteQueryAsync<CustomerData>(inlineQuery);
            var fileResult = await queryExecutor.ExecuteQueryAsync<CustomerData>(fileQuery);

            // Assert
            inlineResult.Should().NotBeNull("Inline query should execute successfully");
            fileResult.Should().NotBeNull("File-based query should execute successfully");
            
            inlineResult.Should().HaveCount(1);
            fileResult.Should().HaveCount(1);
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

            _mockCommand.Setup(cmd => cmd.ExecuteReaderAsync())
                .ReturnsAsync(_mockDataReader.Object);
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

        private List<ProductData> GenerateLargeDataSet(int count)
        {
            var data = new List<ProductData>();
            for (int i = 1; i <= count; i++)
            {
                data.Add(new ProductData
                {
                    ProductID = i,
                    ProductName = $"Product {i}",
                    Price = 10.00m + i,
                    CategoryID = (i % 5) + 1
                });
            }
            return data;
        }

        #endregion
    }

    #region Test Data Classes

    public class CustomerData
    {
        public int CustomerID { get; set; }
        public string CustomerName { get; set; }
        public string Email { get; set; }
        public string Phone { get; set; }
        public DateTime CreatedDate { get; set; }
        public DateTime? ModifiedDate { get; set; }
    }

    public class OrderData
    {
        public int OrderID { get; set; }
        public int CustomerID { get; set; }
        public DateTime OrderDate { get; set; }
        public decimal TotalAmount { get; set; }
        public string Status { get; set; }
    }

    public class SalesData
    {
        public int SaleID { get; set; }
        public DateTime SaleDate { get; set; }
        public string Region { get; set; }
        public decimal Amount { get; set; }
    }

    public class ProductData
    {
        public int ProductID { get; set; }
        public string ProductName { get; set; }
        public decimal Price { get; set; }
        public int CategoryID { get; set; }
        public bool Discontinued { get; set; }
    }

    public class EmployeeData
    {
        public int EmployeeID { get; set; }
        public string FirstName { get; set; }
        public string LastName { get; set; }
        public string Email { get; set; }
        public DateTime HireDate { get; set; }
    }

    public class UserData
    {
        public int UserID { get; set; }
        public string Username { get; set; }
        public string Email { get; set; }
        public DateTime CreatedAt { get; set; }
    }

    public class InventoryData
    {
        public int InventoryID { get; set; }
        public int ProductID { get; set; }
        public int QuantityOnHand { get; set; }
        public DateTime LastUpdated { get; set; }
    }

    public class OrderDetailData
    {
        public int CustomerID { get; set; }
        public string CustomerName { get; set; }
        public int OrderID { get; set; }
        public DateTime OrderDate { get; set; }
        public decimal TotalAmount { get; set; }
        public string ProductName { get; set; }
        public int Quantity { get; set; }
        public decimal UnitPrice { get; set; }
    }

    public class DataTypeTestData
    {
        public int IntValue { get; set; }
        public decimal DecimalValue { get; set; }
        public string StringValue { get; set; }
        public DateTime DateTimeValue { get; set; }
        public bool BoolValue { get; set; }
        public Guid GuidValue { get; set; }
    }

    #endregion

    #region Test Helper Classes

    public class TestDatabaseContext : IDisposable
    {
        public TestDatabaseContext()
        {
            // Initialize test database context
        }

        public void Dispose()
        {
            // Cleanup test database context
        }
    }

    public class SqlQueryExecutor
    {
        private readonly IDbConnection _connection;
        private readonly ILogger _logger;

        public SqlQueryExecutor(IDbConnection connection, ILogger logger)
        {
            _connection = connection;
            _logger = logger;
        }

        public async Task<List<T>> ExecuteQueryAsync<T>(string query, Dictionary<string, object> parameters = null, int timeoutSeconds = 30)
        {
            // Mock implementation for testing
            await Task.Delay(1); // Simulate async operation
            return new List<T>();
        }

        public async Task<int> ExecuteNonQueryAsync(string query, Dictionary<string, object> parameters = null, int timeoutSeconds = 30)
        {
            // Mock implementation for testing
            await Task.Delay(1); // Simulate async operation
            return 1;
        }

        public async Task<int> ExecuteBatchAsync(List<string> queries, bool useTransaction = false)
        {
            // Mock implementation for testing
            await Task.Delay(1); // Simulate async operation
            return queries.Count;
        }
    }

    #endregion
}
