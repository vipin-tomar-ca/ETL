using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Moq;

namespace ETL.Tests.Unit
{
    /// <summary>
    /// Utility classes and helper methods for SQL query testing
    /// Provides test data builders, mock setups, and common test scenarios
    /// </summary>
    public static class SqlQueryTestUtilities
    {
        #region Test Data Builders

        /// <summary>
        /// Builder pattern for creating test customer data
        /// </summary>
        public class CustomerDataBuilder
        {
            private CustomerData _customer = new CustomerData();

            public CustomerDataBuilder WithId(int id)
            {
                _customer.CustomerID = id;
                return this;
            }

            public CustomerDataBuilder WithName(string name)
            {
                _customer.CustomerName = name;
                return this;
            }

            public CustomerDataBuilder WithEmail(string email)
            {
                _customer.Email = email;
                return this;
            }

            public CustomerDataBuilder WithPhone(string phone)
            {
                _customer.Phone = phone;
                return this;
            }

            public CustomerDataBuilder WithCreatedDate(DateTime date)
            {
                _customer.CreatedDate = date;
                return this;
            }

            public CustomerDataBuilder WithModifiedDate(DateTime? date)
            {
                _customer.ModifiedDate = date;
                return this;
            }

            public CustomerData Build()
            {
                return _customer;
            }

            public static CustomerDataBuilder CreateDefault()
            {
                return new CustomerDataBuilder()
                    .WithId(1)
                    .WithName("Test Customer")
                    .WithEmail("test@example.com")
                    .WithPhone("555-1234")
                    .WithCreatedDate(DateTime.Now);
            }
        }

        /// <summary>
        /// Builder pattern for creating test order data
        /// </summary>
        public class OrderDataBuilder
        {
            private OrderData _order = new OrderData();

            public OrderDataBuilder WithId(int id)
            {
                _order.OrderID = id;
                return this;
            }

            public OrderDataBuilder WithCustomerId(int customerId)
            {
                _order.CustomerID = customerId;
                return this;
            }

            public OrderDataBuilder WithOrderDate(DateTime date)
            {
                _order.OrderDate = date;
                return this;
            }

            public OrderDataBuilder WithTotalAmount(decimal amount)
            {
                _order.TotalAmount = amount;
                return this;
            }

            public OrderDataBuilder WithStatus(string status)
            {
                _order.Status = status;
                return this;
            }

            public OrderData Build()
            {
                return _order;
            }

            public static OrderDataBuilder CreateDefault()
            {
                return new OrderDataBuilder()
                    .WithId(1)
                    .WithCustomerId(1)
                    .WithOrderDate(DateTime.Now)
                    .WithTotalAmount(99.99m)
                    .WithStatus("Pending");
            }
        }

        /// <summary>
        /// Builder pattern for creating test product data
        /// </summary>
        public class ProductDataBuilder
        {
            private ProductData _product = new ProductData();

            public ProductDataBuilder WithId(int id)
            {
                _product.ProductID = id;
                return this;
            }

            public ProductDataBuilder WithName(string name)
            {
                _product.ProductName = name;
                return this;
            }

            public ProductDataBuilder WithPrice(decimal price)
            {
                _product.Price = price;
                return this;
            }

            public ProductDataBuilder WithCategoryId(int categoryId)
            {
                _product.CategoryID = categoryId;
                return this;
            }

            public ProductDataBuilder WithDiscontinued(bool discontinued)
            {
                _product.Discontinued = discontinued;
                return this;
            }

            public ProductData Build()
            {
                return _product;
            }

            public static ProductDataBuilder CreateDefault()
            {
                return new ProductDataBuilder()
                    .WithId(1)
                    .WithName("Test Product")
                    .WithPrice(29.99m)
                    .WithCategoryId(1)
                    .WithDiscontinued(false);
            }
        }

        #endregion

        #region Mock Setup Helpers

        /// <summary>
        /// Sets up mock data reader with test data
        /// </summary>
        public static void SetupMockDataReader<T>(Mock<IDataReader> mockReader, List<T> testData)
        {
            var dataTable = ConvertToDataTable(testData);
            var dataReader = dataTable.CreateDataReader();
            var currentRow = -1;

            mockReader.Setup(reader => reader.Read())
                .Returns(() =>
                {
                    currentRow++;
                    return currentRow < testData.Count;
                });

            mockReader.Setup(reader => reader.GetOrdinal(It.IsAny<string>()))
                .Returns<string>(columnName => dataReader.GetOrdinal(columnName));

            mockReader.Setup(reader => reader.GetValue(It.IsAny<int>()))
                .Returns<int>(index => currentRow < testData.Count ? dataReader.GetValue(index) : null);

            mockReader.Setup(reader => reader.IsDBNull(It.IsAny<int>()))
                .Returns<int>(index => currentRow < testData.Count ? dataReader.IsDBNull(index) : true);

            mockReader.Setup(reader => reader.GetInt32(It.IsAny<int>()))
                .Returns<int>(index => currentRow < testData.Count ? dataReader.GetInt32(index) : 0);

            mockReader.Setup(reader => reader.GetString(It.IsAny<int>()))
                .Returns<int>(index => currentRow < testData.Count ? dataReader.GetString(index) : string.Empty);

            mockReader.Setup(reader => reader.GetDecimal(It.IsAny<int>()))
                .Returns<int>(index => currentRow < testData.Count ? dataReader.GetDecimal(index) : 0m);

            mockReader.Setup(reader => reader.GetDateTime(It.IsAny<int>()))
                .Returns<int>(index => currentRow < testData.Count ? dataReader.GetDateTime(index) : DateTime.MinValue);

            mockReader.Setup(reader => reader.GetBoolean(It.IsAny<int>()))
                .Returns<int>(index => currentRow < testData.Count ? dataReader.GetBoolean(index) : false);

            mockReader.Setup(reader => reader.GetGuid(It.IsAny<int>()))
                .Returns<int>(index => currentRow < testData.Count ? dataReader.GetGuid(index) : Guid.Empty);
        }

        /// <summary>
        /// Sets up mock database connection
        /// </summary>
        public static void SetupMockConnection(Mock<IDbConnection> mockConnection, Mock<IDbCommand> mockCommand)
        {
            mockConnection.Setup(conn => conn.CreateCommand()).Returns(mockCommand.Object);
            mockConnection.Setup(conn => conn.OpenAsync()).Returns(Task.CompletedTask);
            mockConnection.Setup(conn => conn.CloseAsync()).Returns(Task.CompletedTask);
        }

        /// <summary>
        /// Sets up mock database command
        /// </summary>
        public static void SetupMockCommand(Mock<IDbCommand> mockCommand, Mock<IDataReader> mockDataReader)
        {
            mockCommand.Setup(cmd => cmd.ExecuteReaderAsync()).ReturnsAsync(mockDataReader.Object);
            mockCommand.Setup(cmd => cmd.ExecuteNonQueryAsync()).ReturnsAsync(1);
            mockCommand.Setup(cmd => cmd.ExecuteScalarAsync()).ReturnsAsync(1);
        }

        #endregion

        #region Test Data Generators

        /// <summary>
        /// Generates a list of test customers
        /// </summary>
        public static List<CustomerData> GenerateTestCustomers(int count)
        {
            var customers = new List<CustomerData>();
            for (int i = 1; i <= count; i++)
            {
                customers.Add(new CustomerDataBuilder()
                    .WithId(i)
                    .WithName($"Customer {i}")
                    .WithEmail($"customer{i}@example.com")
                    .WithPhone($"555-{i:D4}")
                    .WithCreatedDate(DateTime.Now.AddDays(-i))
                    .Build());
            }
            return customers;
        }

        /// <summary>
        /// Generates a list of test orders
        /// </summary>
        public static List<OrderData> GenerateTestOrders(int count, int customerId = 1)
        {
            var orders = new List<OrderData>();
            for (int i = 1; i <= count; i++)
            {
                orders.Add(new OrderDataBuilder()
                    .WithId(i)
                    .WithCustomerId(customerId)
                    .WithOrderDate(DateTime.Now.AddDays(-i))
                    .WithTotalAmount(50.00m + (i * 10))
                    .WithStatus(i % 2 == 0 ? "Completed" : "Pending")
                    .Build());
            }
            return orders;
        }

        /// <summary>
        /// Generates a list of test products
        /// </summary>
        public static List<ProductData> GenerateTestProducts(int count)
        {
            var products = new List<ProductData>();
            for (int i = 1; i <= count; i++)
            {
                products.Add(new ProductDataBuilder()
                    .WithId(i)
                    .WithName($"Product {i}")
                    .WithPrice(10.00m + (i * 5))
                    .WithCategoryId((i % 5) + 1)
                    .WithDiscontinued(i % 10 == 0)
                    .Build());
            }
            return products;
        }

        /// <summary>
        /// Generates test parameters for SQL queries
        /// </summary>
        public static Dictionary<string, object> GenerateTestParameters()
        {
            return new Dictionary<string, object>
            {
                ["CustomerID"] = 123,
                ["StartDate"] = new DateTime(2024, 1, 1),
                ["EndDate"] = new DateTime(2024, 12, 31),
                ["MinAmount"] = 100.00m,
                ["Region"] = "North",
                ["IsActive"] = true,
                ["CategoryID"] = 5,
                ["ProductName"] = "Test Product",
                ["Email"] = "test@example.com",
                ["Phone"] = "555-1234"
            };
        }

        #endregion

        #region SQL Query Templates

        /// <summary>
        /// Common SQL query templates for testing
        /// </summary>
        public static class QueryTemplates
        {
            public const string SelectCustomers = "SELECT CustomerID, CustomerName, Email, Phone, CreatedDate FROM Customers WHERE IsActive = 1";
            
            public const string SelectCustomersWithParameter = "SELECT * FROM Customers WHERE CustomerID = @CustomerID";
            
            public const string SelectOrdersByDateRange = @"
                SELECT OrderID, CustomerID, OrderDate, TotalAmount, Status 
                FROM Orders 
                WHERE OrderDate BETWEEN @StartDate AND @EndDate";
            
            public const string SelectProductsByCategory = "SELECT * FROM Products WHERE CategoryID = @CategoryID AND Discontinued = 0";
            
            public const string InsertCustomer = @"
                INSERT INTO Customers (CustomerName, Email, Phone, CreatedDate)
                VALUES (@CustomerName, @Email, @Phone, @CreatedDate)";
            
            public const string UpdateCustomer = @"
                UPDATE Customers 
                SET Email = @Email, ModifiedDate = @ModifiedDate
                WHERE CustomerID = @CustomerID";
            
            public const string DeleteCustomer = "DELETE FROM Customers WHERE CustomerID = @CustomerID";
            
            public const string ComplexJoinQuery = @"
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
            
            public const string AggregateQuery = @"
                SELECT 
                    c.CustomerID,
                    c.CustomerName,
                    COUNT(o.OrderID) as OrderCount,
                    SUM(o.TotalAmount) as TotalSpent,
                    AVG(o.TotalAmount) as AverageOrderValue
                FROM Customers c
                LEFT JOIN Orders o ON c.CustomerID = o.CustomerID
                WHERE c.CreatedDate >= @StartDate
                GROUP BY c.CustomerID, c.CustomerName
                HAVING COUNT(o.OrderID) > @MinOrderCount";
        }

        #endregion

        #region Performance Test Helpers

        /// <summary>
        /// Measures query execution time
        /// </summary>
        public static async Task<TimeSpan> MeasureExecutionTime(Func<Task> action)
        {
            var startTime = DateTime.UtcNow;
            await action();
            var endTime = DateTime.UtcNow;
            return endTime - startTime;
        }

        /// <summary>
        /// Validates query execution time is within acceptable limits
        /// </summary>
        public static void ValidateExecutionTime(TimeSpan executionTime, TimeSpan maxAllowedTime)
        {
            if (executionTime > maxAllowedTime)
            {
                throw new TimeoutException($"Query execution time {executionTime.TotalMilliseconds}ms exceeded maximum allowed time {maxAllowedTime.TotalMilliseconds}ms");
            }
        }

        #endregion

        #region Data Validation Helpers

        /// <summary>
        /// Validates that query results contain expected data
        /// </summary>
        public static void ValidateQueryResults<T>(List<T> results, int expectedCount, Func<T, bool> validationPredicate = null)
        {
            if (results == null)
                throw new ArgumentNullException(nameof(results));

            if (results.Count != expectedCount)
                throw new InvalidOperationException($"Expected {expectedCount} results, but got {results.Count}");

            if (validationPredicate != null)
            {
                foreach (var result in results)
                {
                    if (!validationPredicate(result))
                        throw new InvalidOperationException("Query result failed validation");
                }
            }
        }

        /// <summary>
        /// Validates that query parameters are correctly set
        /// </summary>
        public static void ValidateQueryParameters(Mock<IDbCommand> mockCommand, Dictionary<string, object> expectedParameters)
        {
            mockCommand.Verify(cmd => cmd.Parameters.Add(It.IsAny<IDbDataParameter>()), 
                Times.Exactly(expectedParameters.Count));

            foreach (var parameter in expectedParameters)
            {
                mockCommand.Verify(cmd => cmd.Parameters.Add(It.Is<IDbDataParameter>(p => 
                    p.ParameterName == parameter.Key && p.Value == parameter.Value)), 
                    Times.Once);
            }
        }

        #endregion

        #region Helper Methods

        private static DataTable ConvertToDataTable<T>(List<T> data)
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

    #region Test Data Classes (if not already defined)

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

    public class ProductData
    {
        public int ProductID { get; set; }
        public string ProductName { get; set; }
        public decimal Price { get; set; }
        public int CategoryID { get; set; }
        public bool Discontinued { get; set; }
    }

    #endregion
}
