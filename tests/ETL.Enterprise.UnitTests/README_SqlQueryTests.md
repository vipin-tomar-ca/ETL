# SQL Query Unit Tests

This directory contains comprehensive unit tests for SQL query execution, validation, and performance testing in the ETL system.

## Overview

The SQL query testing framework provides:

- **Comprehensive Query Testing**: Tests for various SQL operations (SELECT, INSERT, UPDATE, DELETE, MERGE)
- **Multi-Database Support**: Tests for SQL Server, Oracle, MySQL, and PostgreSQL syntax
- **Parameter Validation**: Tests for various parameter types and edge cases
- **Performance Testing**: Load testing, memory usage monitoring, and timeout validation
- **Error Handling**: Tests for connection errors, syntax errors, and permission issues
- **Mock Framework**: Complete database mocking for isolated unit testing

## Test Classes

### 1. SqlQueryTests.cs
Main test class containing comprehensive SQL query tests:

- **Basic Query Execution**: Simple SELECT, INSERT, UPDATE, DELETE operations
- **Parameter Handling**: Single and multiple parameter tests
- **Database Type Tests**: SQL Server, Oracle, MySQL, PostgreSQL specific syntax
- **Data Modification**: INSERT, UPDATE, DELETE, MERGE operations
- **Error Handling**: Connection errors, syntax errors, permission errors
- **Data Type Tests**: Various data types (int, decimal, string, DateTime, bool, Guid)
- **Batch Operations**: Batch INSERT/UPDATE operations with transaction support

### 2. SqlQueryTestUtilities.cs
Utility classes and helper methods:

- **Test Data Builders**: Builder pattern for creating test data
- **Mock Setup Helpers**: Methods for setting up database mocks
- **Test Data Generators**: Methods for generating test datasets
- **SQL Query Templates**: Common query templates for testing
- **Performance Test Helpers**: Execution time measurement and validation
- **Data Validation Helpers**: Result validation and parameter verification

### 3. DatabaseTestSetup.cs
Database test setup and mocking utilities:

- **In-Memory Database**: SQLite-based test database creation
- **Mock Connections**: Complete database connection mocking
- **Schema Creation**: Various database schemas for testing
- **Test Data Scenarios**: Different test data scenarios (large datasets, invalid data, etc.)
- **Error Simulation**: Database error simulation for testing error handling

### 4. SqlQueryPerformanceTests.cs
Performance and load testing:

- **Execution Time Tests**: Query execution time validation
- **Large Dataset Tests**: Performance with large result sets
- **Memory Usage Tests**: Memory consumption monitoring
- **Timeout Tests**: Query timeout handling
- **Batch Operation Tests**: Batch operation performance
- **Stress Tests**: High load and concurrent query testing
- **Performance Benchmarking**: Performance benchmarking and criteria validation

### 5. SqlFileLoader.cs
Utility class for loading and parsing SQL queries from external files:

- **Single Query Loading**: Load individual SQL queries from files
- **Multiple Query Loading**: Load multiple queries from a single file
- **Directory Loading**: Load all SQL files from a directory
- **Parameterized Queries**: Extract parameters from SQL queries
- **Metadata Support**: Parse SQL file metadata (name, description, timeout, etc.)
- **SQL Validation**: Basic SQL syntax validation
- **File Management**: Handle file paths and directory structures

### 6. SqlFileBasedTests.cs
Unit tests that demonstrate file-based SQL query testing:

- **File-Based Query Execution**: Execute queries loaded from SQL files
- **Parameterized File Queries**: Use file-based queries with parameters
- **Metadata-Based Testing**: Use SQL file metadata for test configuration
- **Directory-Based Testing**: Test queries from entire directories
- **Mixed Testing**: Combine inline and file-based queries
- **File Validation**: Test SQL file validation and error handling

## Usage Examples

### Basic Query Testing

```csharp
[TestMethod]
public async Task ExecuteQuery_SimpleSelect_ReturnsExpectedResults()
{
    // Arrange
    var query = "SELECT CustomerID, CustomerName, Email FROM Customers WHERE IsActive = 1";
    var expectedResults = new List<CustomerData>
    {
        new CustomerData { CustomerID = 1, CustomerName = "John Doe", Email = "john@example.com" }
    };

    SetupMockDataReader(expectedResults);
    var queryExecutor = new SqlQueryExecutor(_mockConnection.Object, _mockLogger.Object);

    // Act
    var result = await queryExecutor.ExecuteQueryAsync<CustomerData>(query);

    // Assert
    result.Should().NotBeNull();
    result.Should().HaveCount(1);
    result.First().CustomerName.Should().Be("John Doe");
}
```

### Parameter Testing

```csharp
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

    // Act & Assert
    var result = await queryExecutor.ExecuteQueryAsync<SalesData>(query, parameters);
    // ... assertions
}
```

### Performance Testing

```csharp
[TestMethod]
public async Task ExecuteQuery_LargeResultSet_PerformsWithinTimeLimit()
{
    // Arrange
    var query = "SELECT * FROM Customers ORDER BY CustomerID";
    var maxExecutionTime = TimeSpan.FromSeconds(10);
    var largeDataSet = SqlQueryTestUtilities.GenerateTestCustomers(10000);

    // Act
    var executionTime = await SqlQueryTestUtilities.MeasureExecutionTime(async () =>
    {
        await queryExecutor.ExecuteQueryAsync<CustomerData>(query);
    });

    // Assert
    executionTime.Should().BeLessThan(maxExecutionTime);
}
```

### Using Test Data Builders

```csharp
var customer = new CustomerDataBuilder()
    .WithId(1)
    .WithName("John Doe")
    .WithEmail("john@example.com")
    .WithPhone("555-1234")
    .WithCreatedDate(DateTime.Now)
    .Build();
```

### File-Based SQL Query Testing

```csharp
[TestMethod]
public async Task ExecuteQuery_FromSqlFile_ExecutesSuccessfully()
{
    // Arrange
    var sqlFileLoader = new SqlFileLoader(_mockLogger.Object);
    var sqlQuery = sqlFileLoader.LoadSqlQuery("SimpleSelectCustomers");
    
    // Act
    var result = await queryExecutor.ExecuteQueryAsync<CustomerData>(sqlQuery);
    
    // Assert
    result.Should().NotBeNull();
}
```

### Parameterized File-Based Queries

```csharp
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
    
    // Act
    var result = await queryExecutor.ExecuteQueryAsync<OrderData>(parameterizedQuery.Query, parameters);
    
    // Assert
    result.Should().NotBeNull();
}
```

### Loading Multiple Queries from a File

```csharp
[TestMethod]
public async Task ExecuteQuery_FromSqlFile_MultipleQueries_ExecutesAllSuccessfully()
{
    // Arrange
    var sqlFileLoader = new SqlFileLoader(_mockLogger.Object);
    var queries = sqlFileLoader.LoadMultipleSqlQueries("MultipleQueries", "GO");
    
    // Act & Assert
    foreach (var queryInfo in queries)
    {
        var result = await queryExecutor.ExecuteQueryAsync<CustomerData>(queryInfo.Query);
        result.Should().NotBeNull();
    }
}
```

### Using SQL File Metadata

```csharp
[TestMethod]
public async Task ExecuteQuery_FromSqlFile_WithMetadata_ExecutesWithCorrectConfiguration()
{
    // Arrange
    var sqlFileLoader = new SqlFileLoader(_mockLogger.Object);
    var queryWithMetadata = sqlFileLoader.LoadSqlQueryWithMetadata("CustomerAggregateQuery");
    
    // Act
    var result = await queryExecutor.ExecuteQueryAsync<CustomerAggregateData>(
        queryWithMetadata.Query, 
        timeoutSeconds: queryWithMetadata.Metadata.TimeoutSeconds ?? 30);
    
    // Assert
    result.Should().NotBeNull();
    queryWithMetadata.Metadata.QueryType.Should().Be("SELECT");
}
```

### Database Setup for Testing

```csharp
[TestInitialize]
public void TestInitialize()
{
    _testSetup = new DatabaseTestSetup();
    _connection = await _testSetup.CreateTestDatabaseWithSchemaAsync(DatabaseSchema.CustomerOrders);
}

[TestCleanup]
public void TestCleanup()
{
    _testSetup?.Dispose();
}
```

## Test Data Classes

The framework includes several test data classes:

- **CustomerData**: Customer information
- **OrderData**: Order information
- **ProductData**: Product information
- **SalesData**: Sales information
- **EmployeeData**: Employee information
- **UserData**: User information
- **InventoryData**: Inventory information
- **OrderDetailData**: Order detail information
- **DataTypeTestData**: Various data types for testing

## SQL File Structure

SQL files can contain metadata comments that provide additional information about the query:

```sql
--@name: SimpleSelectCustomers
--@description: Simple SELECT query to retrieve active customers
--@type: SELECT
--@timeout: 30
--@database: SQL Server
--@author: ETL Team
--@version: 1.0
--@tags: customer,select,active

SELECT 
    CustomerID,
    CustomerName,
    Email,
    Phone,
    CreatedDate,
    ModifiedDate
FROM Customers 
WHERE IsActive = 1
ORDER BY CustomerName;
```

### Supported Metadata Fields

- **@name**: Query name/identifier
- **@description**: Description of what the query does
- **@type**: Query type (SELECT, INSERT, UPDATE, DELETE, etc.)
- **@timeout**: Timeout in seconds
- **@database**: Target database type (SQL Server, Oracle, MySQL, PostgreSQL)
- **@author**: Query author
- **@version**: Query version
- **@tags**: Comma-separated tags for categorization

### Multiple Queries in a Single File

SQL files can contain multiple queries separated by delimiters (default: "GO"):

```sql
-- Query 1: Select active customers
SELECT CustomerID, CustomerName, Email 
FROM Customers 
WHERE IsActive = 1;

GO

-- Query 2: Select recent orders
SELECT OrderID, CustomerID, OrderDate, TotalAmount 
FROM Orders 
WHERE OrderDate >= DATEADD(day, -30, GETDATE());
```

## Query Templates

Common SQL query templates are available in `SqlQueryTestUtilities.QueryTemplates`:

- `SelectCustomers`: Basic customer selection
- `SelectCustomersWithParameter`: Parameterized customer selection
- `SelectOrdersByDateRange`: Date range order selection
- `SelectProductsByCategory`: Category-based product selection
- `InsertCustomer`: Customer insertion
- `UpdateCustomer`: Customer update
- `DeleteCustomer`: Customer deletion
- `ComplexJoinQuery`: Complex multi-table join
- `AggregateQuery`: Aggregation query example

## Performance Criteria

The performance tests validate against these criteria:

- **Simple SELECT**: < 1 second
- **Complex JOIN**: < 5 seconds
- **Aggregate Queries**: < 3 seconds
- **Large Result Sets**: < 10 seconds
- **Batch Operations**: < 15 seconds
- **Memory Usage**: < 50MB increase
- **Concurrent Queries**: < 30 seconds for 50 queries

## Running the Tests

### Prerequisites

- .NET 8.0
- MSTest framework
- Moq for mocking
- FluentAssertions for assertions
- Microsoft.Data.Sqlite for in-memory testing

### Running All Tests

```bash
dotnet test tests/ETL.Enterprise.UnitTests/ETL.Enterprise.UnitTests.csproj
```

### Running Specific Test Classes

```bash
# Run only SQL query tests
dotnet test --filter "ClassName=ETL.Tests.Unit.SqlQueryTests"

# Run only performance tests
dotnet test --filter "ClassName=ETL.Tests.Unit.SqlQueryPerformanceTests"
```

### Running with Coverage

```bash
dotnet test --collect:"XPlat Code Coverage"
```

## Best Practices

### Writing SQL Query Tests

1. **Use Descriptive Test Names**: Follow the pattern `Method_Scenario_ExpectedResult`
2. **Arrange-Act-Assert**: Structure tests clearly with these three sections
3. **Mock External Dependencies**: Use mocks for database connections and commands
4. **Test Edge Cases**: Include tests for null parameters, empty results, and error conditions
5. **Validate Performance**: Include performance assertions for critical queries

### Test Data Management

1. **Use Builders**: Use builder pattern for creating test data
2. **Generate Realistic Data**: Create test data that resembles production data
3. **Clean Up**: Ensure proper cleanup of test databases and resources
4. **Isolate Tests**: Each test should be independent and not rely on other tests

### Performance Testing

1. **Set Realistic Limits**: Set performance criteria based on business requirements
2. **Test Under Load**: Include stress tests with multiple concurrent operations
3. **Monitor Memory**: Track memory usage to prevent memory leaks
4. **Benchmark Regularly**: Run performance benchmarks regularly to detect regressions

## Troubleshooting

### Common Issues

1. **Mock Setup Issues**: Ensure all mock methods are properly configured
2. **Async/Await**: Use proper async/await patterns in tests
3. **Resource Cleanup**: Ensure proper disposal of test resources
4. **Test Isolation**: Ensure tests don't interfere with each other

### Debug Tips

1. **Use Debug Output**: Add debug output to understand test execution flow
2. **Check Mock Calls**: Verify that mock methods are called as expected
3. **Validate Test Data**: Ensure test data is correctly generated and used
4. **Performance Profiling**: Use profiling tools to identify performance bottlenecks

## Contributing

When adding new SQL query tests:

1. Follow the existing naming conventions
2. Include comprehensive test coverage
3. Add appropriate performance criteria
4. Update this documentation
5. Ensure all tests pass before submitting

## Dependencies

- Microsoft.VisualStudio.TestTools.UnitTesting
- Microsoft.Extensions.Logging
- Moq
- FluentAssertions
- Microsoft.Data.Sqlite
- System.Data.SqlClient
