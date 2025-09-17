# SQL Server Testing Guide

This guide provides comprehensive information about testing SQL Server specific features and operations in the ETL system.

## Table of Contents

1. [Overview](#overview)
2. [SQL Server Specific Features](#sql-server-specific-features)
3. [Test Organization](#test-organization)
4. [Performance Testing](#performance-testing)
5. [Integration Testing](#integration-testing)
6. [Best Practices](#best-practices)
7. [Troubleshooting](#troubleshooting)

## Overview

The SQL Server testing framework is designed to test SQL Server specific features and ensure optimal performance and reliability. It includes:

- **Feature-Specific Tests**: Tests for SQL Server unique features
- **Performance Validation**: Performance testing for SQL Server operations
- **Integration Scenarios**: End-to-end workflow testing
- **File-Based Testing**: SQL Server specific SQL files with metadata

## SQL Server Specific Features

### 1. Pagination (OFFSET/FETCH)

SQL Server provides the `OFFSET` and `FETCH` clauses for pagination, which are more efficient than using `ROW_NUMBER()` for large datasets.

#### Example Query
```sql
SELECT CustomerID, CustomerName, Email
FROM Customers 
WHERE IsActive = 1
ORDER BY CustomerName
OFFSET @Offset ROWS
FETCH NEXT @PageSize ROWS ONLY;
```

#### Test Example
```csharp
[TestMethod]
public async Task ExecuteQuery_SqlServerPagination_ReturnsCorrectResults()
{
    // Arrange
    var query = "SELECT * FROM Customers ORDER BY CustomerName OFFSET @Offset ROWS FETCH NEXT @PageSize ROWS ONLY";
    var parameters = new Dictionary<string, object> { ["Offset"] = 10, ["PageSize"] = 5 };
    
    // Act
    var result = await queryExecutor.ExecuteQueryAsync<CustomerData>(query, parameters);
    
    // Assert
    result.Should().HaveCount(5);
}
```

### 2. Window Functions

SQL Server provides comprehensive window function support including ranking, aggregate, and analytical functions.

#### Example Query
```sql
SELECT 
    CustomerID,
    CustomerName,
    TotalSpent,
    ROW_NUMBER() OVER (ORDER BY TotalSpent DESC) as CustomerRank,
    RANK() OVER (ORDER BY TotalSpent DESC) as CustomerRankWithTies,
    DENSE_RANK() OVER (ORDER BY TotalSpent DESC) as DenseCustomerRank,
    LAG(TotalSpent, 1, 0) OVER (ORDER BY TotalSpent DESC) as PreviousCustomerSpent,
    LEAD(TotalSpent, 1, 0) OVER (ORDER BY TotalSpent DESC) as NextCustomerSpent,
    PERCENT_RANK() OVER (ORDER BY TotalSpent DESC) as PercentileRank
FROM CustomerStats
ORDER BY TotalSpent DESC;
```

#### Test Example
```csharp
[TestMethod]
public async Task ExecuteQuery_SqlServerWindowFunctions_ReturnsRankedResults()
{
    // Arrange
    var query = "SELECT CustomerID, TotalSpent, ROW_NUMBER() OVER (ORDER BY TotalSpent DESC) as CustomerRank FROM CustomerStats";
    
    // Act
    var result = await queryExecutor.ExecuteQueryAsync<CustomerRankingData>(query);
    
    // Assert
    result.First().CustomerRank.Should().Be(1);
}
```

### 3. Common Table Expressions (CTEs)

CTEs provide a way to create temporary result sets that can be referenced multiple times in a query.

#### Example Query
```sql
WITH OrderSummary AS (
    SELECT 
        o.OrderID,
        o.CustomerID,
        o.OrderDate,
        o.TotalAmount,
        ROW_NUMBER() OVER (PARTITION BY o.CustomerID ORDER BY o.OrderDate DESC) as OrderRank
    FROM Orders o
    WHERE o.OrderDate >= @StartDate
)
SELECT 
    OrderID,
    CustomerID,
    OrderDate,
    TotalAmount,
    OrderRank
FROM OrderSummary
WHERE OrderRank <= 5
ORDER BY CustomerID, OrderDate DESC;
```

#### Test Example
```csharp
[TestMethod]
public async Task ExecuteQuery_SqlServerCTE_ReturnsComplexResults()
{
    // Arrange
    var query = "WITH OrderSummary AS (SELECT OrderID, CustomerID, ROW_NUMBER() OVER (PARTITION BY CustomerID ORDER BY OrderDate DESC) as OrderRank FROM Orders) SELECT * FROM OrderSummary WHERE OrderRank <= 3";
    
    // Act
    var result = await queryExecutor.ExecuteQueryAsync<OrderSummaryData>(query);
    
    // Assert
    result.All(r => r.OrderRank <= 3).Should().BeTrue();
}
```

### 4. MERGE Statements

MERGE statements provide a way to perform INSERT, UPDATE, or DELETE operations in a single statement.

#### Example Query
```sql
MERGE Products AS target
USING (SELECT @ProductID as ProductID, @ProductName as ProductName, @Price as Price) AS source
ON target.ProductID = source.ProductID
WHEN MATCHED THEN
    UPDATE SET 
        ProductName = source.ProductName, 
        Price = source.Price, 
        ModifiedDate = GETDATE()
WHEN NOT MATCHED THEN
    INSERT (ProductID, ProductName, Price, CreatedDate)
    VALUES (source.ProductID, source.ProductName, source.Price, GETDATE());
```

#### Test Example
```csharp
[TestMethod]
public async Task ExecuteQuery_SqlServerMerge_ReturnsAffectedRows()
{
    // Arrange
    var query = "MERGE Products AS target USING (SELECT @ProductID as ProductID, @ProductName as ProductName) AS source ON target.ProductID = source.ProductID WHEN MATCHED THEN UPDATE SET ProductName = source.ProductName WHEN NOT MATCHED THEN INSERT (ProductID, ProductName) VALUES (source.ProductID, source.ProductName)";
    
    // Act
    var result = await queryExecutor.ExecuteNonQueryAsync(query, parameters);
    
    // Assert
    result.Should().Be(1);
}
```

### 5. OUTPUT Clause

The OUTPUT clause allows you to return data from INSERT, UPDATE, or DELETE statements.

#### Example Query
```sql
INSERT INTO Customers (CustomerName, Email, Phone, CreatedDate)
OUTPUT INSERTED.CustomerID, INSERTED.CustomerName, INSERTED.Email
VALUES (@CustomerName, @Email, @Phone, @CreatedDate);
```

#### Test Example
```csharp
[TestMethod]
public async Task ExecuteQuery_SqlServerOutputClause_ReturnsInsertedData()
{
    // Arrange
    var query = "INSERT INTO Customers (CustomerName, Email) OUTPUT INSERTED.CustomerID, INSERTED.CustomerName VALUES (@CustomerName, @Email)";
    
    // Act
    var result = await queryExecutor.ExecuteQueryAsync<CustomerData>(query, parameters);
    
    // Assert
    result.Should().HaveCount(1);
    result.First().CustomerName.Should().Be("New Customer");
}
```

### 6. Full-Text Search

SQL Server provides full-text search capabilities using `CONTAINSTABLE` and `FREETEXTTABLE`.

#### Example Query
```sql
SELECT 
    p.ProductID,
    p.ProductName,
    p.Description,
    KEY_TBL.RANK as SearchRank
FROM Products p
INNER JOIN CONTAINSTABLE(Products, (ProductName, Description), @SearchTerm) AS KEY_TBL
    ON p.ProductID = KEY_TBL.[KEY]
WHERE p.Discontinued = 0
ORDER BY KEY_TBL.RANK DESC;
```

#### Test Example
```csharp
[TestMethod]
public async Task ExecuteQuery_SqlServerFullTextSearch_ReturnsRankedResults()
{
    // Arrange
    var query = "SELECT p.ProductID, p.ProductName, KEY_TBL.RANK as SearchRank FROM Products p INNER JOIN CONTAINSTABLE(Products, (ProductName, Description), @SearchTerm) AS KEY_TBL ON p.ProductID = KEY_TBL.[KEY] ORDER BY KEY_TBL.RANK DESC";
    
    // Act
    var result = await queryExecutor.ExecuteQueryAsync<ProductData>(query, parameters);
    
    // Assert
    result.Should().NotBeEmpty();
    result.First().SearchRank.Should().BeGreaterThan(0);
}
```

## Test Organization

### Directory Structure

```
SqlServerTests/
├── QueryExecution/           # Basic query execution tests
├── Performance/              # Performance and load tests
├── Integration/              # Integration and workflow tests
├── StoredProcedures/         # Stored procedure tests
├── DataTypes/                # Data type specific tests
└── SqlFiles/                 # SQL file organization
    ├── Queries/              # Query files
    ├── StoredProcedures/     # Stored procedure files
    ├── Functions/            # Function files
    └── Views/                # View files
```

### Test Class Organization

Each test class is organized into logical regions:

```csharp
[TestClass]
public class SqlServerQueryExecutionTests
{
    #region SQL Server Specific Features Tests
    // Tests for OFFSET/FETCH, window functions, CTEs, etc.
    #endregion

    #region SQL Server Data Type Tests
    // Tests for SQL Server specific data types
    #endregion

    #region Helper Methods
    // Helper methods for test setup and data conversion
    #endregion
}
```

## Performance Testing

### Performance Criteria

| Feature | Time Limit | Memory Limit | Notes |
|---------|------------|--------------|-------|
| Window Functions | < 3 seconds | < 30MB | For 10,000 records |
| CTE Queries | < 5 seconds | < 50MB | For complex multi-CTE |
| MERGE Operations | < 2 seconds | < 10MB | Single operations |
| Pagination | < 2 seconds | < 20MB | Large offset values |
| Full-Text Search | < 3 seconds | < 25MB | Complex searches |
| Batch Operations | < 20 seconds | < 100MB | 1,000 operations |

### Performance Test Example

```csharp
[TestMethod]
public async Task ExecuteQuery_SqlServerWindowFunctions_PerformsWithinTimeLimit()
{
    // Arrange
    var query = "SELECT CustomerID, TotalSpent, ROW_NUMBER() OVER (ORDER BY TotalSpent DESC) as CustomerRank FROM CustomerStats";
    var maxExecutionTime = TimeSpan.FromSeconds(3);
    
    // Act
    var executionTime = await SqlQueryTestUtilities.MeasureExecutionTime(async () =>
    {
        await queryExecutor.ExecuteQueryAsync<CustomerData>(query);
    });
    
    // Assert
    executionTime.Should().BeLessThan(maxExecutionTime);
}
```

### Memory Usage Testing

```csharp
[TestMethod]
public async Task ExecuteQuery_SqlServerWindowFunctions_MemoryUsageStaysWithinLimits()
{
    // Arrange
    var maxMemoryIncreaseMB = 30;
    
    // Act
    var initialMemory = GC.GetTotalMemory(false);
    await queryExecutor.ExecuteQueryAsync<CustomerData>(query);
    var finalMemory = GC.GetTotalMemory(false);
    var memoryIncrease = (finalMemory - initialMemory) / (1024 * 1024);
    
    // Assert
    memoryIncrease.Should().BeLessThan(maxMemoryIncreaseMB);
}
```

## Integration Testing

### Workflow Testing

Integration tests verify complete workflows using multiple SQL Server features:

```csharp
[TestMethod]
public async Task ExecuteWorkflow_CustomerOrderWorkflow_CompletesSuccessfully()
{
    // Step 1: Create customer with OUTPUT clause
    var customerResult = await _queryExecutor.ExecuteQueryAsync<CustomerData>(createCustomerQuery, customerParameters);
    
    // Step 2: Create order with CTE
    var orderResult = await _queryExecutor.ExecuteQueryAsync<OrderData>(createOrderQuery, orderParameters);
    
    // Step 3: Update order status with MERGE
    var updateResult = await _queryExecutor.ExecuteQueryAsync<OrderData>(updateOrderQuery, updateParameters);
    
    // Assert all steps completed successfully
    customerResult.Should().NotBeNull();
    orderResult.Should().NotBeNull();
    updateResult.Should().NotBeNull();
}
```

### Stored Procedure Testing

```csharp
[TestMethod]
public async Task ExecuteStoredProcedure_GetCustomerOrders_ReturnsExpectedResults()
{
    // Arrange
    var storedProcedureQuery = "EXEC [dbo].[GetCustomerOrders] @CustomerID = @CustomerID, @StartDate = @StartDate, @EndDate = @EndDate, @TotalRecords = @TotalRecords OUTPUT";
    
    // Act
    var result = await _queryExecutor.ExecuteQueryAsync<CustomerOrderData>(storedProcedureQuery, parameters);
    
    // Assert
    result.Should().NotBeNull();
    result.First().CustomerID.Should().Be(123);
}
```

### Function Testing

```csharp
[TestMethod]
public async Task ExecuteFunction_CalculateOrderTotal_ReturnsCorrectTotal()
{
    // Arrange
    var functionQuery = "SELECT [dbo].[CalculateOrderTotal](@OrderID, @TaxRate, @ShippingCost) as CalculatedTotal";
    
    // Act
    var result = await _queryExecutor.ExecuteQueryAsync<OrderTotalData>(functionQuery, parameters);
    
    // Assert
    result.First().CalculatedTotal.Should().Be(118.00m);
}
```

## Best Practices

### 1. Test Organization

- **Group Related Tests**: Organize tests by SQL Server feature
- **Use Descriptive Names**: Follow the pattern `Method_Scenario_ExpectedResult`
- **Include Performance Tests**: Add performance assertions for complex queries
- **Test Edge Cases**: Include tests for null values, empty results, and error conditions

### 2. Mock Setup

- **Mock Database Connections**: Use mocks for database connections and commands
- **Setup Data Readers**: Properly configure mock data readers for expected results
- **Handle Async Operations**: Use proper async/await patterns in tests
- **Clean Up Resources**: Ensure proper disposal of test resources

### 3. Performance Testing

- **Set Realistic Limits**: Base performance criteria on SQL Server capabilities
- **Test Under Load**: Include stress tests with multiple concurrent operations
- **Monitor Memory**: Track memory usage to prevent memory leaks
- **Benchmark Regularly**: Run performance benchmarks to detect regressions

### 4. Integration Testing

- **Test Complete Workflows**: Test end-to-end scenarios with multiple SQL Server features
- **Test Error Handling**: Include tests for transaction rollbacks and error scenarios
- **Test Stored Procedures**: Include tests for stored procedures and functions
- **Test Views**: Include tests for complex views with aggregations

### 5. SQL File Management

- **Use Metadata**: Include proper metadata in SQL files
- **Organize by Type**: Separate queries, stored procedures, functions, and views
- **Version Control**: Track changes to SQL files
- **Documentation**: Include comments explaining complex SQL logic

## Troubleshooting

### Common Issues

#### 1. Mock Setup Issues

**Problem**: Tests fail due to improper mock configuration.

**Solution**: Ensure all mock methods are properly configured:

```csharp
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
```

#### 2. Performance Issues

**Problem**: Tests fail due to performance criteria being too strict.

**Solution**: Adjust performance criteria based on actual SQL Server performance:

```csharp
// Adjust based on actual performance
var maxExecutionTime = TimeSpan.FromSeconds(5); // Increase if needed
var maxMemoryIncreaseMB = 50; // Increase if needed
```

#### 3. Data Type Issues

**Problem**: Tests fail due to SQL Server specific data type handling.

**Solution**: Use appropriate data types and conversion methods:

```csharp
public class SqlServerDataTypeTestData
{
    public Guid UniqueIdentifier { get; set; }
    public DateTime DateTime2 { get; set; }
    public DateTimeOffset DateTimeOffset { get; set; }
    public decimal Money { get; set; }
    public decimal SmallMoney { get; set; }
    public string VarCharMax { get; set; }
    public string NVarCharMax { get; set; }
    public byte[] VarBinaryMax { get; set; }
    public string Xml { get; set; }
}
```

#### 4. Transaction Issues

**Problem**: Tests fail due to transaction management issues.

**Solution**: Ensure proper transaction handling:

```csharp
[TestMethod]
public async Task ExecuteTransaction_WithRollback_RollsBackAllChanges()
{
    // Act & Assert
    await Assert.ThrowsExceptionAsync<SqlException>(async () =>
    {
        await _queryExecutor.ExecuteBatchAsync(transactionQueries, useTransaction: true);
    });
}
```

### Debug Tips

1. **Use SQL Server Profiler**: Profile actual SQL Server queries for performance analysis
2. **Check Execution Plans**: Analyze execution plans for complex queries
3. **Monitor Memory Usage**: Track memory consumption during window function operations
4. **Validate Results**: Ensure SQL Server specific features return expected results
5. **Use Debug Output**: Add debug output to understand test execution flow
6. **Check Mock Calls**: Verify that mock methods are called as expected

### Performance Optimization

1. **Index Optimization**: Ensure proper indexes for tested queries
2. **Query Optimization**: Optimize complex queries for better performance
3. **Memory Management**: Monitor and optimize memory usage
4. **Connection Pooling**: Use connection pooling for better performance
5. **Batch Operations**: Use batch operations for better performance

## Conclusion

The SQL Server testing framework provides comprehensive testing capabilities for SQL Server specific features. By following the best practices and troubleshooting guidelines outlined in this guide, you can ensure reliable and performant SQL Server operations in your ETL system.

For additional information, refer to:
- [SQL Server Tests README](README.md)
- [Main SQL Query Tests Documentation](../README_SqlQueryTests.md)
- [SQL Query Testing Guide](../SQL_QUERY_TESTING_GUIDE.md)
