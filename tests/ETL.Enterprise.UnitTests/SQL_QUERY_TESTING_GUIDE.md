# SQL Query Testing Guide

This guide explains how to use the comprehensive SQL query testing framework that supports both **inline SQL queries** and **file-based SQL queries**.

## Overview

The SQL query testing framework provides two main approaches for testing SQL queries:

1. **Inline SQL Queries**: SQL queries written directly in the test code
2. **File-Based SQL Queries**: SQL queries stored in external `.sql` files

Both approaches can be used together in the same test, providing maximum flexibility for different testing scenarios.

## Quick Start

### 1. Inline SQL Query Testing

```csharp
[TestMethod]
public async Task ExecuteQuery_InlineSql_ExecutesSuccessfully()
{
    // Arrange
    var query = "SELECT CustomerID, CustomerName, Email FROM Customers WHERE IsActive = 1";
    var parameters = new Dictionary<string, object> { ["IsActive"] = true };
    
    // Act
    var result = await queryExecutor.ExecuteQueryAsync<CustomerData>(query, parameters);
    
    // Assert
    result.Should().NotBeNull();
}
```

### 2. File-Based SQL Query Testing

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

### 3. Mixed Approach

```csharp
[TestMethod]
public async Task ExecuteQuery_MixedApproach_ExecutesBothSuccessfully()
{
    // Arrange
    var sqlFileLoader = new SqlFileLoader(_mockLogger.Object);
    
    // Inline query
    var inlineQuery = "SELECT COUNT(*) FROM Customers WHERE IsActive = 1";
    
    // File-based query
    var fileQuery = sqlFileLoader.LoadSqlQuery("GetActiveCustomerCount");
    
    // Act
    var inlineResult = await queryExecutor.ExecuteQueryAsync<int>(inlineQuery);
    var fileResult = await queryExecutor.ExecuteQueryAsync<int>(fileQuery);
    
    // Assert
    inlineResult.Should().NotBeNull();
    fileResult.Should().NotBeNull();
}
```

## When to Use Each Approach

### Use Inline SQL When:
- ✅ Testing simple, short queries
- ✅ Quick prototyping and experimentation
- ✅ Query logic is specific to the test
- ✅ Query is unlikely to be reused
- ✅ Testing parameter variations

### Use File-Based SQL When:
- ✅ Testing complex, long queries
- ✅ Queries are reused across multiple tests
- ✅ Queries need to be maintained by SQL developers
- ✅ Testing production-like queries
- ✅ Queries have metadata requirements
- ✅ Testing multiple related queries together

## File-Based SQL Query Features

### 1. Single Query Files

Create a `.sql` file with a single query:

**File: `SqlFiles/GetCustomerById.sql`**
```sql
--@name: GetCustomerById
--@description: Get customer by ID
--@type: SELECT
--@timeout: 30

SELECT 
    CustomerID,
    CustomerName,
    Email,
    Phone
FROM Customers 
WHERE CustomerID = @CustomerID;
```

**Usage:**
```csharp
var sqlQuery = sqlFileLoader.LoadSqlQuery("GetCustomerById");
var result = await queryExecutor.ExecuteQueryAsync<CustomerData>(sqlQuery, parameters);
```

### 2. Multiple Queries in One File

Create a `.sql` file with multiple queries separated by `GO`:

**File: `SqlFiles/MultipleQueries.sql`**
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

**Usage:**
```csharp
var queries = sqlFileLoader.LoadMultipleSqlQueries("MultipleQueries", "GO");
foreach (var queryInfo in queries)
{
    var result = await queryExecutor.ExecuteQueryAsync<CustomerData>(queryInfo.Query);
}
```

### 3. Parameterized Queries

**File: `SqlFiles/SelectOrdersByDateRange.sql`**
```sql
SELECT 
    OrderID,
    CustomerID,
    OrderDate,
    TotalAmount
FROM Orders 
WHERE OrderDate BETWEEN @StartDate AND @EndDate
ORDER BY OrderDate DESC;
```

**Usage:**
```csharp
var parameterizedQuery = sqlFileLoader.LoadParameterizedSqlQuery("SelectOrdersByDateRange");
var parameters = new Dictionary<string, object>
{
    ["StartDate"] = new DateTime(2024, 1, 1),
    ["EndDate"] = new DateTime(2024, 12, 31)
};
var result = await queryExecutor.ExecuteQueryAsync<OrderData>(parameterizedQuery.Query, parameters);
```

### 4. Queries with Metadata

**File: `SqlFiles/CustomerAggregateQuery.sql`**
```sql
--@name: CustomerAggregateQuery
--@description: Aggregate query to calculate customer statistics
--@type: SELECT
--@timeout: 90
--@database: SQL Server
--@author: ETL Team
--@version: 1.0
--@tags: aggregate,customer,statistics

SELECT 
    c.CustomerID,
    c.CustomerName,
    COUNT(o.OrderID) as OrderCount,
    SUM(o.TotalAmount) as TotalSpent
FROM Customers c
LEFT JOIN Orders o ON c.CustomerID = o.CustomerID
GROUP BY c.CustomerID, c.CustomerName
ORDER BY TotalSpent DESC;
```

**Usage:**
```csharp
var queryWithMetadata = sqlFileLoader.LoadSqlQueryWithMetadata("CustomerAggregateQuery");
var result = await queryExecutor.ExecuteQueryAsync<CustomerAggregateData>(
    queryWithMetadata.Query, 
    timeoutSeconds: queryWithMetadata.Metadata.TimeoutSeconds ?? 30);
```

### 5. Directory-Based Loading

Load all SQL files from a directory:

```csharp
var queries = sqlFileLoader.LoadSqlQueriesFromDirectory("CustomerQueries");
foreach (var kvp in queries)
{
    var fileName = kvp.Key;
    var sqlQuery = kvp.Value;
    var result = await queryExecutor.ExecuteQueryAsync<CustomerData>(sqlQuery);
}
```

## Advanced Features

### 1. SQL File Validation

```csharp
var validationResult = sqlFileLoader.ValidateSqlFile("MyQuery");
if (!validationResult.IsValid)
{
    foreach (var error in validationResult.Errors)
    {
        Console.WriteLine($"SQL Error: {error}");
    }
}
```

### 2. Custom SQL File Directory

```csharp
var customDirectory = Path.Combine(AppDomain.CurrentDomain.BaseDirectory, "CustomSqlFiles");
var sqlFileLoader = new SqlFileLoader(_mockLogger.Object, customDirectory);
```

### 3. Performance Testing with File-Based Queries

```csharp
[TestMethod]
public async Task ExecuteQuery_FileBased_PerformsWithinTimeLimit()
{
    // Arrange
    var sqlFileLoader = new SqlFileLoader(_mockLogger.Object);
    var sqlQuery = sqlFileLoader.LoadSqlQuery("PerformanceTestQuery");
    var maxExecutionTime = TimeSpan.FromSeconds(5);

    // Act
    var executionTime = await SqlQueryTestUtilities.MeasureExecutionTime(async () =>
    {
        await queryExecutor.ExecuteQueryAsync<CustomerData>(sqlQuery);
    });

    // Assert
    executionTime.Should().BeLessThan(maxExecutionTime);
}
```

## Best Practices

### 1. File Organization

```
SqlFiles/
├── CustomerQueries/
│   ├── GetCustomerById.sql
│   ├── GetCustomersByCity.sql
│   └── GetTopCustomers.sql
├── OrderQueries/
│   ├── SelectOrdersByDateRange.sql
│   └── SelectOrdersByCustomer.sql
├── ComplexQueries/
│   ├── ComplexOrderDetailsJoin.sql
│   └── CustomerAggregateQuery.sql
└── PerformanceTests/
    └── PerformanceTestQuery.sql
```

### 2. Naming Conventions

- Use descriptive file names: `GetCustomerById.sql`
- Use PascalCase for file names
- Include query type in name: `SelectOrdersByDateRange.sql`
- Use consistent naming patterns

### 3. Metadata Usage

- Always include `@name` and `@description`
- Set appropriate `@timeout` values
- Use `@tags` for categorization
- Include `@author` and `@version` for maintenance

### 4. Error Handling

```csharp
try
{
    var sqlQuery = sqlFileLoader.LoadSqlQuery("MyQuery");
    var result = await queryExecutor.ExecuteQueryAsync<CustomerData>(sqlQuery);
}
catch (FileNotFoundException ex)
{
    // Handle missing SQL file
    _logger.LogError(ex, "SQL file not found");
}
catch (Exception ex)
{
    // Handle other errors
    _logger.LogError(ex, "Error executing SQL query");
}
```

## Migration from Inline to File-Based

### Step 1: Extract Inline Query
```csharp
// Before (inline)
var query = "SELECT CustomerID, CustomerName FROM Customers WHERE IsActive = 1";

// After (file-based)
var sqlFileLoader = new SqlFileLoader(_mockLogger.Object);
var query = sqlFileLoader.LoadSqlQuery("GetActiveCustomers");
```

### Step 2: Create SQL File
**File: `SqlFiles/GetActiveCustomers.sql`**
```sql
--@name: GetActiveCustomers
--@description: Get active customers
--@type: SELECT
--@timeout: 30

SELECT CustomerID, CustomerName 
FROM Customers 
WHERE IsActive = 1;
```

### Step 3: Update Test
```csharp
[TestMethod]
public async Task ExecuteQuery_GetActiveCustomers_ExecutesSuccessfully()
{
    // Arrange
    var sqlFileLoader = new SqlFileLoader(_mockLogger.Object);
    var query = sqlFileLoader.LoadSqlQuery("GetActiveCustomers");
    
    // Act & Assert
    var result = await queryExecutor.ExecuteQueryAsync<CustomerData>(query);
    result.Should().NotBeNull();
}
```

## Troubleshooting

### Common Issues

1. **File Not Found**
   - Check file path and name
   - Ensure `.sql` extension is included
   - Verify file is in the correct directory

2. **Parameter Mismatch**
   - Verify parameter names match exactly
   - Check parameter types
   - Ensure all required parameters are provided

3. **Metadata Parsing Issues**
   - Check metadata comment format
   - Ensure proper `--@` prefix
   - Verify metadata syntax

4. **Performance Issues**
   - Use appropriate timeout values
   - Consider query complexity
   - Monitor execution times

## Conclusion

The SQL query testing framework provides flexible options for testing both simple and complex SQL queries. Choose the approach that best fits your testing needs:

- **Inline SQL**: Quick, simple, test-specific queries
- **File-Based SQL**: Complex, reusable, maintainable queries
- **Mixed Approach**: Best of both worlds for comprehensive testing

Both approaches are fully supported and can be used together in the same test suite, providing maximum flexibility for your SQL query testing needs.
