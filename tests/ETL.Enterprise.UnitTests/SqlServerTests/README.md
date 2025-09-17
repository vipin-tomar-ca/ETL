# SQL Server Payroll Tests

This directory contains comprehensive tests specifically designed for SQL Server payroll data extraction and analytics, supporting multi-tenant architecture and data security requirements.

## Overview

The SQL Server payroll test suite provides:

- **Payroll Data Extraction**: Tests for extracting employee data from client databases
- **Analytics Queries**: Tests for payroll analytics and reporting in destination database
- **Multi-Tenant Support**: Tests for tenant isolation and cross-tenant analytics
- **Data Security**: Tests for encryption, access control, and compliance
- **Performance Testing**: Performance validation for payroll data operations
- **Integration Testing**: End-to-end payroll data workflow testing

## Directory Structure

```
SqlServerTests/
├── PayrollExtraction/           # Payroll data extraction tests
├── AnalyticsDestination/        # Analytics database tests
├── MultiTenant/                 # Multi-tenant data tests
├── DataSecurity/                # Data security and compliance tests
├── DatabaseSetup/               # Database setup and test data generation
├── QueryValidation/             # Query validation against real database
├── RegressionTesting/           # Regression testing and data comparison
├── TestData/                    # Test data management and orchestration
├── AdvancedFeatures/            # Advanced SQL Server features testing
├── ErrorHandling/               # Error handling and edge case testing
├── TransactionTesting/          # Transaction and concurrency testing
├── DataCleanup/                 # Data cleanup and teardown testing
├── Monitoring/                  # Monitoring and observability testing
├── SqlFiles/                    # SQL Server specific SQL files
│   ├── PayrollQueries/          # Source database extraction queries
│   ├── AnalyticsQueries/        # Destination analytics queries
│   ├── SecurityQueries/         # Security and access control queries
│   ├── DatabaseSetup/           # Database creation and setup scripts
│   ├── TestDataGeneration/      # Test data generation scripts
│   ├── AdvancedFeatures/        # Advanced SQL Server feature scripts
│   ├── ErrorScenarios/          # Error scenario testing scripts
│   └── TransactionTests/        # Transaction testing scripts
└── README.md                    # This documentation
```

## Payroll Data Coverage

### Source Database Extraction (Client Databases)

#### Employee Start Data
- **Purpose**: Extract new hire information for payroll analytics
- **Data**: Employee details, start dates, department assignments, initial compensation
- **File**: `SqlFiles/PayrollQueries/EmployeeStartData.sql`
- **Test**: `PayrollExtraction/PayrollDataExtractionTests.cs`

#### Employee Exit Data
- **Purpose**: Extract termination information for payroll analytics
- **Data**: Exit dates, termination reasons, final settlements, exit interviews
- **File**: `SqlFiles/PayrollQueries/EmployeeExitData.sql`
- **Test**: `PayrollExtraction/PayrollDataExtractionTests.cs`

#### Employee Compensation Data
- **Purpose**: Extract comprehensive compensation information
- **Data**: Base salary, allowances, deductions, benefits, net pay calculations
- **File**: `SqlFiles/PayrollQueries/EmployeeCompensationData.sql`
- **Test**: `PayrollExtraction/PayrollDataExtractionTests.cs`

#### Employee PII Data
- **Purpose**: Extract personally identifiable information (encrypted)
- **Data**: Personal details, addresses, bank information, emergency contacts
- **File**: `SqlFiles/PayrollQueries/EmployeePIIData.sql`
- **Test**: `PayrollExtraction/PayrollDataExtractionTests.cs`

#### Organizational Hierarchy
- **Purpose**: Extract organizational structure and reporting relationships
- **Data**: Management hierarchy, reporting levels, direct reports count
- **File**: `SqlFiles/PayrollQueries/OrganizationalHierarchy.sql`
- **Test**: `PayrollExtraction/PayrollDataExtractionTests.cs`

#### Employee Absence Data
- **Purpose**: Extract absence and leave information
- **Data**: Leave types, dates, approval status, absence patterns
- **File**: `SqlFiles/PayrollQueries/EmployeeAbsenceData.sql`
- **Test**: `PayrollExtraction/PayrollDataExtractionTests.cs`

#### Dynamic Security Data
- **Purpose**: Extract security and access control information
- **Data**: Access levels, permissions, security groups, audit information
- **File**: `SqlFiles/SecurityQueries/DynamicSecurityData.sql`
- **Test**: `DataSecurity/PayrollDataSecurityTests.cs`

### Destination Analytics Database

#### Payroll Analytics Summary
- **Purpose**: Aggregated payroll metrics and trends
- **Data**: Department summaries, cost analysis, absence patterns, trends
- **File**: `SqlFiles/AnalyticsQueries/PayrollAnalyticsSummary.sql`
- **Test**: `AnalyticsDestination/PayrollAnalyticsTests.cs`

#### Employee Analytics Dashboard
- **Purpose**: Individual employee metrics and rankings
- **Data**: Employee rankings, percentiles, comparisons, performance metrics
- **File**: `SqlFiles/AnalyticsQueries/EmployeeAnalyticsDashboard.sql`
- **Test**: `AnalyticsDestination/PayrollAnalyticsTests.cs`

## Multi-Tenant Support

### Tenant Isolation
- **Purpose**: Ensure data segregation between different clients
- **Features**: Tenant-specific filtering, client isolation, data validation
- **Test**: `MultiTenant/MultiTenantPayrollTests.cs`

### Cross-Tenant Analytics
- **Purpose**: Aggregated insights across multiple tenants
- **Features**: Tenant comparisons, cross-tenant rankings, aggregated metrics
- **Test**: `MultiTenant/MultiTenantPayrollTests.cs`

## Data Security Features

### Encryption
- **Purpose**: Protect sensitive PII data
- **Features**: SQL Server encryption functions, key management, encrypted storage
- **Test**: `DataSecurity/PayrollDataSecurityTests.cs`

### Access Control
- **Purpose**: Role-based access to sensitive information
- **Features**: Permission-based filtering, user authorization, access logging
- **Test**: `DataSecurity/PayrollDataSecurityTests.cs`

### Data Masking
- **Purpose**: Mask sensitive data for unauthorized users
- **Features**: Dynamic masking, partial data exposure, role-based visibility
- **Test**: `DataSecurity/PayrollDataSecurityTests.cs`

### Audit Trails
- **Purpose**: Comprehensive logging of data access
- **Features**: Access logging, modification tracking, compliance reporting
- **Test**: `DataSecurity/PayrollDataSecurityTests.cs`

### GDPR Compliance
- **Purpose**: Ensure data privacy and compliance
- **Features**: Consent management, data retention, privacy controls
- **Test**: `DataSecurity/PayrollDataSecurityTests.cs`

## Comprehensive Testing Approach

### Real Database Validation
The new testing approach addresses the limitations of mocked data by:

1. **Database Setup**: Creates actual SQL Server database with realistic schemas
2. **Test Data Generation**: Generates comprehensive test data covering various scenarios
3. **Query Validation**: Executes queries against real database with actual data
4. **Regression Testing**: Compares current results with baseline data to detect changes
5. **Data Integrity**: Validates data quality and consistency
6. **Performance Testing**: Measures query performance with realistic data volumes

### Testing Workflow
1. **Database Setup Phase**: Create database and tables
2. **Test Data Generation Phase**: Generate comprehensive test data
3. **Baseline Data Generation Phase**: Export baseline data as JSON/CSV
4. **Query Validation Phase**: Execute and validate all payroll queries
5. **Regression Testing Phase**: Compare current results with baseline
6. **Performance Testing Phase**: Measure and validate query performance
7. **Data Integrity Testing Phase**: Validate data quality and consistency
8. **Test Report Generation**: Generate comprehensive test report

## Test Categories

### 1. Database Setup and Test Data Generation

**File**: `DatabaseSetup/PayrollDatabaseSetupTests.cs`

Tests database creation and comprehensive test data generation:

- **Database Creation**: Creates PayrollTestDB with all required tables
- **Schema Validation**: Validates database schema and table structures
- **Test Data Generation**: Generates 1000+ employees with realistic data
- **Data Quality Validation**: Validates data integrity and consistency
- **Edge Case Data**: Generates edge cases and boundary conditions
- **Baseline Data Export**: Exports baseline data as JSON for comparison

#### Example Test

```csharp
[TestMethod]
public async Task GenerateComprehensiveTestData_WithAllScenarios_CreatesExpectedData()
{
    // Arrange
    var dataGenerationScript = LoadSqlScript("TestDataGeneration/GenerateComprehensiveTestData.sql");
    var queryExecutor = new SqlQueryExecutor(CreateConnection(), _mockLogger.Object);

    // Act
    var result = await queryExecutor.ExecuteNonQueryAsync(dataGenerationScript);

    // Assert
    result.Should().BeGreaterThan(0, "Test data generation should insert records successfully");
    
    // Verify data counts
    var countQuery = @"SELECT 
        (SELECT COUNT(*) FROM Employees) as EmployeeCount,
        (SELECT COUNT(*) FROM Departments) as DepartmentCount,
        (SELECT COUNT(*) FROM Positions) as PositionCount";
    
    var counts = await queryExecutor.ExecuteQueryAsync<DataCounts>(countQuery);
    counts.First().EmployeeCount.Should().Be(1002);
}
```

### 2. Query Validation Tests

**File**: `QueryValidation/PayrollQueryValidationTests.cs`

Tests payroll queries against real database with actual data:

- **Individual Table Queries**: Validates queries against each table
- **Filtered Queries**: Tests queries with various filters and conditions
- **Payroll Queries**: Validates all payroll data extraction queries
- **Analytics Queries**: Tests analytics and reporting queries
- **Performance Validation**: Measures query execution times
- **Data Structure Validation**: Validates query result structures

#### Example Test

```csharp
[TestMethod]
public async Task ValidateEmployeeStartDataQuery_WithRealData_ReturnsExpectedResults()
{
    // Arrange
    var query = LoadSqlScript("PayrollQueries/EmployeeStartData.sql");
    var parameters = new Dictionary<string, object>
    {
        ["StartDate"] = new DateTime(2024, 1, 1),
        ["EndDate"] = new DateTime(2024, 12, 31),
        ["TenantID"] = "TENANT_001",
        ["ClientID"] = "CLIENT_ABC"
    };

    // Act
    var result = await queryExecutor.ExecuteQueryAsync<Dictionary<string, object>>(query, parameters);

    // Assert
    result.Should().NotBeNull("Employee start data query should return results");
    result.All(r => r["TenantID"].ToString() == "TENANT_001").Should().BeTrue();
}
```

### 3. Regression Testing

**File**: `RegressionTesting/PayrollRegressionTests.cs`

Tests for detecting changes in query results:

- **Data Comparison**: Compares current data with baseline data
- **Query Result Comparison**: Compares query results with baseline results
- **Scenario Comparison**: Compares scenario-specific results
- **Data Integrity Regression**: Validates data integrity over time
- **Performance Regression**: Detects performance degradation
- **Change Detection**: Identifies when queries or data change

#### Example Test

```csharp
[TestMethod]
public async Task CompareCurrentDataWithBaseline_AllTables_DetectsChanges()
{
    // Arrange
    var queryExecutor = new SqlQueryExecutor(CreateConnection(), _mockLogger.Object);
    var tables = new List<string> { "Employees", "Departments", "Positions" };

    // Act & Assert
    foreach (var table in tables)
    {
        var currentData = await queryExecutor.ExecuteQueryAsync<Dictionary<string, object>>($"SELECT * FROM {table}");
        var baselineData = await LoadBaselineData($"{table}_baseline.json");
        
        currentData.Count.Should().Be(baselineData.Count, $"Table {table} should have same number of records as baseline");
    }
}
```

### 4. Test Orchestration

**File**: `TestData/PayrollTestOrchestrator.cs`

Orchestrates complete testing workflow:

- **Complete Workflow**: Executes all testing phases in sequence
- **Phase Management**: Manages individual testing phases
- **Test Reporting**: Generates comprehensive test reports
- **Error Handling**: Handles errors and provides detailed feedback
- **Performance Monitoring**: Monitors overall test execution performance

#### Example Test

```csharp
[TestMethod]
public async Task ExecuteCompletePayrollTestingWorkflow_AllPhases_CompletesSuccessfully()
{
    // Phase 1: Database Setup
    await ExecuteDatabaseSetupPhase();
    
    // Phase 2: Test Data Generation
    await ExecuteTestDataGenerationPhase();
    
    // Phase 3: Baseline Data Generation
    await ExecuteBaselineDataGenerationPhase();
    
    // Phase 4: Query Validation
    await ExecuteQueryValidationPhase();
    
    // Phase 5: Regression Testing
    await ExecuteRegressionTestingPhase();
    
    // Phase 6: Performance Testing
    await ExecutePerformanceTestingPhase();
    
    // Phase 7: Data Integrity Testing
    await ExecuteDataIntegrityTestingPhase();
    
    // Phase 8: Generate Test Report
    await GenerateTestReport();
}
```

### 5. Advanced SQL Server Features Tests

**File**: `AdvancedFeatures/SqlServerAdvancedFeaturesTests.cs`

Tests advanced SQL Server features and capabilities:

- **Stored Procedures**: Tests execution of stored procedures with various parameter types
- **Functions**: Tests scalar-valued, table-valued, and aggregate functions
- **Triggers**: Tests INSERT, UPDATE, and DELETE triggers
- **Advanced Data Types**: Tests XML, JSON, and Geography data types
- **Advanced Query Features**: Tests MERGE, PIVOT, UNPIVOT operations
- **Table-Valued Parameters**: Tests stored procedures with table-valued parameters
- **Error Handling**: Tests stored procedure error handling and validation

#### Example Test

```csharp
[TestMethod]
public async Task ExecuteStoredProcedure_WithVariousParameters_ReturnsExpectedResults()
{
    // Arrange
    var storedProcedureQuery = @"
        EXEC [dbo].[GetEmployeePayrollSummary] 
            @EmployeeID = @EmployeeID,
            @StartDate = @StartDate,
            @EndDate = @EndDate,
            @IncludeBenefits = @IncludeBenefits,
            @TotalRecords = @TotalRecords OUTPUT";

    var parameters = new Dictionary<string, object>
    {
        ["EmployeeID"] = 1,
        ["StartDate"] = new DateTime(2024, 1, 1),
        ["EndDate"] = new DateTime(2024, 12, 31),
        ["IncludeBenefits"] = true,
        ["TotalRecords"] = 0 // Output parameter
    };

    // Act
    var result = await queryExecutor.ExecuteQueryAsync<Dictionary<string, object>>(storedProcedureQuery, parameters);

    // Assert
    result.Should().NotBeNull("Stored procedure should return results");
    parameters["TotalRecords"].Should().BeGreaterThan(0, "Output parameter should be set");
}
```

### 6. Error Handling and Edge Case Tests

**File**: `ErrorHandling/SqlServerErrorHandlingTests.cs`

Tests comprehensive error handling and edge cases:

- **SQL Injection Prevention**: Tests protection against SQL injection attacks
- **Timeout Handling**: Tests query timeout scenarios and handling
- **Connection Management**: Tests connection pool exhaustion and resource cleanup
- **Deadlock Detection**: Tests deadlock detection and handling
- **Data Validation**: Tests handling of NULL values, empty results, and invalid data types
- **Constraint Violations**: Tests handling of constraint violations and business rule errors
- **Transaction Error Handling**: Tests transaction rollback scenarios

#### Example Test

```csharp
[TestMethod]
public async Task ExecuteQuery_WithSQLInjectionAttempt_PreventsInjection()
{
    // Arrange
    var query = "SELECT EmployeeID, FirstName, LastName FROM Employees WHERE EmployeeNumber = @EmployeeNumber";
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
    var tableCount = await queryExecutor.ExecuteScalarAsync<int>(
        "SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = 'Employees'");
    tableCount.Should().Be(1, "Employees table should still exist");
}
```

### 7. Transaction and Concurrency Tests

**File**: `TransactionTesting/SqlServerTransactionTests.cs`

Tests transaction management and concurrency control:

- **Isolation Levels**: Tests READ UNCOMMITTED, READ COMMITTED, REPEATABLE READ, SERIALIZABLE
- **Concurrent Access**: Tests concurrent read and write operations
- **Lock Escalation**: Tests lock escalation behavior and handling
- **Transaction Rollback**: Tests explicit and implicit transaction rollback
- **Savepoints**: Tests savepoint and partial rollback functionality
- **Distributed Transactions**: Tests distributed transaction coordination
- **Long-Running Transactions**: Tests long-running transaction behavior

#### Example Test

```csharp
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
        await queryExecutor1.ExecuteNonQueryAsync("BEGIN TRANSACTION");
        await queryExecutor1.ExecuteNonQueryAsync("UPDATE Employees SET FirstName = 'DirtyRead' WHERE EmployeeID = 1");
        await Task.Delay(2000); // Keep transaction open
        await queryExecutor1.ExecuteNonQueryAsync("ROLLBACK TRANSACTION");
    });

    var task2 = Task.Run(async () =>
    {
        await Task.Delay(500); // Wait for first transaction to start
        var result = await queryExecutor2.ExecuteScalarAsync<string>(
            "SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED; SELECT FirstName FROM Employees WHERE EmployeeID = 1");
        return result;
    });

    await Task.WhenAll(task1, task2);

    // Assert
    var result = await task2;
    result.Should().Be("DirtyRead", "READ UNCOMMITTED should allow dirty reads");
}
```

### 8. Data Cleanup and Teardown Tests

**File**: `DataCleanup/DataCleanupTests.cs`

Tests data cleanup and resource management:

- **Test Data Cleanup**: Tests cleanup of test data after test execution
- **Foreign Key Cleanup**: Tests cleanup with foreign key constraints
- **Database State Reset**: Tests reset of database state to clean state
- **Identity Column Reset**: Tests reset of identity columns
- **Resource Cleanup**: Tests cleanup of database connections and transactions
- **Temporary Object Cleanup**: Tests cleanup of temporary objects
- **Data Integrity Validation**: Tests data integrity after cleanup

#### Example Test

```csharp
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
```

### 9. Monitoring and Observability Tests

**File**: `Monitoring/SqlServerMonitoringTests.cs`

Tests monitoring and observability capabilities:

- **Query Execution Plan Analysis**: Tests execution plan analysis and optimization recommendations
- **Performance Counter Monitoring**: Tests SQL Server performance counter tracking
- **Memory Usage Monitoring**: Tests memory consumption tracking
- **CPU Usage Monitoring**: Tests CPU utilization tracking
- **Deadlock Monitoring**: Tests deadlock detection and analysis
- **Resource Usage Monitoring**: Tests database file size and index usage monitoring
- **Connection Pool Monitoring**: Tests connection pool status tracking
- **Slow Query Detection**: Tests slow query identification and analysis

#### Example Test

```csharp
[TestMethod]
public async Task MonitorPerformanceCounters_DuringQueryExecution_TracksMetrics()
{
    // Arrange
    var queryExecutor = new SqlQueryExecutor(CreateConnection(), _mockLogger.Object);
    var query = LoadSqlScript("PayrollQueries/EmployeeStartData.sql");
    var parameters = new Dictionary<string, object>
    {
        ["StartDate"] = new DateTime(2024, 1, 1),
        ["EndDate"] = new DateTime(2024, 12, 31),
        ["TenantID"] = "TENANT_001",
        ["ClientID"] = "CLIENT_ABC"
    };

    // Act
    var beforeMetrics = await GetPerformanceMetrics(queryExecutor);
    var result = await queryExecutor.ExecuteQueryAsync<Dictionary<string, object>>(query, parameters);
    var afterMetrics = await GetPerformanceMetrics(queryExecutor);

    // Assert
    result.Should().NotBeNull("Query should execute successfully");
    afterMetrics.ExecutionCount.Should().BeGreaterThan(beforeMetrics.ExecutionCount, "Execution count should increase");
    afterMetrics.TotalElapsedTime.Should().BeGreaterThan(beforeMetrics.TotalElapsedTime, "Total elapsed time should increase");
}
```

### 10. Payroll Data Extraction Tests

**File**: `PayrollExtraction/PayrollDataExtractionTests.cs`

Tests payroll data extraction from client databases:

- **Employee Start Data**: New hire information extraction
- **Employee Exit Data**: Termination information extraction
- **Employee Compensation**: Salary and benefits data extraction
- **Employee PII**: Personal information extraction (encrypted)
- **Organizational Hierarchy**: Management structure extraction
- **Employee Absence**: Leave and attendance data extraction
- **Multi-Tenant Extraction**: Tenant-specific data extraction

#### Example Test

```csharp
[TestMethod]
public async Task ExtractEmployeeStartData_NewHires_ReturnsExpectedResults()
{
    // Arrange
    var query = @"SELECT e.EmployeeID, e.EmployeeNumber, e.FirstName, e.LastName, 
                         e.StartDate, e.DepartmentID, d.DepartmentName,
                         @TenantID as TenantID, @ClientID as ClientID
                  FROM Employees e
                  LEFT JOIN Departments d ON e.DepartmentID = d.DepartmentID
                  WHERE e.StartDate >= @StartDate AND e.StartDate <= @EndDate
                    AND e.IsActive = 1";

    var parameters = new Dictionary<string, object>
    {
        ["StartDate"] = new DateTime(2024, 1, 1),
        ["EndDate"] = new DateTime(2024, 12, 31),
        ["TenantID"] = "TENANT_001",
        ["ClientID"] = "CLIENT_ABC"
    };

    // Act & Assert
    var result = await queryExecutor.ExecuteQueryAsync<EmployeeStartData>(query, parameters);
    result.Should().NotBeNull();
    result.First().TenantID.Should().Be("TENANT_001");
}
```

### 2. Analytics Destination Tests

**File**: `AnalyticsDestination/PayrollAnalyticsTests.cs`

Tests analytics queries in destination database:

- **Payroll Analytics Summary**: Aggregated metrics and trends
- **Employee Analytics Dashboard**: Individual employee insights
- **Multi-Tenant Analytics**: Cross-tenant comparisons
- **Performance Testing**: Large dataset performance validation

#### Example Test

```csharp
[TestMethod]
public async Task ExecutePayrollAnalyticsSummary_WithAggregations_ReturnsExpectedResults()
{
    // Arrange
    var query = @"WITH PayrollSummary AS (
                    SELECT TenantID, ClientID, DepartmentID, DepartmentName,
                           COUNT(DISTINCT EmployeeID) as TotalEmployees,
                           SUM(GrossSalary) as TotalGrossSalary,
                           AVG(GrossSalary) as AverageGrossSalary
                    FROM PayrollAnalyticsData
                    WHERE ExtractionDate >= @StartDate AND ExtractionDate <= @EndDate
                    GROUP BY TenantID, ClientID, DepartmentID, DepartmentName
                  )
                  SELECT *, 
                         CASE WHEN TotalEmployees > 0 
                              THEN (TotalGrossSalary / TotalEmployees) 
                              ELSE 0 END as CostPerEmployee
                  FROM PayrollSummary";

    // Act & Assert
    var result = await queryExecutor.ExecuteQueryAsync<PayrollAnalyticsSummaryData>(query, parameters);
    result.Should().NotBeNull();
    result.First().CostPerEmployee.Should().BeGreaterThan(0);
}
```

### 3. Multi-Tenant Tests

**File**: `MultiTenant/MultiTenantPayrollTests.cs`

Tests multi-tenant data isolation and analytics:

- **Tenant Isolation**: Data segregation between tenants
- **Cross-Tenant Analytics**: Aggregated insights across tenants
- **Tenant Comparison**: Comparative analytics between tenants
- **Data Validation**: Tenant-specific data integrity checks

#### Example Test

```csharp
[TestMethod]
public async Task ExtractData_TenantIsolation_ReturnsOnlyTenantData()
{
    // Arrange
    var tenant1Parameters = new Dictionary<string, object>
    {
        ["TenantID"] = "TENANT_001",
        ["ClientID"] = "CLIENT_ABC"
    };

    var tenant2Parameters = new Dictionary<string, object>
    {
        ["TenantID"] = "TENANT_002",
        ["ClientID"] = "CLIENT_XYZ"
    };

    // Act
    var tenant1Result = await queryExecutor.ExecuteQueryAsync<MultiTenantEmployeeData>(query, tenant1Parameters);
    var tenant2Result = await queryExecutor.ExecuteQueryAsync<MultiTenantEmployeeData>(query, tenant2Parameters);

    // Assert
    tenant1Result.All(r => r.TenantID == "TENANT_001").Should().BeTrue();
    tenant2Result.All(r => r.TenantID == "TENANT_002").Should().BeTrue();
}
```

### 4. Data Security Tests

**File**: `DataSecurity/PayrollDataSecurityTests.cs`

Tests data security and compliance features:

- **Data Encryption**: PII data encryption and decryption
- **Access Control**: Role-based access restrictions
- **Data Masking**: Sensitive data masking for unauthorized users
- **Audit Trails**: Data access logging and tracking
- **GDPR Compliance**: Privacy and consent management

#### Example Test

```csharp
[TestMethod]
public async Task ExtractEncryptedPIIData_WithEncryption_ReturnsEncryptedResults()
{
    // Arrange
    var query = @"SELECT e.EmployeeID, e.EmployeeNumber, e.FirstName, e.LastName,
                         ENCRYPTBYKEY(KEY_GUID('PayrollPIIKey'), e.SocialSecurityNumber) as EncryptedSSN,
                         ENCRYPTBYKEY(KEY_GUID('PayrollPIIKey'), e.TaxID) as EncryptedTaxID
                  FROM Employees e
                  WHERE e.IsActive = 1";

    // Act & Assert
    var result = await queryExecutor.ExecuteQueryAsync<EncryptedPIIData>(query, parameters);
    result.Should().NotBeNull();
    result.First().EncryptedSSN.Should().NotBeNull();
    result.First().EncryptedTaxID.Should().NotBeNull();
}
```

## SQL File Conventions

All `.sql` files follow these metadata conventions:

```sql
--@name: EmployeeStartData
--@description: Extract employee start data for payroll analytics - multi-tenant support
--@type: SELECT
--@timeout: 60
--@database: SQL Server
--@author: Payroll ETL Team
--@version: 1.0
--@tags: payroll,employee,start,multitenant,extraction
--@tenant: CLIENT_DATABASE
--@security: ENCRYPTED (for PII data)
```

## Running Tests

### Run All Payroll Tests

```bash
dotnet test tests/ETL.Enterprise.UnitTests/ETL.Enterprise.UnitTests.csproj --filter "Namespace=ETL.Tests.Unit.SqlServer.Payroll"
```

### Run Specific Test Categories

```bash
# Payroll extraction tests
dotnet test --filter "ClassName=ETL.Tests.Unit.SqlServer.Payroll.PayrollDataExtractionTests"

# Analytics tests
dotnet test --filter "ClassName=ETL.Tests.Unit.SqlServer.Payroll.PayrollAnalyticsTests"

# Multi-tenant tests
dotnet test --filter "ClassName=ETL.Tests.Unit.SqlServer.Payroll.MultiTenantPayrollTests"

# Data security tests
dotnet test --filter "ClassName=ETL.Tests.Unit.SqlServer.Payroll.PayrollDataSecurityTests"
```

### Run Performance Tests

```bash
dotnet test --filter "TestMethod~Performance"
```

## Best Practices

### 1. Multi-Tenant Data Handling
- Always include `@TenantID` and `@ClientID` parameters
- Validate tenant isolation in tests
- Use tenant-specific filtering in queries

### 2. Data Security
- Encrypt PII data using SQL Server encryption functions
- Implement role-based access control
- Mask sensitive data for unauthorized users
- Maintain comprehensive audit trails

### 3. Performance Considerations
- Use appropriate indexes for tenant and date filtering
- Implement pagination for large datasets
- Monitor query execution times
- Test with realistic data volumes

### 4. Data Validation
- Validate data integrity across tenants
- Check for null values and data consistency
- Implement data quality checks
- Monitor data extraction completeness

## Troubleshooting

### Common Issues

1. **Tenant Isolation Failures**
   - Verify `@TenantID` and `@ClientID` parameters are correctly set
   - Check tenant-specific filtering in queries
   - Validate mock data includes correct tenant identifiers

2. **Performance Issues**
   - Review query execution plans
   - Check for missing indexes on tenant and date columns
   - Consider query optimization for large datasets

3. **Security Test Failures**
   - Verify encryption keys are properly configured
   - Check access control permissions
   - Validate data masking logic

4. **Data Validation Errors**
   - Review data quality checks
   - Verify mock data matches expected schema
   - Check for data type mismatches

## Getting Started

Refer to `SQL_SERVER_TESTING_GUIDE.md` for detailed instructions on how to write, run, and manage SQL Server payroll tests.