# ETL Scalable - Enterprise-Grade Data Transformation Solution

A high-performance, scalable ETL (Extract, Transform, Load) solution built in C# designed to handle large-scale data processing across multiple databases. This solution is optimized for processing 1 TB/day across N databases with enterprise-grade features.

## Features

### 🚀 **High Performance**
- **Batch Processing**: Configurable batch sizes (default: 100,000 records per batch)
- **Parallel Processing**: Task Parallel Library (TPL) for concurrent operations
- **Bulk Operations**: SQL Server bulk insert for optimal performance
- **Memory Optimization**: Efficient memory management for large datasets

### 🔧 **Enterprise Features**
- **Multi-Database Support**: Process data from N databases simultaneously
- **Comprehensive Error Handling**: Try-catch blocks with detailed error logging
- **SQL Server Integration**: ADO.NET with staging table support
- **Logging & Monitoring**: SQL Server-based logging with detailed metrics
- **Cancellation Support**: Full cancellation token support for long-running operations

### 🛠 **Data Transformations**
- **String Cleaning**: Trim whitespace, remove special characters, normalize case
- **Custom Transformations**: Extensible transformation pipeline
- **LINQ Integration**: Leverage LINQ for complex data operations
- **Configurable Rules**: Flexible transformation rule system

### 📊 **Scalability**
- **Horizontal Scaling**: Process multiple databases in parallel
- **Vertical Scaling**: Optimize for available CPU cores
- **Memory Efficient**: Batch processing prevents memory overflow
- **1 TB/Day Capacity**: Designed for enterprise-scale data volumes

## Architecture

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   Source DB 1   │    │   Source DB 2   │    │   Source DB N   │
└─────────────────┘    └─────────────────┘    └─────────────────┘
         │                       │                       │
         └───────────────────────┼───────────────────────┘
                                 │
                    ┌─────────────────┐
                    │  DataTransformer│
                    │  (Parallel)     │
                    └─────────────────┘
                                 │
                    ┌─────────────────┐
                    │  Staging Table  │
                    │  (SQL Server)   │
                    └─────────────────┘
                                 │
                    ┌─────────────────┐
                    │  Logs Table     │
                    │  (Error Tracking)│
                    └─────────────────┘
```

## Quick Start

### 1. Installation

```bash
# Clone the repository
git clone <repository-url>
cd ETL-scalable

# Restore dependencies
dotnet restore

# Build the solution
dotnet build
```

### 2. Basic Usage

```csharp
using ETL.Core;
using Microsoft.Extensions.Logging;

// Configure source databases
var sourceDatabases = new List<SourceDatabaseConfig>
{
    new SourceDatabaseConfig
    {
        DatabaseName = "CustomerDB",
        ConnectionString = "Server=server1;Database=CustomerDB;Trusted_Connection=true;",
        SourceTable = "Customers"
    },
    new SourceDatabaseConfig
    {
        DatabaseName = "OrderDB",
        ConnectionString = "Server=server2;Database=OrderDB;Trusted_Connection=true;",
        SourceTable = "Orders"
    }
};

// Configure transformation rules
var rules = new TransformationRules
{
    CleanStrings = true,
    StringCleaningOptions = new StringCleaningOptions
    {
        TrimWhitespace = true,
        RemoveSpecialCharacters = false,
        NormalizeCase = true,
        RemoveExtraSpaces = true,
        MaxLength = 100
    },
    CustomTransformations = new List<Func<TransformedRecord, CancellationToken, Task>>
    {
        async (record, token) =>
        {
            // Custom transformation logic
            if (record.Data.ContainsKey("Email"))
            {
                record.Data["Email"] = record.Data["Email"].ToString().ToLowerInvariant();
            }
        }
    }
};

// Create logger
var loggerFactory = LoggerFactory.Create(builder => builder.AddConsole());
var logger = loggerFactory.CreateLogger<DataTransformer>();

// Initialize transformer
using var transformer = new DataTransformer(
    connectionString: "Server=localhost;Database=StagingDB;Trusted_Connection=true;",
    logger: logger,
    batchSize: 100000,
    maxDegreeOfParallelism: Environment.ProcessorCount
);

// Execute transformation
var result = await transformer.TransformDataAsync(sourceDatabases, rules);

// Check results
Console.WriteLine($"Processed: {result.ProcessedRecords} records");
Console.WriteLine($"Transformed: {result.TransformedRecords} records");
Console.WriteLine($"Duration: {result.Duration}");
Console.WriteLine($"Success: {result.Success}");
```

### 3. Advanced Configuration

```csharp
// Custom batch size and parallelism
var transformer = new DataTransformer(
    connectionString: stagingConnectionString,
    logger: logger,
    batchSize: 50000,           // Smaller batches for memory-constrained environments
    maxDegreeOfParallelism: 8   // Limit parallel operations
);

// Complex transformation rules
var advancedRules = new TransformationRules
{
    CleanStrings = true,
    StringCleaningOptions = new StringCleaningOptions
    {
        TrimWhitespace = true,
        RemoveSpecialCharacters = true,
        NormalizeCase = true,
        RemoveExtraSpaces = true,
        MaxLength = 255
    },
    CustomTransformations = new List<Func<TransformedRecord, CancellationToken, Task>>
    {
        // Data validation
        async (record, token) =>
        {
            if (record.Data.ContainsKey("Age"))
            {
                var age = Convert.ToInt32(record.Data["Age"]);
                if (age < 0 || age > 150)
                {
                    record.Data["Age"] = DBNull.Value;
                }
            }
        },
        
        // Data enrichment
        async (record, token) =>
        {
            if (record.Data.ContainsKey("Country"))
            {
                var country = record.Data["Country"].ToString();
                record.Data["Region"] = GetRegionFromCountry(country);
            }
        },
        
        // Aggregation
        async (record, token) =>
        {
            if (record.Data.ContainsKey("Amount"))
            {
                var amount = Convert.ToDecimal(record.Data["Amount"]);
                record.Data["AmountCategory"] = amount switch
                {
                    < 100 => "Low",
                    < 1000 => "Medium",
                    _ => "High"
                };
            }
        }
    }
};
```

## Database Schema

### Staging Table
```sql
CREATE TABLE StagingTable (
    Id INT IDENTITY(1,1) PRIMARY KEY,
    -- Your data columns here
    TransformationTimestamp DATETIME2 NOT NULL,
    CreatedDate DATETIME2 DEFAULT GETUTCDATE()
);
```

### Logs Table
```sql
CREATE TABLE Logs (
    Id INT IDENTITY(1,1) PRIMARY KEY,
    Operation NVARCHAR(100) NOT NULL,
    StartTime DATETIME2 NOT NULL,
    EndTime DATETIME2 NULL,
    Duration BIGINT NULL, -- milliseconds
    ProcessedRecords BIGINT NULL,
    TransformedRecords BIGINT NULL,
    ErrorCount INT NULL,
    Success BIT NOT NULL,
    Details NVARCHAR(MAX) NULL,
    ErrorMessage NVARCHAR(MAX) NULL,
    StackTrace NVARCHAR(MAX) NULL
);
```

## Performance Optimization

### 1. Batch Size Tuning
```csharp
// For memory-constrained environments
var transformer = new DataTransformer(connectionString, logger, batchSize: 50000);

// For high-performance environments
var transformer = new DataTransformer(connectionString, logger, batchSize: 200000);
```

### 2. Parallelism Configuration
```csharp
// Conservative approach
var transformer = new DataTransformer(connectionString, logger, maxDegreeOfParallelism: 4);

// Aggressive approach (use with caution)
var transformer = new DataTransformer(connectionString, logger, maxDegreeOfParallelism: 16);
```

### 3. Database Optimization
```sql
-- Create indexes on staging table
CREATE INDEX IX_StagingTable_TransformationTimestamp 
ON StagingTable (TransformationTimestamp);

-- Partition large tables
CREATE PARTITION FUNCTION PF_Date (DATETIME2)
AS RANGE RIGHT FOR VALUES ('2024-01-01', '2024-02-01', '2024-03-01');
```

## Error Handling

The solution provides comprehensive error handling:

```csharp
var result = await transformer.TransformDataAsync(sourceDatabases, rules);

if (!result.Success)
{
    foreach (var error in result.Errors)
    {
        Console.WriteLine($"Error: {error.Message}");
        Console.WriteLine($"Severity: {error.Severity}");
        Console.WriteLine($"Database: {error.DatabaseName}");
        Console.WriteLine($"Timestamp: {error.Timestamp}");
    }
}
```

## Testing

Run the unit tests:

```bash
# Run all tests
dotnet test

# Run with coverage
dotnet test --collect:"XPlat Code Coverage"

# Run specific test
dotnet test --filter "FullyQualifiedName~DataTransformerTests"
```

## Monitoring and Logging

### Performance Metrics
- **Processed Records**: Total records processed
- **Transformed Records**: Successfully transformed records
- **Duration**: Total processing time
- **Error Count**: Number of errors encountered
- **Success Rate**: Percentage of successful transformations

### Log Analysis
```sql
-- Get performance summary
SELECT 
    Operation,
    COUNT(*) as ExecutionCount,
    AVG(Duration) as AvgDuration,
    SUM(ProcessedRecords) as TotalProcessed,
    SUM(TransformedRecords) as TotalTransformed,
    AVG(CAST(TransformedRecords AS FLOAT) / NULLIF(ProcessedRecords, 0)) as SuccessRate
FROM Logs 
WHERE Operation = 'DataTransformation'
GROUP BY Operation;

-- Get recent errors
SELECT 
    StartTime,
    ErrorMessage,
    DatabaseName,
    Severity
FROM Logs 
WHERE Success = 0 
ORDER BY StartTime DESC;
```

## Deployment

### 1. Production Setup
```bash
# Build release version
dotnet build -c Release

# Publish for deployment
dotnet publish -c Release -o ./publish
```

### 2. Configuration
```json
{
  "ConnectionStrings": {
    "StagingDatabase": "Server=prod-server;Database=StagingDB;Integrated Security=true;",
    "SourceDatabases": [
      {
        "Name": "CustomerDB",
        "ConnectionString": "Server=server1;Database=CustomerDB;Integrated Security=true;",
        "SourceTable": "Customers"
      }
    ]
  },
  "TransformationSettings": {
    "BatchSize": 100000,
    "MaxDegreeOfParallelism": 8,
    "StringCleaning": {
      "TrimWhitespace": true,
      "RemoveSpecialCharacters": false,
      "NormalizeCase": true,
      "RemoveExtraSpaces": true,
      "MaxLength": 255
    }
  }
}
```

### 3. Windows Service
```csharp
// Create a Windows Service wrapper for scheduled execution
public class ETLService : ServiceBase
{
    protected override void OnStart(string[] args)
    {
        // Initialize and run ETL process
    }
}
```

## Troubleshooting

### Common Issues

1. **Memory Issues**
   - Reduce batch size
   - Increase server memory
   - Monitor memory usage during execution

2. **Performance Issues**
   - Check database indexes
   - Optimize network connectivity
   - Adjust parallelism settings

3. **Connection Issues**
   - Verify connection strings
   - Check firewall settings
   - Ensure proper permissions

### Debug Mode
```csharp
// Enable detailed logging
var loggerFactory = LoggerFactory.Create(builder => 
    builder.AddConsole().SetMinimumLevel(LogLevel.Debug));
```

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests for new functionality
5. Submit a pull request

## License

This project is licensed under the MIT License - see the LICENSE file for details.

## Support

For support and questions:
- Create an issue in the repository
- Contact the development team
- Check the documentation

---

**Note**: This solution is designed for enterprise environments. Always test thoroughly in a staging environment before deploying to production.
