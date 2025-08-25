# .NET Spark MultiDB Delta Processing Guide

## Overview

The `Spark_MultiDB_Delta.cs` application is a .NET Spark application designed to process delta (changed) data from multiple CDC-enabled SQL Server databases. The application supports high-volume data processing (1 TB/day) with 10-20 Spark nodes and provides comprehensive error handling, monitoring, and multi-database join capabilities.

## Architecture

### Application Structure

```
Spark_MultiDB_Delta/
├── Spark_MultiDB_Delta.cs              # Main Spark processor class
├── Spark_MultiDB_Delta_Program.cs      # Main program entry point
├── Spark_MultiDB_Delta.csproj          # Project file
├── appsettings.json                    # Configuration file
├── jars/                               # Spark JAR dependencies
├── spark/                              # Spark configuration files
└── logs/                               # Application logs
```

### Processing Flow

```
1. Configuration Retrieval
   ├── Load database configurations from metadata
   ├── Validate CDC settings and connection strings
   └── Initialize Spark session with optimized settings

2. Parallel Database Processing
   ├── Process CDC-enabled databases
   │   ├── Query CDC change tables via JDBC
   │   ├── Handle LSN ranges and operation types
   │   └── Transform CDC data for joins
   └── Process non-CDC databases
       ├── Apply timestamp-based delta conditions
       ├── Load data via JDBC
       └── Add processing metadata

3. Multi-Database Joins
   ├── Register DataFrames as temporary views
   ├── Perform Spark SQL joins
   ├── Handle operation types (Insert, Update, Delete)
   └── Write joined results to target

4. Metadata Updates
   ├── Update LastProcessedLSN for CDC databases
   ├── Log processing statistics
   └── Handle errors and retries
```

## Key Features

### ✅ **CDC Support**
- **CDC Change Table Queries** - Direct JDBC queries to CDC change tables
- **LSN Range Processing** - Incremental processing using LastProcessedLSN
- **Operation Type Handling** - Support for Insert (2), Update (4), Delete (1) operations
- **Net Changes Support** - Configurable for final state only or all changes

### ✅ **Multi-Database Processing**
- **Parallel Processing** - Concurrent processing of multiple databases
- **Spark SQL Joins** - Complex multi-database joins using Spark SQL
- **Temporary Views** - DataFrames registered as views for SQL operations
- **Join Strategies** - Broadcast hash joins for optimal performance

### ✅ **Performance Optimization**
- **Adaptive Query Execution** - Spark's adaptive query execution enabled
- **Partition Management** - Dynamic partition coalescing and skew handling
- **Memory Management** - Optimized buffer sizes and memory fractions
- **JDBC Optimization** - Batch processing and connection pooling

### ✅ **Error Handling & Monitoring**
- **Comprehensive Logging** - Console, file, and database logging
- **Health Checks** - Database connectivity and Spark cluster health
- **Retry Logic** - Configurable retry policies with exponential backoff
- **Error Recovery** - Graceful handling of failures with fallback strategies

## Configuration

### 1. Connection Strings

#### Metadata Database
```json
{
  "ConnectionStrings": {
    "MetadataDB": "Data Source=ETL-SERVER;Initial Catalog=ETLMetadata;Integrated Security=SSPI;Application Name=Spark_MultiDB_Delta;"
  }
}
```

#### Target Database
```json
{
  "ConnectionStrings": {
    "TargetDB": "Data Source=ETL-SERVER;Initial Catalog=ETLTarget;Integrated Security=SSPI;Application Name=Spark_MultiDB_Delta;"
  }
}
```

### 2. Spark Configuration

#### Cluster Settings
```json
{
  "Spark": {
    "Master": "spark://spark-master:7077",
    "AppName": "ETL-Spark-MultiDB-Delta",
    "DriverMemory": "4g",
    "ExecutorMemory": "8g",
    "ExecutorCores": "4",
    "NumExecutors": "10",
    "MaxExecutors": "20"
  }
}
```

#### Performance Settings
```json
{
  "Spark": {
    "SQL": {
      "AdaptiveEnabled": true,
      "ShufflePartitions": 200,
      "AdaptiveAdvisoryPartitionSizeInBytes": "128m"
    },
    "JDBC": {
      "BatchSize": 10000,
      "QueryTimeout": 300,
      "FetchSize": 10000
    }
  }
}
```

### 3. Processing Configuration

#### CDC Settings
```json
{
  "Processing": {
    "CDC": {
      "DefaultRetentionHours": 24,
      "LSNValidationEnabled": true,
      "NetChangesEnabled": true
    }
  }
}
```

#### Multi-Database Settings
```json
{
  "MultiDatabase": {
    "JoinStrategy": {
      "Type": "BroadcastHashJoin",
      "BroadcastThreshold": "10m"
    },
    "Partitioning": {
      "Strategy": "Hash",
      "NumPartitions": 200
    }
  }
}
```

## Database Schema Requirements

### Metadata Database (ETLMetadata)

#### Connections Table
```sql
CREATE TABLE dbo.Connections
(
    ConnectionID INT IDENTITY(1,1) PRIMARY KEY,
    DatabaseName NVARCHAR(128) NOT NULL,
    ConnectionString NVARCHAR(MAX) NOT NULL,
    IsCDCEnabled BIT NOT NULL DEFAULT 0,
    LastProcessedLSN BINARY(10) NULL,
    DeltaCondition NVARCHAR(500) NULL,
    TableName NVARCHAR(128) NOT NULL,
    SchemaName NVARCHAR(128) NOT NULL DEFAULT 'dbo',
    ProcessingEngine NVARCHAR(50) NOT NULL DEFAULT 'Spark',
    BatchSize INT NOT NULL DEFAULT 10000,
    TimeoutMinutes INT NOT NULL DEFAULT 30
);
```

#### CDCStatus Table
```sql
CREATE TABLE dbo.CDCStatus
(
    CDCStatusID INT IDENTITY(1,1) PRIMARY KEY,
    ConnectionID INT NOT NULL,
    CaptureInstance NVARCHAR(128) NOT NULL,
    SourceSchema NVARCHAR(128) NOT NULL,
    SourceTable NVARCHAR(128) NOT NULL,
    IsEnabled BIT NOT NULL DEFAULT 0,
    SupportsNetChanges BIT NOT NULL DEFAULT 1,
    CaptureColumnList NVARCHAR(MAX) NULL,
    FOREIGN KEY (ConnectionID) REFERENCES dbo.Connections(ConnectionID)
);
```

### Target Database (ETLTarget)

#### Staging Tables
```sql
-- Staging table for individual database deltas
CREATE TABLE staging.ETLDatabase_Sales_Delta
(
    __$operation INT,
    __$seqval BIGINT,
    __$start_lsn BINARY(10),
    __$end_lsn BINARY(10),
    SaleID INT,
    CustomerID INT,
    ProductID INT,
    Quantity INT,
    UnitPrice DECIMAL(10,2),
    SaleDate DATETIME2,
    OperationType NVARCHAR(20),
    ProcessingEngine NVARCHAR(50),
    SourceDatabase NVARCHAR(128),
    ProcessingTimestamp DATETIME2,
    INDEX IX_ProcessingTimestamp (ProcessingTimestamp),
    INDEX IX_SourceDatabase (SourceDatabase)
);

-- Staging table for multi-database joined results
CREATE TABLE staging.MultiDB_Joined_Delta
(
    -- Joined columns from multiple databases
    CustomerID INT,
    SaleID INT,
    OrderID INT,
    ProductID INT,
    -- ... other joined columns
    ProcessingTimestamp DATETIME2,
    INDEX IX_CustomerID (CustomerID),
    INDEX IX_ProcessingTimestamp (ProcessingTimestamp)
);
```

## Usage Examples

### 1. Basic Execution

#### Command Line
```bash
# Build the application
dotnet build Spark_MultiDB_Delta.csproj

# Run with default configuration
dotnet run --project Spark_MultiDB_Delta.csproj

# Run with custom configuration
dotnet run --project Spark_MultiDB_Delta.csproj --environment Production
```

#### Spark Submit
```bash
# Submit to Spark cluster
spark-submit \
  --class org.apache.spark.deploy.dotnet.DotnetRunner \
  --master spark://spark-master:7077 \
  --deploy-mode cluster \
  --driver-memory 4g \
  --executor-memory 8g \
  --executor-cores 4 \
  --num-executors 10 \
  --conf spark.sql.adaptive.enabled=true \
  --conf spark.sql.adaptive.coalescePartitions.enabled=true \
  --conf spark.sql.shuffle.partitions=200 \
  Spark_MultiDB_Delta.jar
```

### 2. Configuration Database Setup

#### Add CDC-Enabled Database
```sql
INSERT INTO dbo.Connections 
(DatabaseName, ConnectionString, IsCDCEnabled, TableName, ProcessingEngine)
VALUES 
('ETLDatabase', 'Data Source=ETL-SERVER;Initial Catalog=ETLDatabase;Integrated Security=SSPI;', 
 1, 'Sales', 'Spark');

INSERT INTO dbo.CDCStatus 
(ConnectionID, CaptureInstance, SourceSchema, SourceTable, IsEnabled, SupportsNetChanges, CaptureColumnList)
VALUES 
(1, 'dbo_Sales', 'dbo', 'Sales', 1, 1, 'SaleID,CustomerID,ProductID,Quantity,UnitPrice,SaleDate');
```

#### Add Non-CDC Database
```sql
INSERT INTO dbo.Connections 
(DatabaseName, ConnectionString, IsCDCEnabled, DeltaCondition, TableName, ProcessingEngine)
VALUES 
('LegacyDatabase', 'Data Source=ETL-SERVER;Initial Catalog=LegacyDatabase;Integrated Security=SSPI;', 
 0, 'LastUpdated', 'Orders', 'Spark');
```

### 3. Monitoring and Logging

#### Check Processing Status
```sql
-- Check recent processing history
SELECT * FROM dbo.ProcessingHistory 
WHERE ProcessingEngine = 'Spark' 
ORDER BY StartTime DESC;

-- Check CDC status
SELECT * FROM dbo.CDCStatus 
WHERE IsEnabled = 1;

-- Check active connections
SELECT * FROM dbo.Connections 
WHERE ProcessingEngine = 'Spark' AND IsActive = 1;
```

#### Monitor Spark Application
```bash
# Check Spark application status
curl http://spark-master:8080/api/v1/applications

# Check Spark logs
tail -f logs/spark_multidb_delta.log

# Monitor Spark UI
open http://spark-master:8080
```

## Performance Optimization

### 1. Spark Configuration

#### Memory Management
```json
{
  "Spark": {
    "DriverMemory": "4g",
    "ExecutorMemory": "8g",
    "MemoryFraction": 0.8,
    "StorageFraction": 0.2
  }
}
```

#### Partitioning Strategy
```json
{
  "MultiDatabase": {
    "Partitioning": {
      "Strategy": "Hash",
      "PartitionColumns": ["CustomerID", "TransactionID"],
      "NumPartitions": 200
    }
  }
}
```

#### Join Optimization
```json
{
  "MultiDatabase": {
    "JoinStrategy": {
      "Type": "BroadcastHashJoin",
      "BroadcastThreshold": "10m",
      "AutoBroadcastJoinThreshold": "10m"
    }
  }
}
```

### 2. Database Optimization

#### Connection Pooling
```json
{
  "Spark": {
    "JDBC": {
      "BatchSize": 10000,
      "FetchSize": 10000,
      "IsolationLevel": "READ_COMMITTED"
    }
  }
}
```

#### Indexing Strategy
```sql
-- Create indexes for CDC change tables
CREATE INDEX IX_cdc_dbo_Sales_CT_start_lsn 
ON cdc.dbo_Sales_CT(__$start_lsn);

CREATE INDEX IX_cdc_dbo_Sales_CT_seqval 
ON cdc.dbo_Sales_CT(__$seqval);

-- Create indexes for staging tables
CREATE CLUSTERED INDEX IX_staging_ProcessingTimestamp 
ON staging.ETLDatabase_Sales_Delta(ProcessingTimestamp);

CREATE NONCLUSTERED INDEX IX_staging_SourceDatabase 
ON staging.ETLDatabase_Sales_Delta(SourceDatabase);
```

### 3. Scaling Strategies

#### Horizontal Scaling
```json
{
  "Spark": {
    "NumExecutors": "10",
    "MaxExecutors": "20",
    "DynamicAllocation": {
      "Enabled": true,
      "MinExecutors": "5",
      "MaxExecutors": "20"
    }
  }
}
```

#### Vertical Scaling
```json
{
  "Spark": {
    "ExecutorMemory": "16g",
    "ExecutorCores": "8",
    "DriverMemory": "8g"
  }
}
```

## Error Handling

### 1. Error Categories

#### CDC Errors
- **LSN Range Issues** - Invalid LSN ranges or missing LSN
- **Capture Instance Errors** - Capture instance not found or disabled
- **Schema Changes** - Column changes in CDC change tables

#### Database Errors
- **Connection Failures** - Network timeouts or authentication issues
- **Query Errors** - SQL syntax or constraint violations
- **Resource Exhaustion** - Memory or connection pool exhaustion

#### Spark Errors
- **Cluster Issues** - Executor failures or resource allocation problems
- **Memory Issues** - Out of memory errors or garbage collection problems
- **Network Issues** - Shuffle failures or data transfer problems

### 2. Recovery Strategies

#### Automatic Recovery
```json
{
  "ErrorHandling": {
    "RetryPolicy": {
      "MaxRetries": 3,
      "RetryDelaySeconds": 60,
      "BackoffMultiplier": 2.0
    },
    "Fallback": {
      "Enabled": true,
      "Strategy": "SkipAndContinue"
    }
  }
}
```

#### Manual Recovery
```sql
-- Reset LSN for failed processing
UPDATE dbo.Connections 
SET LastProcessedLSN = NULL
WHERE DatabaseName = 'ETLDatabase';

-- Reset processing status
UPDATE dbo.ProcessingHistory 
SET Status = 'PENDING', RetryCount = 0
WHERE Status = 'FAILED' AND ProcessingEngine = 'Spark';
```

### 3. Monitoring and Alerting

#### Error Monitoring
```sql
-- Create error monitoring view
CREATE VIEW dbo.vw_SparkErrors AS
SELECT TOP 100
    l.LogID,
    l.BatchID,
    l.DatabaseName,
    l.LogLevel,
    l.LogMessage,
    l.ErrorMessage,
    l.CreatedDate
FROM dbo.Logs l
WHERE l.LogLevel = 'ERROR' 
AND l.PackageName = 'Spark_MultiDB_Delta'
ORDER BY l.CreatedDate DESC;
```

#### Performance Monitoring
```sql
-- Create performance monitoring view
CREATE VIEW dbo.vw_SparkPerformance AS
SELECT 
    ph.DatabaseName,
    ph.TableName,
    ph.ProcessingTimeSeconds,
    ph.RecordsProcessed,
    ph.RecordsFailed,
    ph.StartTime,
    ph.EndTime,
    CAST(ph.RecordsProcessed AS FLOAT) / NULLIF(ph.ProcessingTimeSeconds, 0) AS RecordsPerSecond
FROM dbo.ProcessingHistory ph
WHERE ph.ProcessingEngine = 'Spark'
ORDER BY ph.StartTime DESC;
```

## Best Practices

### 1. CDC Best Practices

#### LSN Management
- **Validate LSN ranges** before processing
- **Handle LSN gaps** due to CDC cleanup
- **Backup LSN state** regularly
- **Monitor CDC retention** settings

#### Performance Optimization
- **Index CDC change tables** on LSN columns
- **Use net changes** when only final state is needed
- **Monitor CDC job performance** and cleanup
- **Configure appropriate retention** periods

### 2. Spark Best Practices

#### Memory Management
- **Tune executor memory** based on data size
- **Monitor garbage collection** metrics
- **Use appropriate storage levels** for caching
- **Configure memory fractions** correctly

#### Partitioning
- **Use appropriate partition sizes** (128MB target)
- **Monitor partition skew** and handle accordingly
- **Configure shuffle partitions** based on data size
- **Use adaptive query execution** features

#### Join Optimization
- **Use broadcast joins** for small tables
- **Monitor join performance** and adjust strategies
- **Cache frequently used DataFrames**
- **Use appropriate join types** (inner, left, right)

### 3. Database Best Practices

#### Connection Management
- **Use connection pooling** for JDBC connections
- **Configure appropriate timeouts** for long-running queries
- **Monitor connection usage** and adjust pool sizes
- **Use read-only connections** where possible

#### Indexing Strategy
- **Index CDC change tables** on LSN and sequence columns
- **Create composite indexes** for join conditions
- **Monitor index usage** and performance
- **Maintain index statistics** regularly

#### Staging Table Management
- **Partition staging tables** by date or processing timestamp
- **Create appropriate indexes** for query performance
- **Implement cleanup strategies** for old data
- **Monitor table growth** and storage usage

## Troubleshooting

### Common Issues

#### CDC Issues
```sql
-- Check CDC status
SELECT name, is_cdc_enabled FROM sys.databases WHERE name = 'ETLDatabase';
SELECT name, is_tracked_by_cdc FROM sys.tables WHERE name = 'Sales';

-- Check CDC capture instances
SELECT * FROM sys.dm_cdc_capture_instances;

-- Check CDC jobs
SELECT name, enabled FROM msdb.dbo.sysjobs WHERE name LIKE '%cdc%';
```

#### Spark Issues
```bash
# Check Spark application status
curl http://spark-master:8080/api/v1/applications

# Check Spark logs
tail -f logs/spark_multidb_delta.log

# Check Spark UI for errors
open http://spark-master:8080

# Check executor logs
ssh spark-worker-1 "tail -f /opt/spark/logs/spark-*-executor-*.out"
```

#### Database Issues
```sql
-- Test database connections
SELECT name, state_desc FROM sys.databases;
SELECT * FROM sys.dm_exec_connections;

-- Check authentication
SELECT name, type_desc, is_disabled FROM sys.server_principals;

-- Check resource usage
SELECT * FROM sys.dm_exec_requests WHERE session_id > 50;
```

### Debugging Steps

1. **Check Application Logs**
   ```bash
   tail -f logs/spark_multidb_delta.log
   ```

2. **Verify Configuration**
   ```sql
   SELECT * FROM dbo.Connections WHERE ProcessingEngine = 'Spark';
   SELECT * FROM dbo.CDCStatus WHERE IsEnabled = 1;
   ```

3. **Check Processing History**
   ```sql
   SELECT * FROM dbo.ProcessingHistory 
   WHERE ProcessingEngine = 'Spark' 
   ORDER BY StartTime DESC;
   ```

4. **Monitor Resource Usage**
   ```bash
   # Check memory usage
   free -h
   
   # Check disk usage
   df -h
   
   # Check network connectivity
   ping spark-master
   ```

## Deployment

### 1. Production Deployment

#### Docker Deployment
```dockerfile
FROM mcr.microsoft.com/dotnet/runtime:8.0

# Install Java and Spark
RUN apt-get update && apt-get install -y openjdk-11-jdk

# Copy application files
COPY Spark_MultiDB_Delta /app/
COPY spark /opt/spark/

# Set environment variables
ENV SPARK_HOME=/opt/spark
ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64

# Run application
ENTRYPOINT ["dotnet", "/app/Spark_MultiDB_Delta.dll"]
```

#### Kubernetes Deployment
```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: spark-multidb-delta
spec:
  replicas: 1
  selector:
    matchLabels:
      app: spark-multidb-delta
  template:
    metadata:
      labels:
        app: spark-multidb-delta
    spec:
      containers:
      - name: spark-multidb-delta
        image: etl/spark-multidb-delta:latest
        resources:
          requests:
            memory: "4Gi"
            cpu: "2"
          limits:
            memory: "8Gi"
            cpu: "4"
        env:
        - name: ASPNETCORE_ENVIRONMENT
          value: "Production"
        - name: SPARK_MASTER
          value: "spark://spark-master:7077"
```

### 2. Monitoring and Alerting

#### Prometheus Metrics
```csharp
// Add Prometheus metrics
services.AddMetrics()
    .AddPrometheusFormatter()
    .AddHealthChecks()
    .AddCheck<SparkHealthCheck>("spark")
    .AddCheck<DatabaseHealthCheck>("database");
```

#### Grafana Dashboards
```json
{
  "dashboard": {
    "title": "Spark MultiDB Delta Processing",
    "panels": [
      {
        "title": "Processing Rate",
        "type": "graph",
        "targets": [
          {
            "expr": "rate(spark_records_processed_total[5m])"
          }
        ]
      },
      {
        "title": "Error Rate",
        "type": "graph",
        "targets": [
          {
            "expr": "rate(spark_errors_total[5m])"
          }
        ]
      }
    ]
  }
}
```

## Conclusion

The .NET Spark MultiDB Delta application provides a robust, scalable solution for processing delta data from multiple CDC-enabled SQL Server databases. With comprehensive error handling, performance optimization, and monitoring capabilities, it can handle high-volume data processing requirements while maintaining data integrity and providing detailed insights into processing performance.

The application is designed to be production-ready with proper logging, error handling, and recovery mechanisms. Regular monitoring and maintenance will ensure optimal performance and reliability in enterprise environments.
