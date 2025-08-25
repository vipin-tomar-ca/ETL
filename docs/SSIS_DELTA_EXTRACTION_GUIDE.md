# SSIS Delta Extraction Package Guide

## Overview

The `Extract_Delta.dtsx` SSIS package is designed to extract delta (changed) data from multiple SQL Server databases using both Change Data Capture (CDC) and timestamp-based methods. The package supports high-volume data processing (1 TB/day) with parallel processing capabilities and comprehensive error handling.

## Architecture

### Package Structure

```
Extract_Delta.dtsx
├── Connection Managers
│   ├── MetadataDB (ETLMetadata database)
│   ├── StagingDB (ETLStaging database)
│   └── ParquetFileSystem (File system for Parquet files)
├── Variables
│   ├── CurrentDatabaseName, CurrentConnectionString
│   ├── IsCDCEnabled, LastProcessedLSN, DeltaCondition
│   ├── TableName, SchemaName, StagingTableName
│   ├── ProcessingEngine, BatchID, ErrorMessage
│   └── RecordsProcessed, RecordsFailed
├── Main Sequence
│   ├── Initialize Package
│   ├── Get Database List
│   ├── Foreach Database Loop
│   │   ├── Set Current Database Variables
│   │   ├── CDC Processing (for CDC-enabled databases)
│   │   └── Non-CDC Processing (for timestamp-based databases)
│   └── Finalize Package
└── Event Handlers
    ├── OnError (Log errors to database)
    └── OnWarning (Log warnings)
```

## Key Features

### ✅ **CDC Support**
- **CDC Source Components** - Uses native CDC source components for CDC-enabled databases
- **LSN Tracking** - Maintains LastProcessedLSN for incremental processing
- **Net Changes Support** - Configurable for all changes or net changes only
- **Capture Instance Management** - Automatic capture instance detection

### ✅ **Timestamp-Based Delta**
- **OLE DB Source** - Uses parameterized queries with timestamp conditions
- **Flexible Delta Conditions** - Configurable delta columns (ModifiedDate, LastUpdated, etc.)
- **LastRunTimestamp Tracking** - Maintains processing timestamps

### ✅ **Multi-Engine Support**
- **Spark Processing** - Outputs to Parquet files for Spark processing
- **C# Processing** - Outputs to SQL Server staging tables for C# processing
- **Conditional Split** - Routes data based on ProcessingEngine configuration

### ✅ **Performance Optimization**
- **Parallel Processing** - Supports up to 4 concurrent database processing
- **Buffer Optimization** - 10MB buffer size with 10,000 row batches
- **Fast Load** - Uses SQL Server Fast Load for staging tables
- **Parquet Compression** - Snappy compression for optimal file sizes

### ✅ **Error Handling**
- **Event Handlers** - Comprehensive error and warning logging
- **Database Logging** - Logs errors to ETLMetadata.Logs table
- **Retry Logic** - Configurable retry counts and timeouts
- **Graceful Failures** - Continues processing other databases on failure

## Database Schema

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
    LastRunTimestamp DATETIME2 NULL,
    DeltaCondition NVARCHAR(500) NULL,
    TableName NVARCHAR(128) NOT NULL,
    SchemaName NVARCHAR(128) NOT NULL DEFAULT 'dbo',
    StagingTableName NVARCHAR(128) NULL,
    ProcessingEngine NVARCHAR(50) NOT NULL DEFAULT 'CSharp',
    IsActive BIT NOT NULL DEFAULT 1,
    BatchSize INT NOT NULL DEFAULT 10000,
    TimeoutMinutes INT NOT NULL DEFAULT 30,
    -- ... other fields
);
```

#### Logs Table
```sql
CREATE TABLE dbo.Logs
(
    LogID BIGINT IDENTITY(1,1) PRIMARY KEY,
    BatchID NVARCHAR(50) NOT NULL,
    DatabaseName NVARCHAR(128) NULL,
    LogLevel NVARCHAR(20) NOT NULL,
    LogMessage NVARCHAR(MAX) NOT NULL,
    ErrorMessage NVARCHAR(MAX) NULL,
    RecordsProcessed BIGINT NULL,
    RecordsFailed BIGINT NULL,
    -- ... other fields
);
```

#### ProcessingHistory Table
```sql
CREATE TABLE dbo.ProcessingHistory
(
    HistoryID BIGINT IDENTITY(1,1) PRIMARY KEY,
    BatchID NVARCHAR(50) NOT NULL,
    ConnectionID INT NOT NULL,
    DatabaseName NVARCHAR(128) NOT NULL,
    Status NVARCHAR(20) NOT NULL,
    StartTime DATETIME2 NOT NULL,
    EndTime DATETIME2 NULL,
    RecordsProcessed BIGINT NOT NULL DEFAULT 0,
    RecordsFailed BIGINT NOT NULL DEFAULT 0,
    FilePath NVARCHAR(500) NULL,
    -- ... other fields
);
```

## Configuration

### 1. Database Connections

#### CDC-Enabled Database
```sql
INSERT INTO dbo.Connections 
(DatabaseName, ConnectionString, IsCDCEnabled, DeltaCondition, TableName, SchemaName, StagingTableName, ProcessingEngine)
VALUES 
('ETLDatabase', 'Data Source=ETL-SERVER;Initial Catalog=ETLDatabase;Integrated Security=SSPI;', 
 1, 'ModifiedDate', 'Sales', 'dbo', 'staging.Sales_Delta', 'Spark');
```

#### Non-CDC Database
```sql
INSERT INTO dbo.Connections 
(DatabaseName, ConnectionString, IsCDCEnabled, DeltaCondition, TableName, SchemaName, StagingTableName, ProcessingEngine)
VALUES 
('LegacyDatabase', 'Data Source=ETL-SERVER;Initial Catalog=LegacyDatabase;Integrated Security=SSPI;', 
 0, 'LastUpdated', 'Orders', 'dbo', 'staging.Orders_Delta', 'CSharp');
```

### 2. CDC Configuration

#### Enable CDC for Database
```sql
-- Enable CDC at database level
EXEC sys.sp_cdc_enable_db;

-- Enable CDC for specific table
EXEC sys.sp_cdc_enable_table
    @source_schema = N'dbo',
    @source_name = N'Sales',
    @role_name = NULL,
    @supports_net_changes = 1;
```

#### CDC Status Tracking
```sql
INSERT INTO dbo.CDCStatus 
(ConnectionID, CaptureInstance, SourceSchema, SourceTable, IsEnabled, SupportsNetChanges)
VALUES 
(1, 'dbo_Sales', 'dbo', 'Sales', 1, 1);
```

### 3. Staging Table Configuration

#### For C# Processing
```sql
INSERT INTO dbo.StagingTables 
(ConnectionID, StagingTableName, StagingSchema, PartitionStrategy, CompressionType, CleanupStrategy, RetentionDays)
VALUES 
(1, 'Sales_Delta', 'staging', 'DATE', 'PAGE', 'CLUSTERED', 'DELETE', 7);
```

## Package Execution

### 1. Prerequisites

#### SQL Server Requirements
- **SQL Server Edition**: Enterprise, Developer, or Standard (CDC not available in Express)
- **Recovery Model**: FULL (required for CDC)
- **SQL Server Agent**: Must be running for CDC jobs
- **Permissions**: sysadmin or db_owner role

#### Database Setup
```sql
-- Run metadata database setup
EXEC database/Create_Metadata_Tables.sql

-- Run CDC setup for target databases
EXEC database/Enable_CDC.sql
```

### 2. Package Execution

#### Command Line Execution
```bash
# Execute package using DTExec
DTExec /File "Extract_Delta.dtsx" /Server "ETL-SERVER" /MaxConcurrentExecutables 4

# Execute with logging
DTExec /File "Extract_Delta.dtsx" /Server "ETL-SERVER" /LogLevel 3 /Log "C:\ETL\Logs\Extract_Delta.log"
```

#### SQL Server Agent Job
```sql
-- Create SQL Server Agent job
USE msdb;
GO

EXEC dbo.sp_add_job
    @job_name = N'Extract_Delta_Job',
    @enabled = 1;

EXEC dbo.sp_add_jobstep
    @job_name = N'Extract_Delta_Job',
    @step_name = N'Execute Extract_Delta Package',
    @subsystem = N'SSIS',
    @command = N'/File "C:\ETL\Packages\Extract_Delta.dtsx" /Server "ETL-SERVER"';

EXEC dbo.sp_add_schedule
    @schedule_name = N'Extract_Delta_Schedule',
    @freq_type = 4, -- Daily
    @freq_interval = 1,
    @active_start_time = 020000; -- 2:00 AM
```

### 3. Monitoring and Logging

#### Check Processing Status
```sql
-- Get recent processing history
SELECT * FROM dbo.vw_RecentProcessingHistory;

-- Get processing statistics for specific batch
EXEC dbo.GetProcessingStatistics @BatchID = 'your-batch-id';

-- Check active connections
SELECT * FROM dbo.vw_ActiveConnections;
```

#### Monitor CDC Status
```sql
-- Check CDC capture instances
SELECT * FROM sys.dm_cdc_capture_instances;

-- Check CDC log scan sessions
SELECT * FROM sys.dm_cdc_log_scan_sessions ORDER BY start_time DESC;

-- Check CDC jobs
SELECT name, enabled, description FROM msdb.dbo.sysjobs WHERE name LIKE '%cdc%';
```

## Performance Optimization

### 1. Parallel Processing

#### Package Properties
```xml
<DTS:Properties>
    <DTS:Property DTS:Name="MaxConcurrentExecutables">4</DTS:Property>
    <DTS:Property DTS:Name="EngineThreads">4</DTS:Property>
    <DTS:Property DTS:Name="BufferSize">10485760</DTS:Property>
    <DTS:Property DTS:Name="DefaultBufferMaxRows">10000</DTS:Property>
</DTS:Properties>
```

#### Database-Level Optimization
```sql
-- Configure connection batch sizes
UPDATE dbo.Connections 
SET BatchSize = 50000, TimeoutMinutes = 60
WHERE ProcessingEngine = 'Spark';

-- Enable table partitioning for staging tables
ALTER TABLE staging.Sales_Delta 
PARTITION BY RANGE (SaleDate) (
    PARTITION p202401 VALUES LESS THAN ('2024-02-01'),
    PARTITION p202402 VALUES LESS THAN ('2024-03-01')
);
```

### 2. Memory and Buffer Management

#### Buffer Configuration
- **Buffer Size**: 10MB (10485760 bytes)
- **Buffer Max Rows**: 10,000 rows
- **Temp Storage Path**: C:\ETL\Temp
- **Auto Adjust Buffer Size**: Enabled

#### Memory Optimization
```sql
-- Configure SQL Server memory for staging database
ALTER DATABASE ETLStaging 
SET MEMORY_OPTIMIZED = ON;

-- Create memory-optimized staging tables
CREATE TABLE staging.Sales_Delta_Memory
(
    SaleID INT,
    CustomerID INT,
    ProductID INT,
    Quantity INT,
    UnitPrice DECIMAL(10,2),
    SaleDate DATETIME2,
    INDEX IX_SaleDate NONCLUSTERED (SaleDate)
) WITH (MEMORY_OPTIMIZED = ON);
```

### 3. File System Optimization

#### Parquet File Configuration
- **Compression**: Snappy (optimal for Spark)
- **Row Group Size**: 100,000 rows
- **File Size Target**: 128MB per file
- **Partition Strategy**: Date-based partitioning

#### Storage Configuration
```bash
# Configure file system for optimal performance
# Use SSD storage for temp files
mkdir -p C:\ETL\Temp
mkdir -p C:\ETL\Staging\Parquet

# Configure Windows file system optimization
fsutil behavior set memoryusage 2
fsutil behavior set mftzone 2
```

## Error Handling and Recovery

### 1. Error Categories

#### CDC Errors
- **CDC Not Enabled**: Database or table CDC not enabled
- **LSN Range Issues**: Invalid LSN range or missing LSN
- **Capture Instance Errors**: Capture instance not found or disabled

#### Connection Errors
- **Authentication Failures**: Invalid credentials or permissions
- **Network Timeouts**: Connection timeouts or network issues
- **Database Unavailable**: Database offline or maintenance mode

#### Data Processing Errors
- **Schema Mismatches**: Column changes or data type issues
- **Constraint Violations**: Primary key or foreign key violations
- **Resource Exhaustion**: Memory or disk space issues

### 2. Recovery Strategies

#### Automatic Recovery
```sql
-- Retry failed connections
UPDATE dbo.Connections 
SET RetryCount = RetryCount + 1
WHERE ConnectionID = @ConnectionID 
AND RetryCount < MaxRetries;

-- Reset failed processing
UPDATE dbo.ProcessingHistory 
SET Status = 'PENDING', RetryCount = RetryCount + 1
WHERE Status = 'FAILED' 
AND RetryCount < 3;
```

#### Manual Recovery
```sql
-- Reset CDC processing for specific database
UPDATE dbo.Connections 
SET LastProcessedLSN = NULL
WHERE DatabaseName = 'ETLDatabase';

-- Reset timestamp-based processing
UPDATE dbo.Connections 
SET LastRunTimestamp = DATEADD(HOUR, -24, GETDATE())
WHERE DatabaseName = 'LegacyDatabase';
```

### 3. Monitoring and Alerting

#### Error Monitoring
```sql
-- Create error monitoring view
CREATE VIEW dbo.vw_RecentErrors AS
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
ORDER BY l.CreatedDate DESC;

-- Set up SQL Server Agent alerts
EXEC msdb.dbo.sp_add_alert
    @name = N'ETL_Error_Alert',
    @message_id = 0,
    @severity = 0,
    @enabled = 1,
    @delay_between_responses = 300,
    @include_event_description_in = 1;
```

## Scaling for High Volume

### 1. Horizontal Scaling

#### Multiple Package Instances
```bash
# Run multiple package instances for different database groups
DTExec /File "Extract_Delta.dtsx" /Server "ETL-SERVER-1" /Set "\Package.Variables[DatabaseGroup].Properties[Value]";"Group1"
DTExec /File "Extract_Delta.dtsx" /Server "ETL-SERVER-2" /Set "\Package.Variables[DatabaseGroup].Properties[Value]";"Group2"
```

#### Database Sharding
```sql
-- Configure database sharding in metadata
UPDATE dbo.Connections 
SET ProcessingEngine = 'Spark',
    StagingTableName = 'staging.Sales_Delta_Shard1'
WHERE DatabaseName LIKE '%Shard1%';

UPDATE dbo.Connections 
SET ProcessingEngine = 'Spark',
    StagingTableName = 'staging.Sales_Delta_Shard2'
WHERE DatabaseName LIKE '%Shard2%';
```

### 2. Vertical Scaling

#### Resource Allocation
```sql
-- Configure SQL Server resource allocation
ALTER DATABASE ETLStaging 
SET MAXSIZE = 1TB;

-- Configure tempdb for high volume
ALTER DATABASE tempdb 
MODIFY FILE (NAME = tempdev, SIZE = 10GB, FILEGROWTH = 1GB);
```

#### Memory Optimization
```sql
-- Configure buffer pool extension
ALTER SERVER CONFIGURATION 
SET BUFFER POOL EXTENSION ON 
(FILENAME = 'D:\BufferPoolExtension.bpe', SIZE = 10GB);
```

## Best Practices

### 1. CDC Best Practices

#### CDC Configuration
- **Enable CDC** only on tables that require change tracking
- **Configure retention** based on business requirements (1-7 days typical)
- **Monitor CDC jobs** for performance and cleanup
- **Use net changes** when only final state is needed

#### LSN Management
- **Store LSNs** in metadata database for recovery
- **Validate LSN ranges** before processing
- **Handle LSN gaps** due to CDC cleanup
- **Backup LSN state** regularly

### 2. Performance Best Practices

#### Database Optimization
- **Index staging tables** for query performance
- **Partition large tables** by date or other criteria
- **Use compression** for storage efficiency
- **Configure statistics** for query optimization

#### Package Optimization
- **Tune buffer sizes** based on available memory
- **Use appropriate batch sizes** for destination systems
- **Monitor resource usage** during execution
- **Optimize data flow** paths

### 3. Monitoring Best Practices

#### Logging Strategy
- **Log all errors** with sufficient detail for troubleshooting
- **Track performance metrics** for optimization
- **Monitor resource usage** to prevent bottlenecks
- **Set up alerts** for critical failures

#### Maintenance Tasks
- **Clean up old logs** regularly to prevent database bloat
- **Archive processing history** for long-term analysis
- **Update statistics** on metadata tables
- **Rebuild indexes** on staging tables

## Troubleshooting

### Common Issues

#### CDC Issues
```sql
-- Check CDC status
SELECT name, is_cdc_enabled FROM sys.databases WHERE name = 'ETLDatabase';
SELECT name, is_tracked_by_cdc FROM sys.tables WHERE name = 'Sales';

-- Check CDC jobs
SELECT name, enabled FROM msdb.dbo.sysjobs WHERE name LIKE '%cdc%';

-- Check CDC capture instances
SELECT * FROM sys.dm_cdc_capture_instances;
```

#### Performance Issues
```sql
-- Check buffer usage
SELECT * FROM sys.dm_exec_query_memory_grants;

-- Check tempdb usage
SELECT * FROM sys.dm_db_file_space_usage;

-- Check staging table performance
SELECT * FROM sys.dm_db_index_usage_stats 
WHERE database_id = DB_ID('ETLStaging');
```

#### Connection Issues
```sql
-- Test database connections
SELECT name, state_desc FROM sys.databases;
SELECT * FROM sys.dm_exec_connections;

-- Check authentication
SELECT name, type_desc, is_disabled FROM sys.server_principals;
```

### Debugging Steps

1. **Check Package Logs**
   ```sql
   SELECT * FROM dbo.Logs 
   WHERE BatchID = 'your-batch-id' 
   ORDER BY CreatedDate DESC;
   ```

2. **Verify CDC Configuration**
   ```sql
   SELECT * FROM dbo.vw_ActiveConnections 
   WHERE IsCDCEnabled = 1;
   ```

3. **Check Processing History**
   ```sql
   SELECT * FROM dbo.ProcessingHistory 
   WHERE Status = 'FAILED' 
   ORDER BY StartTime DESC;
   ```

4. **Monitor Resource Usage**
   ```sql
   SELECT * FROM sys.dm_exec_requests 
   WHERE session_id > 50;
   ```

## Conclusion

The `Extract_Delta.dtsx` SSIS package provides a robust, scalable solution for delta data extraction from multiple SQL Server databases. With support for both CDC and timestamp-based methods, comprehensive error handling, and performance optimization features, it can handle high-volume data processing requirements while maintaining data integrity and providing detailed monitoring capabilities.

The package is designed to be production-ready with proper logging, error handling, and recovery mechanisms. Regular monitoring and maintenance will ensure optimal performance and reliability in enterprise environments.
