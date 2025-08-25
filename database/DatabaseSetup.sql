-- ETL Scalable Database Setup Script
-- This script creates the necessary database objects for the ETL solution
-- Run this script on your SQL Server instance before using the DataTransformer

USE master;
GO

-- Create the staging database if it doesn't exist
IF NOT EXISTS (SELECT name FROM sys.databases WHERE name = 'ETL_Staging')
BEGIN
    CREATE DATABASE ETL_Staging;
END
GO

-- Create the logs database if it doesn't exist
IF NOT EXISTS (SELECT name FROM sys.databases WHERE name = 'ETL_Logs')
BEGIN
    CREATE DATABASE ETL_Logs;
END
GO

USE ETL_Staging;
GO

-- Create staging table for transformed data
-- Modify the columns based on your specific data requirements
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[StagingTable]') AND type in (N'U'))
BEGIN
    CREATE TABLE [dbo].[StagingTable] (
        [Id] INT IDENTITY(1,1) PRIMARY KEY,
        
        -- Sample columns - modify these based on your data structure
        [CustomerId] INT NULL,
        [FirstName] NVARCHAR(100) NULL,
        [LastName] NVARCHAR(100) NULL,
        [Email] NVARCHAR(255) NULL,
        [Phone] NVARCHAR(20) NULL,
        [Address] NVARCHAR(500) NULL,
        [City] NVARCHAR(100) NULL,
        [State] NVARCHAR(50) NULL,
        [Country] NVARCHAR(100) NULL,
        [PostalCode] NVARCHAR(20) NULL,
        [Age] INT NULL,
        [Gender] NVARCHAR(10) NULL,
        [Income] DECIMAL(18,2) NULL,
        [Status] NVARCHAR(50) NULL,
        
        -- ETL metadata columns
        [TransformationTimestamp] DATETIME2 NOT NULL,
        [SourceDatabase] NVARCHAR(100) NULL,
        [SourceTable] NVARCHAR(100) NULL,
        [BatchId] UNIQUEIDENTIFIER NULL,
        [CreatedDate] DATETIME2 DEFAULT GETUTCDATE(),
        
        -- Index for performance
        INDEX IX_StagingTable_TransformationTimestamp ([TransformationTimestamp]),
        INDEX IX_StagingTable_SourceDatabase ([SourceDatabase]),
        INDEX IX_StagingTable_CreatedDate ([CreatedDate])
    );
END
GO

-- Create a more flexible staging table with dynamic columns
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[StagingTableDynamic]') AND type in (N'U'))
BEGIN
    CREATE TABLE [dbo].[StagingTableDynamic] (
        [Id] INT IDENTITY(1,1) PRIMARY KEY,
        [ColumnName] NVARCHAR(100) NOT NULL,
        [ColumnValue] NVARCHAR(MAX) NULL,
        [DataType] NVARCHAR(50) NULL,
        [RecordId] UNIQUEIDENTIFIER NOT NULL,
        [TransformationTimestamp] DATETIME2 NOT NULL,
        [SourceDatabase] NVARCHAR(100) NULL,
        [SourceTable] NVARCHAR(100) NULL,
        [BatchId] UNIQUEIDENTIFIER NULL,
        [CreatedDate] DATETIME2 DEFAULT GETUTCDATE(),
        
        INDEX IX_StagingTableDynamic_RecordId ([RecordId]),
        INDEX IX_StagingTableDynamic_TransformationTimestamp ([TransformationTimestamp]),
        INDEX IX_StagingTableDynamic_SourceDatabase ([SourceDatabase])
    );
END
GO

-- Create batch tracking table
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[BatchTracking]') AND type in (N'U'))
BEGIN
    CREATE TABLE [dbo].[BatchTracking] (
        [BatchId] UNIQUEIDENTIFIER PRIMARY KEY,
        [SourceDatabase] NVARCHAR(100) NOT NULL,
        [SourceTable] NVARCHAR(100) NOT NULL,
        [BatchNumber] INT NOT NULL,
        [Offset] BIGINT NOT NULL,
        [BatchSize] INT NOT NULL,
        [RecordsProcessed] INT NOT NULL,
        [RecordsTransformed] INT NOT NULL,
        [StartTime] DATETIME2 NOT NULL,
        [EndTime] DATETIME2 NULL,
        [Duration] BIGINT NULL, -- milliseconds
        [Status] NVARCHAR(20) NOT NULL, -- Started, Completed, Failed
        [ErrorMessage] NVARCHAR(MAX) NULL,
        [CreatedDate] DATETIME2 DEFAULT GETUTCDATE(),
        
        INDEX IX_BatchTracking_SourceDatabase ([SourceDatabase]),
        INDEX IX_BatchTracking_Status ([Status]),
        INDEX IX_BatchTracking_StartTime ([StartTime])
    );
END
GO

USE ETL_Logs;
GO

-- Create logs table for tracking ETL operations
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[Logs]') AND type in (N'U'))
BEGIN
    CREATE TABLE [dbo].[Logs] (
        [Id] INT IDENTITY(1,1) PRIMARY KEY,
        [Operation] NVARCHAR(100) NOT NULL,
        [StartTime] DATETIME2 NOT NULL,
        [EndTime] DATETIME2 NULL,
        [Duration] BIGINT NULL, -- milliseconds
        [ProcessedRecords] BIGINT NULL,
        [TransformedRecords] BIGINT NULL,
        [ErrorCount] INT NULL,
        [Success] BIT NOT NULL,
        [Details] NVARCHAR(MAX) NULL,
        [ErrorMessage] NVARCHAR(MAX) NULL,
        [StackTrace] NVARCHAR(MAX) NULL,
        [SourceDatabase] NVARCHAR(100) NULL,
        [Severity] NVARCHAR(20) NULL,
        [CreatedDate] DATETIME2 DEFAULT GETUTCDATE(),
        
        INDEX IX_Logs_Operation ([Operation]),
        INDEX IX_Logs_StartTime ([StartTime]),
        INDEX IX_Logs_Success ([Success]),
        INDEX IX_Logs_SourceDatabase ([SourceDatabase])
    );
END
GO

-- Create error details table for detailed error tracking
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[ErrorDetails]') AND type in (N'U'))
BEGIN
    CREATE TABLE [dbo].[ErrorDetails] (
        [Id] INT IDENTITY(1,1) PRIMARY KEY,
        [LogId] INT NOT NULL,
        [ErrorType] NVARCHAR(100) NULL,
        [ErrorMessage] NVARCHAR(MAX) NOT NULL,
        [ErrorCode] NVARCHAR(50) NULL,
        [SourceDatabase] NVARCHAR(100) NULL,
        [SourceTable] NVARCHAR(100) NULL,
        [RecordId] NVARCHAR(100) NULL,
        [ColumnName] NVARCHAR(100) NULL,
        [ColumnValue] NVARCHAR(MAX) NULL,
        [Severity] NVARCHAR(20) NOT NULL,
        [Timestamp] DATETIME2 NOT NULL,
        [CreatedDate] DATETIME2 DEFAULT GETUTCDATE(),
        
        CONSTRAINT FK_ErrorDetails_Logs FOREIGN KEY ([LogId]) REFERENCES [dbo].[Logs]([Id]),
        INDEX IX_ErrorDetails_LogId ([LogId]),
        INDEX IX_ErrorDetails_Severity ([Severity]),
        INDEX IX_ErrorDetails_Timestamp ([Timestamp]),
        INDEX IX_ErrorDetails_SourceDatabase ([SourceDatabase])
    );
END
GO

-- Create performance metrics table
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[PerformanceMetrics]') AND type in (N'U'))
BEGIN
    CREATE TABLE [dbo].[PerformanceMetrics] (
        [Id] INT IDENTITY(1,1) PRIMARY KEY,
        [Operation] NVARCHAR(100) NOT NULL,
        [SourceDatabase] NVARCHAR(100) NULL,
        [StartTime] DATETIME2 NOT NULL,
        [EndTime] DATETIME2 NOT NULL,
        [Duration] BIGINT NOT NULL, -- milliseconds
        [RecordsProcessed] BIGINT NOT NULL,
        [RecordsTransformed] BIGINT NOT NULL,
        [RecordsPerSecond] DECIMAL(18,2) NULL,
        [MemoryUsageMB] DECIMAL(18,2) NULL,
        [CpuUsagePercent] DECIMAL(5,2) NULL,
        [NetworkUsageMB] DECIMAL(18,2) NULL,
        [BatchSize] INT NULL,
        [Parallelism] INT NULL,
        [Success] BIT NOT NULL,
        [CreatedDate] DATETIME2 DEFAULT GETUTCDATE(),
        
        INDEX IX_PerformanceMetrics_Operation ([Operation]),
        INDEX IX_PerformanceMetrics_StartTime ([StartTime]),
        INDEX IX_PerformanceMetrics_SourceDatabase ([SourceDatabase]),
        INDEX IX_PerformanceMetrics_Success ([Success])
    );
END
GO

-- Create stored procedures for common operations

-- Stored procedure to log transformation results
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[LogTransformationResult]') AND type in (N'P'))
BEGIN
    EXEC('CREATE PROCEDURE [dbo].[LogTransformationResult]
        @Operation NVARCHAR(100),
        @StartTime DATETIME2,
        @EndTime DATETIME2,
        @Duration BIGINT,
        @ProcessedRecords BIGINT,
        @TransformedRecords BIGINT,
        @ErrorCount INT,
        @Success BIT,
        @Details NVARCHAR(MAX),
        @SourceDatabase NVARCHAR(100) = NULL
    AS
    BEGIN
        INSERT INTO [dbo].[Logs] (
            [Operation], [StartTime], [EndTime], [Duration], 
            [ProcessedRecords], [TransformedRecords], [ErrorCount], 
            [Success], [Details], [SourceDatabase]
        )
        VALUES (
            @Operation, @StartTime, @EndTime, @Duration,
            @ProcessedRecords, @TransformedRecords, @ErrorCount,
            @Success, @Details, @SourceDatabase
        );
        
        SELECT SCOPE_IDENTITY() AS LogId;
    END');
END
GO

-- Stored procedure to log errors
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[LogError]') AND type in (N'P'))
BEGIN
    EXEC('CREATE PROCEDURE [dbo].[LogError]
        @Operation NVARCHAR(100),
        @ErrorMessage NVARCHAR(MAX),
        @StackTrace NVARCHAR(MAX) = NULL,
        @SourceDatabase NVARCHAR(100) = NULL,
        @Severity NVARCHAR(20) = ''Error''
    AS
    BEGIN
        INSERT INTO [dbo].[Logs] (
            [Operation], [StartTime], [Success], [ErrorMessage], 
            [StackTrace], [SourceDatabase], [Severity]
        )
        VALUES (
            @Operation, GETUTCDATE(), 0, @ErrorMessage,
            @StackTrace, @SourceDatabase, @Severity
        );
    END');
END
GO

-- Stored procedure to get performance summary
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[GetPerformanceSummary]') AND type in (N'P'))
BEGIN
    EXEC('CREATE PROCEDURE [dbo].[GetPerformanceSummary]
        @StartDate DATETIME2 = NULL,
        @EndDate DATETIME2 = NULL,
        @SourceDatabase NVARCHAR(100) = NULL
    AS
    BEGIN
        IF @StartDate IS NULL
            SET @StartDate = DATEADD(DAY, -7, GETUTCDATE());
        IF @EndDate IS NULL
            SET @EndDate = GETUTCDATE();
            
        SELECT 
            [Operation],
            COUNT(*) as ExecutionCount,
            AVG([Duration]) as AvgDuration,
            SUM([ProcessedRecords]) as TotalProcessed,
            SUM([TransformedRecords]) as TotalTransformed,
            AVG(CAST([TransformedRecords] AS FLOAT) / NULLIF([ProcessedRecords], 0)) as SuccessRate,
            MIN([StartTime]) as FirstExecution,
            MAX([StartTime]) as LastExecution
        FROM [dbo].[Logs] 
        WHERE [StartTime] BETWEEN @StartDate AND @EndDate
            AND (@SourceDatabase IS NULL OR [SourceDatabase] = @SourceDatabase)
        GROUP BY [Operation]
        ORDER BY [LastExecution] DESC;
    END');
END
GO

-- Stored procedure to cleanup old data
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[CleanupOldData]') AND type in (N'P'))
BEGIN
    EXEC('CREATE PROCEDURE [dbo].[CleanupOldData]
        @DaysToKeep INT = 30
    AS
    BEGIN
        DECLARE @CutoffDate DATETIME2 = DATEADD(DAY, -@DaysToKeep, GETUTCDATE());
        
        -- Cleanup old logs
        DELETE FROM [dbo].[Logs] 
        WHERE [StartTime] < @CutoffDate;
        
        -- Cleanup old error details
        DELETE FROM [dbo].[ErrorDetails] 
        WHERE [Timestamp] < @CutoffDate;
        
        -- Cleanup old performance metrics
        DELETE FROM [dbo].[PerformanceMetrics] 
        WHERE [StartTime] < @CutoffDate;
        
        -- Cleanup old staging data
        DELETE FROM [ETL_Staging].[dbo].[StagingTable] 
        WHERE [CreatedDate] < @CutoffDate;
        
        DELETE FROM [ETL_Staging].[dbo].[StagingTableDynamic] 
        WHERE [CreatedDate] < @CutoffDate;
        
        DELETE FROM [ETL_Staging].[dbo].[BatchTracking] 
        WHERE [CreatedDate] < @CutoffDate;
        
        PRINT ''Cleanup completed for data older than '' + CAST(@DaysToKeep AS VARCHAR) + '' days'';
    END');
END
GO

-- Create views for easy monitoring

-- View for recent transformation activity
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[vw_RecentTransformations]') AND type in (N'V'))
BEGIN
    EXEC('CREATE VIEW [dbo].[vw_RecentTransformations] AS
    SELECT 
        [Operation],
        [StartTime],
        [EndTime],
        [Duration],
        [ProcessedRecords],
        [TransformedRecords],
        [Success],
        [SourceDatabase],
        CAST([TransformedRecords] AS FLOAT) / NULLIF([ProcessedRecords], 0) as SuccessRate
    FROM [dbo].[Logs]
    WHERE [StartTime] >= DATEADD(HOUR, -24, GETUTCDATE())
    ORDER BY [StartTime] DESC');
END
GO

-- View for error summary
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[vw_ErrorSummary]') AND type in (N'V'))
BEGIN
    EXEC('CREATE VIEW [dbo].[vw_ErrorSummary] AS
    SELECT 
        [SourceDatabase],
        [Operation],
        COUNT(*) as ErrorCount,
        MAX([StartTime]) as LastError,
        [ErrorMessage]
    FROM [dbo].[Logs]
    WHERE [Success] = 0
        AND [StartTime] >= DATEADD(DAY, -7, GETUTCDATE())
    GROUP BY [SourceDatabase], [Operation], [ErrorMessage]
    ORDER BY [ErrorCount] DESC');
END
GO

-- Create sample data for testing (optional)
-- Uncomment the following section if you want to create sample data

/*
USE ETL_Staging;
GO

-- Insert sample data into staging table
INSERT INTO [dbo].[StagingTable] (
    [CustomerId], [FirstName], [LastName], [Email], [Phone], [Address], [City], [State], [Country], [PostalCode], [Age], [Gender], [Income], [Status], [TransformationTimestamp], [SourceDatabase], [SourceTable]
)
VALUES 
    (1, 'John', 'Doe', 'john.doe@email.com', '(555) 123-4567', '123 Main St', 'New York', 'NY', 'USA', '10001', 30, 'Male', 75000.00, 'Active', GETUTCDATE(), 'SampleDB', 'Customers'),
    (2, 'Jane', 'Smith', 'jane.smith@email.com', '(555) 987-6543', '456 Oak Ave', 'Los Angeles', 'CA', 'USA', '90210', 25, 'Female', 65000.00, 'Active', GETUTCDATE(), 'SampleDB', 'Customers'),
    (3, 'Bob', 'Johnson', 'bob.johnson@email.com', '(555) 456-7890', '789 Pine Rd', 'Chicago', 'IL', 'USA', '60601', 35, 'Male', 85000.00, 'Inactive', GETUTCDATE(), 'SampleDB', 'Customers');
*/

-- Print completion message
PRINT 'ETL Scalable database setup completed successfully!';
PRINT 'Created databases: ETL_Staging, ETL_Logs';
PRINT 'Created tables: StagingTable, StagingTableDynamic, BatchTracking, Logs, ErrorDetails, PerformanceMetrics';
PRINT 'Created stored procedures: LogTransformationResult, LogError, GetPerformanceSummary, CleanupOldData';
PRINT 'Created views: vw_RecentTransformations, vw_ErrorSummary';
PRINT '';
PRINT 'Next steps:';
PRINT '1. Modify the StagingTable schema to match your data requirements';
PRINT '2. Update connection strings in your application';
PRINT '3. Configure transformation rules';
PRINT '4. Run your ETL process';
