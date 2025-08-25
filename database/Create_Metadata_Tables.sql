-- =============================================
-- Create Metadata Tables for SSIS Delta Extraction
-- =============================================
-- This script creates the metadata database structure required for the Extract_Delta.dtsx package
-- The metadata database stores connection information, CDC settings, and processing history
-- =============================================

USE master;
GO

-- Create metadata database if it doesn't exist
IF NOT EXISTS (SELECT name FROM sys.databases WHERE name = 'ETLMetadata')
BEGIN
    PRINT 'Creating ETLMetadata database...';
    CREATE DATABASE ETLMetadata;
END
GO

USE ETLMetadata;
GO

-- =============================================
-- Create Connections Table
-- =============================================
-- Stores connection information and CDC settings for each database/table
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[Connections]') AND type in (N'U'))
BEGIN
    PRINT 'Creating Connections table...';
    
    CREATE TABLE dbo.Connections
    (
        ConnectionID INT IDENTITY(1,1) PRIMARY KEY,
        DatabaseName NVARCHAR(128) NOT NULL,
        ConnectionString NVARCHAR(MAX) NOT NULL,
        IsCDCEnabled BIT NOT NULL DEFAULT 0,
        LastProcessedLSN BINARY(10) NULL,
        LastRunTimestamp DATETIME2 NULL,
        DeltaCondition NVARCHAR(500) NULL, -- e.g., "ModifiedDate", "LastUpdated"
        TableName NVARCHAR(128) NOT NULL,
        SchemaName NVARCHAR(128) NOT NULL DEFAULT 'dbo',
        StagingTableName NVARCHAR(128) NULL,
        ProcessingEngine NVARCHAR(50) NOT NULL DEFAULT 'CSharp', -- 'Spark' or 'CSharp'
        IsActive BIT NOT NULL DEFAULT 1,
        CreatedDate DATETIME2 NOT NULL DEFAULT GETDATE(),
        LastUpdated DATETIME2 NOT NULL DEFAULT GETDATE(),
        RetryCount INT NOT NULL DEFAULT 0,
        MaxRetries INT NOT NULL DEFAULT 3,
        BatchSize INT NOT NULL DEFAULT 10000,
        TimeoutMinutes INT NOT NULL DEFAULT 30,
        Description NVARCHAR(500) NULL
    );
    
    -- Create indexes for performance
    CREATE INDEX IX_Connections_DatabaseName ON dbo.Connections(DatabaseName);
    CREATE INDEX IX_Connections_IsActive ON dbo.Connections(IsActive);
    CREATE INDEX IX_Connections_ProcessingEngine ON dbo.Connections(ProcessingEngine);
    
    PRINT 'Connections table created successfully.';
END
ELSE
BEGIN
    PRINT 'Connections table already exists.';
END
GO

-- =============================================
-- Create Logs Table
-- =============================================
-- Stores detailed logs for package execution and errors
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[Logs]') AND type in (N'U'))
BEGIN
    PRINT 'Creating Logs table...';
    
    CREATE TABLE dbo.Logs
    (
        LogID BIGINT IDENTITY(1,1) PRIMARY KEY,
        BatchID NVARCHAR(50) NOT NULL,
        DatabaseName NVARCHAR(128) NULL,
        TableName NVARCHAR(128) NULL,
        LogLevel NVARCHAR(20) NOT NULL, -- 'INFO', 'WARNING', 'ERROR', 'DEBUG'
        LogMessage NVARCHAR(MAX) NOT NULL,
        ErrorMessage NVARCHAR(MAX) NULL,
        ErrorCode INT NULL,
        ErrorSource NVARCHAR(500) NULL,
        RecordsProcessed BIGINT NULL,
        RecordsFailed BIGINT NULL,
        ProcessingTimeSeconds INT NULL,
        CreatedDate DATETIME2 NOT NULL DEFAULT GETDATE(),
        PackageName NVARCHAR(128) NULL,
        TaskName NVARCHAR(128) NULL,
        ExecutionID BIGINT NULL
    );
    
    -- Create indexes for performance
    CREATE INDEX IX_Logs_BatchID ON dbo.Logs(BatchID);
    CREATE INDEX IX_Logs_DatabaseName ON dbo.Logs(DatabaseName);
    CREATE INDEX IX_Logs_LogLevel ON dbo.Logs(LogLevel);
    CREATE INDEX IX_Logs_CreatedDate ON dbo.Logs(CreatedDate);
    CREATE INDEX IX_Logs_PackageName ON dbo.Logs(PackageName);
    
    PRINT 'Logs table created successfully.';
END
ELSE
BEGIN
    PRINT 'Logs table already exists.';
END
GO

-- =============================================
-- Create ProcessingHistory Table
-- =============================================
-- Stores processing history and statistics for each extraction
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[ProcessingHistory]') AND type in (N'U'))
BEGIN
    PRINT 'Creating ProcessingHistory table...';
    
    CREATE TABLE dbo.ProcessingHistory
    (
        HistoryID BIGINT IDENTITY(1,1) PRIMARY KEY,
        BatchID NVARCHAR(50) NOT NULL,
        ConnectionID INT NOT NULL,
        DatabaseName NVARCHAR(128) NOT NULL,
        TableName NVARCHAR(128) NOT NULL,
        ProcessingEngine NVARCHAR(50) NOT NULL,
        StartTime DATETIME2 NOT NULL,
        EndTime DATETIME2 NULL,
        ProcessingTimeSeconds AS DATEDIFF(SECOND, StartTime, EndTime),
        RecordsProcessed BIGINT NOT NULL DEFAULT 0,
        RecordsFailed BIGINT NOT NULL DEFAULT 0,
        LastProcessedLSN BINARY(10) NULL,
        LastRunTimestamp DATETIME2 NULL,
        Status NVARCHAR(20) NOT NULL DEFAULT 'RUNNING', -- 'RUNNING', 'COMPLETED', 'FAILED', 'CANCELLED'
        ErrorMessage NVARCHAR(MAX) NULL,
        RetryCount INT NOT NULL DEFAULT 0,
        FilePath NVARCHAR(500) NULL, -- Path to generated Parquet file
        FileSizeMB DECIMAL(10,2) NULL,
        CompressionRatio DECIMAL(5,2) NULL,
        CreatedDate DATETIME2 NOT NULL DEFAULT GETDATE(),
        
        CONSTRAINT FK_ProcessingHistory_Connections FOREIGN KEY (ConnectionID) REFERENCES dbo.Connections(ConnectionID)
    );
    
    -- Create indexes for performance
    CREATE INDEX IX_ProcessingHistory_BatchID ON dbo.ProcessingHistory(BatchID);
    CREATE INDEX IX_ProcessingHistory_DatabaseName ON dbo.ProcessingHistory(DatabaseName);
    CREATE INDEX IX_ProcessingHistory_Status ON dbo.ProcessingHistory(Status);
    CREATE INDEX IX_ProcessingHistory_StartTime ON dbo.ProcessingHistory(StartTime);
    CREATE INDEX IX_ProcessingHistory_ProcessingEngine ON dbo.ProcessingHistory(ProcessingEngine);
    
    PRINT 'ProcessingHistory table created successfully.';
END
ELSE
BEGIN
    PRINT 'ProcessingHistory table already exists.';
END
GO

-- =============================================
-- Create CDCStatus Table
-- =============================================
-- Stores CDC-specific status and configuration
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[CDCStatus]') AND type in (N'U'))
BEGIN
    PRINT 'Creating CDCStatus table...';
    
    CREATE TABLE dbo.CDCStatus
    (
        CDCStatusID INT IDENTITY(1,1) PRIMARY KEY,
        ConnectionID INT NOT NULL,
        CaptureInstance NVARCHAR(128) NOT NULL,
        SourceSchema NVARCHAR(128) NOT NULL,
        SourceTable NVARCHAR(128) NOT NULL,
        IsEnabled BIT NOT NULL DEFAULT 0,
        LastProcessedLSN BINARY(10) NULL,
        LastProcessedTime DATETIME2 NULL,
        RetentionPeriodMinutes INT NOT NULL DEFAULT 1440, -- 1 day
        CleanupJobEnabled BIT NOT NULL DEFAULT 1,
        CaptureJobEnabled BIT NOT NULL DEFAULT 1,
        SupportsNetChanges BIT NOT NULL DEFAULT 1,
        CaptureColumnList NVARCHAR(MAX) NULL,
        FileGroupName NVARCHAR(128) NULL,
        CreatedDate DATETIME2 NOT NULL DEFAULT GETDATE(),
        LastUpdated DATETIME2 NOT NULL DEFAULT GETDATE(),
        
        CONSTRAINT FK_CDCStatus_Connections FOREIGN KEY (ConnectionID) REFERENCES dbo.Connections(ConnectionID)
    );
    
    -- Create indexes for performance
    CREATE INDEX IX_CDCStatus_ConnectionID ON dbo.CDCStatus(ConnectionID);
    CREATE INDEX IX_CDCStatus_CaptureInstance ON dbo.CDCStatus(CaptureInstance);
    CREATE INDEX IX_CDCStatus_IsEnabled ON dbo.CDCStatus(IsEnabled);
    
    PRINT 'CDCStatus table created successfully.';
END
ELSE
BEGIN
    PRINT 'CDCStatus table already exists.';
END
GO

-- =============================================
-- Create StagingTables Table
-- =============================================
-- Stores staging table configurations for C# processing
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[StagingTables]') AND type in (N'U'))
BEGIN
    PRINT 'Creating StagingTables table...';
    
    CREATE TABLE dbo.StagingTables
    (
        StagingTableID INT IDENTITY(1,1) PRIMARY KEY,
        ConnectionID INT NOT NULL,
        StagingTableName NVARCHAR(128) NOT NULL,
        StagingSchema NVARCHAR(128) NOT NULL DEFAULT 'staging',
        TableStructure NVARCHAR(MAX) NULL, -- JSON structure of the table
        PartitionStrategy NVARCHAR(50) NULL, -- 'NONE', 'DATE', 'HASH'
        PartitionColumn NVARCHAR(128) NULL,
        CompressionType NVARCHAR(20) NULL, -- 'NONE', 'PAGE', 'ROW'
        IndexStrategy NVARCHAR(50) NULL, -- 'NONE', 'CLUSTERED', 'NONCLUSTERED'
        CleanupStrategy NVARCHAR(50) NULL, -- 'NONE', 'DELETE', 'TRUNCATE', 'ARCHIVE'
        RetentionDays INT NOT NULL DEFAULT 7,
        IsActive BIT NOT NULL DEFAULT 1,
        CreatedDate DATETIME2 NOT NULL DEFAULT GETDATE(),
        LastUpdated DATETIME2 NOT NULL DEFAULT GETDATE(),
        
        CONSTRAINT FK_StagingTables_Connections FOREIGN KEY (ConnectionID) REFERENCES dbo.Connections(ConnectionID)
    );
    
    -- Create indexes for performance
    CREATE INDEX IX_StagingTables_ConnectionID ON dbo.StagingTables(ConnectionID);
    CREATE INDEX IX_StagingTables_IsActive ON dbo.StagingTables(IsActive);
    
    PRINT 'StagingTables table created successfully.';
END
ELSE
BEGIN
    PRINT 'StagingTables table already exists.';
END
GO

-- =============================================
-- Insert Sample Data
-- =============================================
-- Insert sample connection configurations for testing

-- Sample CDC-enabled database
INSERT INTO dbo.Connections 
(DatabaseName, ConnectionString, IsCDCEnabled, DeltaCondition, TableName, SchemaName, StagingTableName, ProcessingEngine, Description)
VALUES 
('ETLDatabase', 'Data Source=ETL-SERVER;Initial Catalog=ETLDatabase;Integrated Security=SSPI;Application Name=SSIS_Extract_Delta;', 
 1, 'ModifiedDate', 'Sales', 'dbo', 'staging.Sales_Delta', 'Spark', 'Sample CDC-enabled database for Sales data');

-- Sample non-CDC database
INSERT INTO dbo.Connections 
(DatabaseName, ConnectionString, IsCDCEnabled, DeltaCondition, TableName, SchemaName, StagingTableName, ProcessingEngine, Description)
VALUES 
('LegacyDatabase', 'Data Source=ETL-SERVER;Initial Catalog=LegacyDatabase;Integrated Security=SSPI;Application Name=SSIS_Extract_Delta;', 
 0, 'LastUpdated', 'Orders', 'dbo', 'staging.Orders_Delta', 'CSharp', 'Sample non-CDC database for Orders data');

-- Sample CDC status
INSERT INTO dbo.CDCStatus 
(ConnectionID, CaptureInstance, SourceSchema, SourceTable, IsEnabled, SupportsNetChanges, CaptureColumnList)
VALUES 
(1, 'dbo_Sales', 'dbo', 'Sales', 1, 1, 'SaleID,CustomerID,ProductID,Quantity,UnitPrice,SaleDate,CreatedDate,ModifiedDate');

-- Sample staging table configuration
INSERT INTO dbo.StagingTables 
(ConnectionID, StagingTableName, StagingSchema, PartitionStrategy, CompressionType, IndexStrategy, CleanupStrategy, RetentionDays)
VALUES 
(1, 'Sales_Delta', 'staging', 'DATE', 'PAGE', 'CLUSTERED', 'DELETE', 7);

PRINT 'Sample data inserted successfully.';
GO

-- =============================================
-- Create Stored Procedures for Package Support
-- =============================================

-- Procedure to get active connections for processing
CREATE OR ALTER PROCEDURE dbo.GetActiveConnections
    @ProcessingEngine NVARCHAR(50) = NULL
AS
BEGIN
    SET NOCOUNT ON;
    
    SELECT 
        c.ConnectionID,
        c.DatabaseName,
        c.ConnectionString,
        c.IsCDCEnabled,
        c.LastProcessedLSN,
        c.LastRunTimestamp,
        c.DeltaCondition,
        c.TableName,
        c.SchemaName,
        c.StagingTableName,
        c.ProcessingEngine,
        c.BatchSize,
        c.TimeoutMinutes,
        cs.CaptureInstance,
        cs.SupportsNetChanges,
        cs.CaptureColumnList,
        st.PartitionStrategy,
        st.CompressionType
    FROM dbo.Connections c
    LEFT JOIN dbo.CDCStatus cs ON c.ConnectionID = cs.ConnectionID
    LEFT JOIN dbo.StagingTables st ON c.ConnectionID = st.ConnectionID
    WHERE c.IsActive = 1
    AND (@ProcessingEngine IS NULL OR c.ProcessingEngine = @ProcessingEngine)
    ORDER BY c.DatabaseName, c.TableName;
END
GO

-- Procedure to update processing status
CREATE OR ALTER PROCEDURE dbo.UpdateProcessingStatus
    @BatchID NVARCHAR(50),
    @ConnectionID INT,
    @Status NVARCHAR(20),
    @RecordsProcessed BIGINT = 0,
    @RecordsFailed BIGINT = 0,
    @LastProcessedLSN BINARY(10) = NULL,
    @LastRunTimestamp DATETIME2 = NULL,
    @ErrorMessage NVARCHAR(MAX) = NULL,
    @FilePath NVARCHAR(500) = NULL
AS
BEGIN
    SET NOCOUNT ON;
    
    -- Update or insert processing history
    IF EXISTS (SELECT 1 FROM dbo.ProcessingHistory WHERE BatchID = @BatchID AND ConnectionID = @ConnectionID)
    BEGIN
        UPDATE dbo.ProcessingHistory
        SET Status = @Status,
            EndTime = CASE WHEN @Status IN ('COMPLETED', 'FAILED', 'CANCELLED') THEN GETDATE() ELSE EndTime END,
            RecordsProcessed = @RecordsProcessed,
            RecordsFailed = @RecordsFailed,
            LastProcessedLSN = @LastProcessedLSN,
            LastRunTimestamp = @LastRunTimestamp,
            ErrorMessage = @ErrorMessage,
            FilePath = @FilePath
        WHERE BatchID = @BatchID AND ConnectionID = @ConnectionID;
    END
    ELSE
    BEGIN
        INSERT INTO dbo.ProcessingHistory 
        (BatchID, ConnectionID, DatabaseName, TableName, ProcessingEngine, StartTime, Status, RecordsProcessed, RecordsFailed, LastProcessedLSN, LastRunTimestamp, ErrorMessage, FilePath)
        SELECT 
            @BatchID, 
            @ConnectionID, 
            c.DatabaseName, 
            c.TableName, 
            c.ProcessingEngine, 
            GETDATE(), 
            @Status, 
            @RecordsProcessed, 
            @RecordsFailed, 
            @LastProcessedLSN, 
            @LastRunTimestamp, 
            @ErrorMessage, 
            @FilePath
        FROM dbo.Connections c
        WHERE c.ConnectionID = @ConnectionID;
    END
    
    -- Update connection last processed information
    UPDATE dbo.Connections
    SET LastProcessedLSN = @LastProcessedLSN,
        LastRunTimestamp = @LastRunTimestamp,
        LastUpdated = GETDATE()
    WHERE ConnectionID = @ConnectionID;
END
GO

-- Procedure to log messages
CREATE OR ALTER PROCEDURE dbo.LogMessage
    @BatchID NVARCHAR(50),
    @DatabaseName NVARCHAR(128) = NULL,
    @TableName NVARCHAR(128) = NULL,
    @LogLevel NVARCHAR(20),
    @LogMessage NVARCHAR(MAX),
    @ErrorMessage NVARCHAR(MAX) = NULL,
    @ErrorCode INT = NULL,
    @ErrorSource NVARCHAR(500) = NULL,
    @RecordsProcessed BIGINT = NULL,
    @RecordsFailed BIGINT = NULL,
    @ProcessingTimeSeconds INT = NULL,
    @PackageName NVARCHAR(128) = 'Extract_Delta',
    @TaskName NVARCHAR(128) = NULL
AS
BEGIN
    SET NOCOUNT ON;
    
    INSERT INTO dbo.Logs 
    (BatchID, DatabaseName, TableName, LogLevel, LogMessage, ErrorMessage, ErrorCode, ErrorSource, 
     RecordsProcessed, RecordsFailed, ProcessingTimeSeconds, PackageName, TaskName)
    VALUES 
    (@BatchID, @DatabaseName, @TableName, @LogLevel, @LogMessage, @ErrorMessage, @ErrorCode, @ErrorSource,
     @RecordsProcessed, @RecordsFailed, @ProcessingTimeSeconds, @PackageName, @TaskName);
END
GO

-- Procedure to get processing statistics
CREATE OR ALTER PROCEDURE dbo.GetProcessingStatistics
    @BatchID NVARCHAR(50) = NULL,
    @DatabaseName NVARCHAR(128) = NULL,
    @StartDate DATETIME2 = NULL,
    @EndDate DATETIME2 = NULL
AS
BEGIN
    SET NOCOUNT ON;
    
    SELECT 
        ph.BatchID,
        ph.DatabaseName,
        ph.TableName,
        ph.ProcessingEngine,
        ph.Status,
        ph.StartTime,
        ph.EndTime,
        ph.ProcessingTimeSeconds,
        ph.RecordsProcessed,
        ph.RecordsFailed,
        ph.FilePath,
        ph.FileSizeMB,
        ph.CompressionRatio,
        c.Description
    FROM dbo.ProcessingHistory ph
    INNER JOIN dbo.Connections c ON ph.ConnectionID = c.ConnectionID
    WHERE (@BatchID IS NULL OR ph.BatchID = @BatchID)
    AND (@DatabaseName IS NULL OR ph.DatabaseName = @DatabaseName)
    AND (@StartDate IS NULL OR ph.StartTime >= @StartDate)
    AND (@EndDate IS NULL OR ph.StartTime <= @EndDate)
    ORDER BY ph.StartTime DESC;
END
GO

-- =============================================
-- Create Views for Easy Reporting
-- =============================================

-- View for active connections with CDC status
CREATE OR ALTER VIEW dbo.vw_ActiveConnections
AS
SELECT 
    c.ConnectionID,
    c.DatabaseName,
    c.TableName,
    c.SchemaName,
    c.IsCDCEnabled,
    c.ProcessingEngine,
    c.LastProcessedLSN,
    c.LastRunTimestamp,
    c.DeltaCondition,
    c.StagingTableName,
    c.IsActive,
    c.CreatedDate,
    c.LastUpdated,
    cs.CaptureInstance,
    cs.IsEnabled AS CDCIsEnabled,
    cs.SupportsNetChanges,
    st.PartitionStrategy,
    st.CompressionType,
    st.CleanupStrategy,
    st.RetentionDays
FROM dbo.Connections c
LEFT JOIN dbo.CDCStatus cs ON c.ConnectionID = cs.ConnectionID
LEFT JOIN dbo.StagingTables st ON c.ConnectionID = st.ConnectionID
WHERE c.IsActive = 1;
GO

-- View for recent processing history
CREATE OR ALTER VIEW dbo.vw_RecentProcessingHistory
AS
SELECT TOP 100
    ph.BatchID,
    ph.DatabaseName,
    ph.TableName,
    ph.ProcessingEngine,
    ph.Status,
    ph.StartTime,
    ph.EndTime,
    ph.ProcessingTimeSeconds,
    ph.RecordsProcessed,
    ph.RecordsFailed,
    ph.FilePath,
    ph.FileSizeMB,
    ph.CompressionRatio,
    c.Description
FROM dbo.ProcessingHistory ph
INNER JOIN dbo.Connections c ON ph.ConnectionID = c.ConnectionID
ORDER BY ph.StartTime DESC;
GO

-- =============================================
-- Final Summary
-- =============================================
PRINT '=============================================';
PRINT 'Metadata Database Setup Complete';
PRINT '=============================================';
PRINT 'Tables Created:';
PRINT '1. Connections - Database connection and CDC settings';
PRINT '2. Logs - Detailed logging and error tracking';
PRINT '3. ProcessingHistory - Processing statistics and history';
PRINT '4. CDCStatus - CDC-specific configuration';
PRINT '5. StagingTables - Staging table configurations';
PRINT '';
PRINT 'Stored Procedures Created:';
PRINT '1. GetActiveConnections - Get active connections for processing';
PRINT '2. UpdateProcessingStatus - Update processing status and statistics';
PRINT '3. LogMessage - Log messages and errors';
PRINT '4. GetProcessingStatistics - Get processing statistics and reports';
PRINT '';
PRINT 'Views Created:';
PRINT '1. vw_ActiveConnections - Active connections with CDC status';
PRINT '2. vw_RecentProcessingHistory - Recent processing history';
PRINT '';
PRINT 'Sample data inserted for testing.';
PRINT '=============================================';

-- Display sample data
SELECT 'Active Connections' AS InfoType, COUNT(*) AS Count FROM dbo.vw_ActiveConnections
UNION ALL
SELECT 'CDC Enabled' AS InfoType, COUNT(*) AS Count FROM dbo.vw_ActiveConnections WHERE IsCDCEnabled = 1
UNION ALL
SELECT 'Spark Processing' AS InfoType, COUNT(*) AS Count FROM dbo.vw_ActiveConnections WHERE ProcessingEngine = 'Spark'
UNION ALL
SELECT 'C# Processing' AS InfoType, COUNT(*) AS Count FROM dbo.vw_ActiveConnections WHERE ProcessingEngine = 'CSharp';

GO
