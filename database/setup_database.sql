-- =============================================
-- SSIS Scalable ETL - Database Setup Script
-- =============================================
-- This script sets up the staging database for the SSIS Transform Component
-- Run this script on your staging database before using the SSIS package

USE [YourStagingDatabase] -- Replace with your actual database name
GO

-- =============================================
-- Create Staging Table
-- =============================================
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[staging_transformed_data]') AND type in (N'U'))
BEGIN
    PRINT 'Creating staging_transformed_data table...'
    
    CREATE TABLE [dbo].[staging_transformed_data] (
        [Id] INT IDENTITY(1,1) PRIMARY KEY,
        [SourceId] INT NOT NULL,
        [TransformedString] NVARCHAR(MAX) NULL,
        [ProcessedDate] DATETIME2 NOT NULL,
        [ErrorMessage] NVARCHAR(MAX) NULL,
        [HasError] BIT NOT NULL DEFAULT(0),
        [BatchId] UNIQUEIDENTIFIER NOT NULL,
        [CreatedDate] DATETIME2 NOT NULL DEFAULT(GETUTCDATE())
    )
    
    PRINT 'Table staging_transformed_data created successfully.'
END
ELSE
BEGIN
    PRINT 'Table staging_transformed_data already exists.'
END
GO

-- =============================================
-- Create Performance Indexes
-- =============================================
PRINT 'Creating performance indexes...'

-- Index on SourceId for lookups
IF NOT EXISTS (SELECT * FROM sys.indexes WHERE name = 'IX_staging_transformed_data_SourceId' AND object_id = OBJECT_ID('staging_transformed_data'))
BEGIN
    CREATE NONCLUSTERED INDEX IX_staging_transformed_data_SourceId 
    ON [dbo].[staging_transformed_data] ([SourceId])
    PRINT 'Index IX_staging_transformed_data_SourceId created.'
END

-- Index on ProcessedDate for date range queries
IF NOT EXISTS (SELECT * FROM sys.indexes WHERE name = 'IX_staging_transformed_data_ProcessedDate' AND object_id = OBJECT_ID('staging_transformed_data'))
BEGIN
    CREATE NONCLUSTERED INDEX IX_staging_transformed_data_ProcessedDate 
    ON [dbo].[staging_transformed_data] ([ProcessedDate])
    PRINT 'Index IX_staging_transformed_data_ProcessedDate created.'
END

-- Index on HasError for error analysis
IF NOT EXISTS (SELECT * FROM sys.indexes WHERE name = 'IX_staging_transformed_data_HasError' AND object_id = OBJECT_ID('staging_transformed_data'))
BEGIN
    CREATE NONCLUSTERED INDEX IX_staging_transformed_data_HasError 
    ON [dbo].[staging_transformed_data] ([HasError])
    PRINT 'Index IX_staging_transformed_data_HasError created.'
END

-- Composite index for batch processing
IF NOT EXISTS (SELECT * FROM sys.indexes WHERE name = 'IX_staging_transformed_data_BatchId_ProcessedDate' AND object_id = OBJECT_ID('staging_transformed_data'))
BEGIN
    CREATE NONCLUSTERED INDEX IX_staging_transformed_data_BatchId_ProcessedDate 
    ON [dbo].[staging_transformed_data] ([BatchId], [ProcessedDate])
    PRINT 'Index IX_staging_transformed_data_BatchId_ProcessedDate created.'
END
GO

-- =============================================
-- Create Stored Procedures for Maintenance
-- =============================================

-- Procedure to clean up old staging data
IF EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[sp_CleanupStagingData]') AND type in (N'P', N'PC'))
    DROP PROCEDURE [dbo].[sp_CleanupStagingData]
GO

CREATE PROCEDURE [dbo].[sp_CleanupStagingData]
    @RetentionDays INT = 30,
    @BatchSize INT = 10000
AS
BEGIN
    SET NOCOUNT ON;
    
    DECLARE @CutoffDate DATETIME2 = DATEADD(day, -@RetentionDays, GETUTCDATE());
    DECLARE @DeletedRows INT = 0;
    DECLARE @TotalDeleted INT = 0;
    DECLARE @StartTime DATETIME2 = GETUTCDATE();
    
    PRINT 'Starting cleanup of staging data older than ' + CAST(@RetentionDays AS VARCHAR) + ' days...';
    PRINT 'Cutoff date: ' + CAST(@CutoffDate AS VARCHAR);
    
    WHILE EXISTS (SELECT 1 FROM staging_transformed_data WHERE ProcessedDate < @CutoffDate)
    BEGIN
        DELETE TOP (@BatchSize) FROM staging_transformed_data 
        WHERE ProcessedDate < @CutoffDate;
        
        SET @DeletedRows = @@ROWCOUNT;
        SET @TotalDeleted = @TotalDeleted + @DeletedRows;
        
        IF @DeletedRows > 0
        BEGIN
            PRINT 'Deleted ' + CAST(@DeletedRows AS VARCHAR) + ' rows. Total deleted: ' + CAST(@TotalDeleted AS VARCHAR);
            
            -- Small delay to prevent blocking
            WAITFOR DELAY '00:00:00.100';
        END
        ELSE
        BEGIN
            BREAK;
        END
    END
    
    DECLARE @Duration FLOAT = DATEDIFF(MILLISECOND, @StartTime, GETUTCDATE()) / 1000.0;
    PRINT 'Cleanup completed. Total rows deleted: ' + CAST(@TotalDeleted AS VARCHAR) + ' in ' + CAST(@Duration AS VARCHAR) + ' seconds.';
END
GO

-- Procedure to get processing statistics
IF EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[sp_GetStagingStatistics]') AND type in (N'P', N'PC'))
    DROP PROCEDURE [dbo].[sp_GetStagingStatistics]
GO

CREATE PROCEDURE [dbo].[sp_GetStagingStatistics]
    @HoursBack INT = 24
AS
BEGIN
    SET NOCOUNT ON;
    
    DECLARE @StartDate DATETIME2 = DATEADD(HOUR, -@HoursBack, GETUTCDATE());
    
    SELECT 
        'Processing Statistics' as ReportType,
        COUNT(*) as TotalRows,
        SUM(CASE WHEN HasError = 1 THEN 1 ELSE 0 END) as ErrorRows,
        SUM(CASE WHEN HasError = 0 THEN 1 ELSE 0 END) as SuccessRows,
        CAST(SUM(CASE WHEN HasError = 1 THEN 1 ELSE 0 END) * 100.0 / NULLIF(COUNT(*), 0) AS DECIMAL(5,2)) as ErrorPercentage,
        MIN(ProcessedDate) as FirstProcessedDate,
        MAX(ProcessedDate) as LastProcessedDate,
        COUNT(DISTINCT BatchId) as TotalBatches,
        CAST(COUNT(*) * 1.0 / NULLIF(DATEDIFF(SECOND, MIN(ProcessedDate), MAX(ProcessedDate)), 0) AS DECIMAL(10,2)) as RowsPerSecond
    FROM staging_transformed_data 
    WHERE ProcessedDate >= @StartDate;
    
    -- Recent errors
    SELECT TOP 10
        'Recent Errors' as ReportType,
        SourceId,
        ErrorMessage,
        ProcessedDate,
        BatchId
    FROM staging_transformed_data 
    WHERE HasError = 1 AND ProcessedDate >= @StartDate
    ORDER BY ProcessedDate DESC;
    
    -- Batch performance
    SELECT 
        'Batch Performance' as ReportType,
        BatchId,
        COUNT(*) as RowsInBatch,
        SUM(CASE WHEN HasError = 1 THEN 1 ELSE 0 END) as ErrorsInBatch,
        MIN(ProcessedDate) as BatchStartTime,
        MAX(ProcessedDate) as BatchEndTime,
        DATEDIFF(MILLISECOND, MIN(ProcessedDate), MAX(ProcessedDate)) as BatchDurationMs
    FROM staging_transformed_data 
    WHERE ProcessedDate >= @StartDate
    GROUP BY BatchId
    ORDER BY MIN(ProcessedDate) DESC;
END
GO

-- Procedure to rebuild indexes
IF EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[sp_RebuildStagingIndexes]') AND type in (N'P', N'PC'))
    DROP PROCEDURE [dbo].[sp_RebuildStagingIndexes]
GO

CREATE PROCEDURE [dbo].[sp_RebuildStagingIndexes]
AS
BEGIN
    SET NOCOUNT ON;
    
    DECLARE @StartTime DATETIME2 = GETUTCDATE();
    
    PRINT 'Starting index rebuild on staging_transformed_data...';
    
    ALTER INDEX ALL ON [dbo].[staging_transformed_data] REBUILD;
    
    DECLARE @Duration FLOAT = DATEDIFF(MILLISECOND, @StartTime, GETUTCDATE()) / 1000.0;
    PRINT 'Index rebuild completed in ' + CAST(@Duration AS VARCHAR) + ' seconds.';
    
    -- Update statistics
    PRINT 'Updating statistics...';
    UPDATE STATISTICS [dbo].[staging_transformed_data];
    PRINT 'Statistics updated successfully.';
END
GO

-- =============================================
-- Create Views for Monitoring
-- =============================================

-- View for real-time monitoring
IF EXISTS (SELECT * FROM sys.views WHERE name = 'vw_StagingMonitoring')
    DROP VIEW [dbo].[vw_StagingMonitoring]
GO

CREATE VIEW [dbo].[vw_StagingMonitoring]
AS
SELECT 
    GETUTCDATE() as CurrentTime,
    COUNT(*) as TotalRows,
    SUM(CASE WHEN HasError = 1 THEN 1 ELSE 0 END) as ErrorRows,
    SUM(CASE WHEN HasError = 0 THEN 1 ELSE 0 END) as SuccessRows,
    CAST(SUM(CASE WHEN HasError = 1 THEN 1 ELSE 0 END) * 100.0 / NULLIF(COUNT(*), 0) AS DECIMAL(5,2)) as ErrorPercentage,
    MIN(ProcessedDate) as FirstProcessedDate,
    MAX(ProcessedDate) as LastProcessedDate,
    COUNT(DISTINCT BatchId) as TotalBatches,
    DATEDIFF(MINUTE, MIN(ProcessedDate), MAX(ProcessedDate)) as ProcessingDurationMinutes
FROM staging_transformed_data
GO

-- View for recent activity
IF EXISTS (SELECT * FROM sys.views WHERE name = 'vw_RecentActivity')
    DROP VIEW [dbo].[vw_RecentActivity]
GO

CREATE VIEW [dbo].[vw_RecentActivity]
AS
SELECT TOP 100
    Id,
    SourceId,
    TransformedString,
    ProcessedDate,
    HasError,
    ErrorMessage,
    BatchId,
    DATEDIFF(MILLISECOND, LAG(ProcessedDate) OVER (ORDER BY ProcessedDate), ProcessedDate) as TimeSincePreviousRow
FROM staging_transformed_data
ORDER BY ProcessedDate DESC
GO

-- =============================================
-- Create Sample Data (Optional)
-- =============================================
-- Uncomment the following section to create sample data for testing

/*
PRINT 'Creating sample data for testing...'

DECLARE @i INT = 1;
DECLARE @batchId UNIQUEIDENTIFIER = NEWID();

WHILE @i <= 1000
BEGIN
    INSERT INTO staging_transformed_data (SourceId, TransformedString, ProcessedDate, HasError, BatchId)
    VALUES 
        (@i, 'SAMPLE DATA ' + CAST(@i AS VARCHAR), GETUTCDATE(), 0, @batchId),
        (@i + 1000, NULL, GETUTCDATE(), 1, @batchId);
    
    SET @i = @i + 1;
END

PRINT 'Sample data created successfully.';
*/

-- =============================================
-- Setup Complete
-- =============================================
PRINT '=============================================';
PRINT 'SSIS Scalable ETL Database Setup Complete!';
PRINT '=============================================';
PRINT '';
PRINT 'Created objects:';
PRINT '- Table: staging_transformed_data';
PRINT '- Indexes: 4 performance indexes';
PRINT '- Stored Procedures: 3 maintenance procedures';
PRINT '- Views: 2 monitoring views';
PRINT '';
PRINT 'Next steps:';
PRINT '1. Update the connection string in your SSIS package';
PRINT '2. Configure the Script Component with the provided C# code';
PRINT '3. Test with a small dataset first';
PRINT '4. Monitor performance and adjust batch sizes as needed';
PRINT '';
PRINT 'Useful commands:';
PRINT '- EXEC sp_GetStagingStatistics 24  -- Get last 24 hours stats';
PRINT '- EXEC sp_CleanupStagingData 30    -- Clean up data older than 30 days';
PRINT '- EXEC sp_RebuildStagingIndexes    -- Rebuild indexes for performance';
PRINT '- SELECT * FROM vw_StagingMonitoring -- Real-time monitoring';
PRINT '=============================================';
