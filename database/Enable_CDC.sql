-- =============================================
-- Enable Change Data Capture (CDC) Script
-- =============================================
-- This script enables CDC at the database level and for specific tables
-- CDC allows you to capture insert, update, and delete activity on SQL Server tables
-- 
-- Prerequisites:
-- 1. SQL Server Enterprise, Developer, or Standard Edition (CDC not available in Express)
-- 2. Database must be in FULL recovery model
-- 3. User must have sysadmin role or db_owner role
-- 4. SQL Server Agent service must be running
-- =============================================

USE master;
GO

-- =============================================
-- Step 1: Check SQL Server Edition
-- =============================================
-- CDC is only available in Enterprise, Developer, and Standard editions
-- Express and Web editions do not support CDC
IF SERVERPROPERTY('EngineEdition') IN (1, 2, 3) -- Enterprise, Standard, Personal
BEGIN
    PRINT 'SQL Server Edition supports CDC: ' + CAST(SERVERPROPERTY('EngineEdition') AS VARCHAR(10));
END
ELSE
BEGIN
    RAISERROR('CDC is not supported in this SQL Server edition. Enterprise, Developer, or Standard edition is required.', 16, 1);
    RETURN;
END
GO

-- =============================================
-- Step 2: Check if target database exists
-- =============================================
IF NOT EXISTS (SELECT name FROM sys.databases WHERE name = 'ETLDatabase')
BEGIN
    PRINT 'Creating database ETLDatabase...';
    CREATE DATABASE ETLDatabase;
END
GO

USE ETLDatabase;
GO

-- =============================================
-- Step 3: Check and set recovery model to FULL
-- =============================================
-- CDC requires FULL recovery model
IF (SELECT recovery_model_desc FROM sys.databases WHERE name = 'ETLDatabase') != 'FULL'
BEGIN
    PRINT 'Setting recovery model to FULL for CDC support...';
    ALTER DATABASE ETLDatabase SET RECOVERY FULL;
END
ELSE
BEGIN
    PRINT 'Database is already in FULL recovery model.';
END
GO

-- =============================================
-- Step 4: Check if CDC is already enabled at database level
-- =============================================
IF EXISTS (SELECT 1 FROM sys.databases WHERE name = 'ETLDatabase' AND is_cdc_enabled = 1)
BEGIN
    PRINT 'CDC is already enabled at the database level.';
END
ELSE
BEGIN
    PRINT 'Enabling CDC at the database level...';
    
    -- Enable CDC at database level
    EXEC sys.sp_cdc_enable_db;
    
    IF @@ERROR = 0
    BEGIN
        PRINT 'CDC successfully enabled at database level.';
    END
    ELSE
    BEGIN
        RAISERROR('Failed to enable CDC at database level. Check permissions and SQL Server Agent status.', 16, 1);
        RETURN;
    END
END
GO

-- =============================================
-- Step 5: Create sample table if it doesn't exist
-- =============================================
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[Sales]') AND type in (N'U'))
BEGIN
    PRINT 'Creating sample Sales table...';
    
    CREATE TABLE dbo.Sales
    (
        SaleID INT IDENTITY(1,1) PRIMARY KEY,
        CustomerID INT NOT NULL,
        ProductID INT NOT NULL,
        Quantity INT NOT NULL,
        UnitPrice DECIMAL(10,2) NOT NULL,
        SaleDate DATETIME2 NOT NULL DEFAULT GETDATE(),
        CreatedDate DATETIME2 NOT NULL DEFAULT GETDATE(),
        ModifiedDate DATETIME2 NOT NULL DEFAULT GETDATE()
    );
    
    -- Insert sample data
    INSERT INTO dbo.Sales (CustomerID, ProductID, Quantity, UnitPrice, SaleDate)
    VALUES 
        (1, 101, 2, 29.99, GETDATE()),
        (2, 102, 1, 49.99, GETDATE()),
        (3, 103, 3, 19.99, GETDATE());
        
    PRINT 'Sample Sales table created with test data.';
END
ELSE
BEGIN
    PRINT 'Sales table already exists.';
END
GO

-- =============================================
-- Step 6: Enable CDC for the Sales table
-- =============================================
-- Check if CDC is already enabled for the Sales table
IF EXISTS (SELECT 1 FROM sys.tables t 
           INNER JOIN sys.schemas s ON t.schema_id = s.schema_id
           WHERE t.name = 'Sales' AND s.name = 'dbo' AND t.is_tracked_by_cdc = 1)
BEGIN
    PRINT 'CDC is already enabled for the Sales table.';
END
ELSE
BEGIN
    PRINT 'Enabling CDC for the Sales table...';
    
    -- Enable CDC for the Sales table
    -- @supports_net_changes = 1 enables net changes (only the final state of each row)
    -- @role_name = NULL means no access control role is created
    EXEC sys.sp_cdc_enable_table
        @source_schema = N'dbo',
        @source_name = N'Sales',
        @role_name = NULL,
        @supports_net_changes = 1;
    
    IF @@ERROR = 0
    BEGIN
        PRINT 'CDC successfully enabled for the Sales table.';
    END
    ELSE
    BEGIN
        RAISERROR('Failed to enable CDC for the Sales table.', 16, 1);
        RETURN;
    END
END
GO

-- =============================================
-- Step 7: Configure CDC cleanup job retention
-- =============================================
-- Set retention period to 1 day (1440 minutes)
-- This controls how long CDC data is kept before being cleaned up
PRINT 'Configuring CDC cleanup job retention to 1 day (1440 minutes)...';

EXEC sys.sp_cdc_change_job
    @job_type = N'cleanup',
    @retention = 1440; -- 1 day in minutes

IF @@ERROR = 0
BEGIN
    PRINT 'CDC cleanup job retention configured successfully.';
END
ELSE
BEGIN
    PRINT 'Warning: Failed to configure CDC cleanup job retention.';
END
GO

-- =============================================
-- Step 8: Verify CDC configuration
-- =============================================
PRINT 'Verifying CDC configuration...';

-- Check database level CDC
SELECT 
    name AS DatabaseName,
    is_cdc_enabled AS CDCEnabled,
    recovery_model_desc AS RecoveryModel
FROM sys.databases 
WHERE name = 'ETLDatabase';

-- Check table level CDC
SELECT 
    s.name AS SchemaName,
    t.name AS TableName,
    t.is_tracked_by_cdc AS CDCTracked,
    c.name AS CaptureInstanceName
FROM sys.tables t
INNER JOIN sys.schemas s ON t.schema_id = s.schema_id
LEFT JOIN sys.dm_cdc_capture_instances c ON t.object_id = c.source_object_id
WHERE t.name = 'Sales' AND s.name = 'dbo';

-- Check CDC jobs
SELECT 
    name AS JobName,
    enabled AS JobEnabled,
    description AS JobDescription
FROM msdb.dbo.sysjobs 
WHERE name LIKE '%cdc%';

-- Check CDC capture instances
SELECT 
    capture_instance,
    source_object_id,
    source_schema,
    source_table,
    capture_column_list,
    filegroup_name
FROM sys.dm_cdc_capture_instances;

GO

-- =============================================
-- Step 9: Create helper functions for CDC operations
-- =============================================

-- Function to get CDC changes for a specific time range
CREATE OR ALTER FUNCTION dbo.GetCDCSalesChanges
(
    @from_lsn BINARY(10),
    @to_lsn BINARY(10),
    @row_filter_option NVARCHAR(30) = 'all'
)
RETURNS TABLE
AS
RETURN
(
    SELECT 
        __$operation,
        __$seqval,
        __$update_mask,
        SaleID,
        CustomerID,
        ProductID,
        Quantity,
        UnitPrice,
        SaleDate,
        CreatedDate,
        ModifiedDate,
        CASE __$operation
            WHEN 1 THEN 'Delete'
            WHEN 2 THEN 'Insert'
            WHEN 3 THEN 'Update (Before)'
            WHEN 4 THEN 'Update (After)'
        END AS OperationType
    FROM cdc.fn_cdc_get_all_changes_dbo_Sales(@from_lsn, @to_lsn, @row_filter_option)
);
GO

-- Function to get net changes (final state only)
CREATE OR ALTER FUNCTION dbo.GetCDCSalesNetChanges
(
    @from_lsn BINARY(10),
    @to_lsn BINARY(10)
)
RETURNS TABLE
AS
RETURN
(
    SELECT 
        __$operation,
        __$seqval,
        __$update_mask,
        SaleID,
        CustomerID,
        ProductID,
        Quantity,
        UnitPrice,
        SaleDate,
        CreatedDate,
        ModifiedDate,
        CASE __$operation
            WHEN 1 THEN 'Delete'
            WHEN 2 THEN 'Insert'
            WHEN 4 THEN 'Update'
        END AS OperationType
    FROM cdc.fn_cdc_get_net_changes_dbo_Sales(@from_lsn, @to_lsn, 'all')
);
GO

-- =============================================
-- Step 10: Create stored procedure for CDC monitoring
-- =============================================
CREATE OR ALTER PROCEDURE dbo.MonitorCDCChanges
    @minutes_back INT = 60
AS
BEGIN
    SET NOCOUNT ON;
    
    DECLARE @from_lsn BINARY(10), @to_lsn BINARY(10);
    DECLARE @start_time DATETIME = DATEADD(MINUTE, -@minutes_back, GETDATE());
    
    -- Get LSN range for the time period
    SET @from_lsn = sys.fn_cdc_map_time_to_lsn('smallest greater than or equal', @start_time);
    SET @to_lsn = sys.fn_cdc_map_time_to_lsn('largest less than or equal', GETDATE());
    
    -- Check if we have valid LSNs
    IF @from_lsn IS NULL OR @to_lsn IS NULL
    BEGIN
        PRINT 'No CDC data available for the specified time range.';
        RETURN;
    END
    
    -- Get all changes in the time range
    SELECT 
        'All Changes' AS ChangeType,
        __$operation,
        __$seqval,
        SaleID,
        CustomerID,
        ProductID,
        Quantity,
        UnitPrice,
        SaleDate,
        CASE __$operation
            WHEN 1 THEN 'Delete'
            WHEN 2 THEN 'Insert'
            WHEN 3 THEN 'Update (Before)'
            WHEN 4 THEN 'Update (After)'
        END AS OperationType
    FROM cdc.fn_cdc_get_all_changes_dbo_Sales(@from_lsn, @to_lsn, 'all')
    ORDER BY __$seqval;
    
    -- Get net changes (final state only)
    SELECT 
        'Net Changes' AS ChangeType,
        __$operation,
        __$seqval,
        SaleID,
        CustomerID,
        ProductID,
        Quantity,
        UnitPrice,
        SaleDate,
        CASE __$operation
            WHEN 1 THEN 'Delete'
            WHEN 2 THEN 'Insert'
            WHEN 4 THEN 'Update'
        END AS OperationType
    FROM cdc.fn_cdc_get_net_changes_dbo_Sales(@from_lsn, @to_lsn, 'all')
    ORDER BY __$seqval;
END
GO

-- =============================================
-- Step 11: Create sample data manipulation for testing
-- =============================================
PRINT 'Creating sample data changes for CDC testing...';

-- Insert new record
INSERT INTO dbo.Sales (CustomerID, ProductID, Quantity, UnitPrice, SaleDate)
VALUES (4, 104, 5, 39.99, GETDATE());

-- Update existing record
UPDATE dbo.Sales 
SET Quantity = 3, ModifiedDate = GETDATE()
WHERE SaleID = 1;

-- Delete a record
DELETE FROM dbo.Sales WHERE SaleID = 2;

-- Wait a moment for CDC to capture changes
WAITFOR DELAY '00:00:02';

-- Test the monitoring procedure
PRINT 'Testing CDC monitoring for the last 5 minutes...';
EXEC dbo.MonitorCDCChanges @minutes_back = 5;

GO

-- =============================================
-- Step 12: Create cleanup and maintenance procedures
-- =============================================

-- Procedure to manually clean up CDC data
CREATE OR ALTER PROCEDURE dbo.CleanupCDCData
    @retention_hours INT = 24
AS
BEGIN
    SET NOCOUNT ON;
    
    DECLARE @low_water_mark BINARY(10);
    DECLARE @cleanup_time DATETIME = DATEADD(HOUR, -@retention_hours, GETDATE());
    
    -- Get LSN for cleanup time
    SET @low_water_mark = sys.fn_cdc_map_time_to_lsn('largest less than or equal', @cleanup_time);
    
    IF @low_water_mark IS NOT NULL
    BEGIN
        PRINT 'Cleaning up CDC data older than ' + CAST(@retention_hours AS VARCHAR(10)) + ' hours...';
        
        -- Clean up CDC data
        EXEC sys.sp_cdc_cleanup_change_table 
            @capture_instance = 'dbo_Sales',
            @low_water_mark = @low_water_mark;
            
        PRINT 'CDC cleanup completed.';
    END
    ELSE
    BEGIN
        PRINT 'No CDC data to clean up.';
    END
END
GO

-- Procedure to get CDC statistics
CREATE OR ALTER PROCEDURE dbo.GetCDCStatistics
AS
BEGIN
    SET NOCOUNT ON;
    
    PRINT 'CDC Statistics:';
    PRINT '===============';
    
    -- Database CDC status
    SELECT 
        'Database CDC Status' AS InfoType,
        name AS DatabaseName,
        is_cdc_enabled AS CDCEnabled,
        recovery_model_desc AS RecoveryModel
    FROM sys.databases 
    WHERE name = 'ETLDatabase';
    
    -- Table CDC status
    SELECT 
        'Table CDC Status' AS InfoType,
        s.name AS SchemaName,
        t.name AS TableName,
        t.is_tracked_by_cdc AS CDCTracked
    FROM sys.tables t
    INNER JOIN sys.schemas s ON t.schema_id = s.schema_id
    WHERE t.name = 'Sales' AND s.name = 'dbo';
    
    -- CDC capture instances
    SELECT 
        'CDC Capture Instances' AS InfoType,
        capture_instance,
        source_schema,
        source_table,
        capture_column_list
    FROM sys.dm_cdc_capture_instances;
    
    -- CDC jobs
    SELECT 
        'CDC Jobs' AS InfoType,
        name AS JobName,
        enabled AS JobEnabled,
        description AS JobDescription
    FROM msdb.dbo.sysjobs 
    WHERE name LIKE '%cdc%';
    
    -- CDC log scan status
    SELECT 
        'CDC Log Scan Status' AS InfoType,
        session_id,
        start_time,
        end_time,
        duration,
        scan_phase,
        error_count
    FROM sys.dm_cdc_log_scan_sessions
    ORDER BY start_time DESC;
END
GO

-- =============================================
-- Step 13: Final verification and summary
-- =============================================
PRINT '=============================================';
PRINT 'CDC Setup Complete - Summary';
PRINT '=============================================';
PRINT '1. Database CDC: Enabled';
PRINT '2. Table CDC: Enabled for dbo.Sales';
PRINT '3. Retention: 1 day (1440 minutes)';
PRINT '4. Helper functions created';
PRINT '5. Monitoring procedures created';
PRINT '6. Sample data changes applied for testing';
PRINT '';
PRINT 'To monitor CDC changes, use:';
PRINT 'EXEC dbo.MonitorCDCChanges @minutes_back = 60';
PRINT '';
PRINT 'To get CDC statistics, use:';
PRINT 'EXEC dbo.GetCDCStatistics';
PRINT '';
PRINT 'To clean up old CDC data, use:';
PRINT 'EXEC dbo.CleanupCDCData @retention_hours = 24';
PRINT '=============================================';

-- Run final verification
EXEC dbo.GetCDCStatistics;

GO
