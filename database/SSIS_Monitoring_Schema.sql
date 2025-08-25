-- SSIS Monitoring and Scaling Database Schema
-- This schema supports enhanced SSIS package orchestration, monitoring, and scaling

USE master;
GO

-- Create monitoring database if it doesn't exist
IF NOT EXISTS (SELECT name FROM sys.databases WHERE name = 'SSISMonitoring')
BEGIN
    CREATE DATABASE SSISMonitoring;
END
GO

USE SSISMonitoring;
GO

-- SSIS Package Configurations
CREATE TABLE dbo.SSISPackageConfigurations
(
    ConfigurationID INT IDENTITY(1,1) PRIMARY KEY,
    PackagePath NVARCHAR(500) NOT NULL,
    Environment NVARCHAR(50) NOT NULL,
    ConnectionStrings NVARCHAR(MAX), -- JSON
    Variables NVARCHAR(MAX), -- JSON
    Parameters NVARCHAR(MAX), -- JSON
    ExecutionSettings NVARCHAR(MAX), -- JSON
    IsActive BIT NOT NULL DEFAULT 1,
    CreatedDate DATETIME2 NOT NULL DEFAULT GETDATE(),
    LastModified DATETIME2 NOT NULL DEFAULT GETDATE(),
    CreatedBy NVARCHAR(100),
    ModifiedBy NVARCHAR(100),
    CONSTRAINT UQ_PackageEnvironment UNIQUE (PackagePath, Environment)
);

-- SSIS Execution History
CREATE TABLE dbo.SSISExecutionHistory
(
    ExecutionID NVARCHAR(50) PRIMARY KEY,
    PackagePath NVARCHAR(500) NOT NULL,
    Status NVARCHAR(20) NOT NULL, -- Running, Success, Failed, Cancelled
    StartTime DATETIME2 NOT NULL,
    EndTime DATETIME2 NULL,
    ExecutionTimeSeconds DECIMAL(10,2) NULL,
    ExitCode INT NULL,
    Output NVARCHAR(MAX) NULL,
    Error NVARCHAR(MAX) NULL,
    Parameters NVARCHAR(MAX) NULL, -- JSON
    RecordsProcessed INT NULL,
    ServerName NVARCHAR(100) NULL,
    ProcessID INT NULL,
    CreatedDate DATETIME2 NOT NULL DEFAULT GETDATE()
);

-- SSIS Package Dependencies
CREATE TABLE dbo.SSISPackageDependencies
(
    DependencyID INT IDENTITY(1,1) PRIMARY KEY,
    PackagePath NVARCHAR(500) NOT NULL,
    DependentPackagePath NVARCHAR(500) NOT NULL,
    DependencyType NVARCHAR(50) NOT NULL, -- Data, Control, Logical
    IsActive BIT NOT NULL DEFAULT 1,
    CreatedDate DATETIME2 NOT NULL DEFAULT GETDATE(),
    CONSTRAINT UQ_PackageDependency UNIQUE (PackagePath, DependentPackagePath)
);

-- SSIS Chunking Configuration
CREATE TABLE dbo.SSISChunkingConfigurations
(
    ChunkingID INT IDENTITY(1,1) PRIMARY KEY,
    PackagePath NVARCHAR(500) NOT NULL,
    ChunkingStrategy NVARCHAR(50) NOT NULL, -- Range, Hash, RoundRobin
    NumberOfChunks INT NOT NULL,
    MaxConcurrentChunks INT NOT NULL,
    SourceQuery NVARCHAR(MAX) NOT NULL,
    ChunkingKey NVARCHAR(100) NULL,
    IsActive BIT NOT NULL DEFAULT 1,
    CreatedDate DATETIME2 NOT NULL DEFAULT GETDATE(),
    LastModified DATETIME2 NOT NULL DEFAULT GETDATE()
);

-- SSIS Chunk Execution History
CREATE TABLE dbo.SSISChunkExecutionHistory
(
    ChunkExecutionID NVARCHAR(50) PRIMARY KEY,
    ParentExecutionID NVARCHAR(50) NOT NULL,
    ChunkID INT NOT NULL,
    PackagePath NVARCHAR(500) NOT NULL,
    Status NVARCHAR(20) NOT NULL,
    StartTime DATETIME2 NOT NULL,
    EndTime DATETIME2 NULL,
    ExecutionTimeSeconds DECIMAL(10,2) NULL,
    RecordsProcessed INT NULL,
    ChunkOffset INT NULL,
    ChunkSize INT NULL,
    CreatedDate DATETIME2 NOT NULL DEFAULT GETDATE(),
    FOREIGN KEY (ParentExecutionID) REFERENCES dbo.SSISExecutionHistory(ExecutionID)
);

-- SSIS Performance Metrics
CREATE TABLE dbo.SSISPerformanceMetrics
(
    MetricID INT IDENTITY(1,1) PRIMARY KEY,
    PackagePath NVARCHAR(500) NOT NULL,
    MetricDate DATE NOT NULL,
    MetricHour INT NOT NULL,
    TotalExecutions INT NOT NULL DEFAULT 0,
    SuccessfulExecutions INT NOT NULL DEFAULT 0,
    FailedExecutions INT NOT NULL DEFAULT 0,
    AvgExecutionTimeSeconds DECIMAL(10,2) NULL,
    MaxExecutionTimeSeconds DECIMAL(10,2) NULL,
    MinExecutionTimeSeconds DECIMAL(10,2) NULL,
    TotalRecordsProcessed BIGINT NOT NULL DEFAULT 0,
    AvgRecordsProcessed DECIMAL(10,2) NULL,
    CreatedDate DATETIME2 NOT NULL DEFAULT GETDATE(),
    CONSTRAINT UQ_PackageDateHour UNIQUE (PackagePath, MetricDate, MetricHour)
);

-- SSIS Health Checks
CREATE TABLE dbo.SSISHealthChecks
(
    HealthCheckID INT IDENTITY(1,1) PRIMARY KEY,
    PackagePath NVARCHAR(500) NOT NULL,
    HealthStatus NVARCHAR(20) NOT NULL, -- Healthy, Degraded, Warning, Critical
    CheckDate DATETIME2 NOT NULL DEFAULT GETDATE(),
    LastExecutionDate DATETIME2 NULL,
    LastExecutionStatus NVARCHAR(20) NULL,
    AvgExecutionTimeLast30Days DECIMAL(10,2) NULL,
    SuccessRateLast30Days DECIMAL(5,2) NULL,
    Issues NVARCHAR(MAX) NULL, -- JSON
    Recommendations NVARCHAR(MAX) NULL -- JSON
);

-- Indexes for performance
CREATE INDEX IX_SSISExecutionHistory_PackagePath ON dbo.SSISExecutionHistory(PackagePath);
CREATE INDEX IX_SSISExecutionHistory_Status ON dbo.SSISExecutionHistory(Status);
CREATE INDEX IX_SSISExecutionHistory_StartTime ON dbo.SSISExecutionHistory(StartTime);
CREATE INDEX IX_SSISExecutionHistory_ExecutionTime ON dbo.SSISExecutionHistory(ExecutionTimeSeconds);

CREATE INDEX IX_SSISChunkExecution_ParentExecution ON dbo.SSISChunkExecutionHistory(ParentExecutionID);
CREATE INDEX IX_SSISChunkExecution_Status ON dbo.SSISChunkExecutionHistory(Status);

CREATE INDEX IX_SSISPerformanceMetrics_PackageDate ON dbo.SSISPerformanceMetrics(PackagePath, MetricDate);
CREATE INDEX IX_SSISHealthChecks_PackagePath ON dbo.SSISHealthChecks(PackagePath);
CREATE INDEX IX_SSISHealthChecks_Status ON dbo.SSISHealthChecks(HealthStatus);

-- Views for easy querying
CREATE VIEW dbo.vw_SSISExecutionSummary AS
SELECT 
    PackagePath,
    COUNT(*) as TotalExecutions,
    SUM(CASE WHEN Status = 'Success' THEN 1 ELSE 0 END) as SuccessfulExecutions,
    SUM(CASE WHEN Status = 'Failed' THEN 1 ELSE 0 END) as FailedExecutions,
    AVG(ExecutionTimeSeconds) as AvgExecutionTime,
    MAX(ExecutionTimeSeconds) as MaxExecutionTime,
    SUM(RecordsProcessed) as TotalRecordsProcessed,
    MAX(StartTime) as LastExecution
FROM dbo.SSISExecutionHistory
WHERE StartTime >= DATEADD(day, -30, GETDATE())
GROUP BY PackagePath;

CREATE VIEW dbo.vw_SSISHealthStatus AS
SELECT 
    h.PackagePath,
    h.HealthStatus,
    h.CheckDate,
    h.LastExecutionDate,
    h.AvgExecutionTimeLast30Days,
    h.SuccessRateLast30Days,
    s.TotalExecutions,
    s.SuccessfulExecutions,
    s.FailedExecutions
FROM dbo.SSISHealthChecks h
LEFT JOIN dbo.vw_SSISExecutionSummary s ON h.PackagePath = s.PackagePath
WHERE h.CheckDate = (SELECT MAX(CheckDate) FROM dbo.SSISHealthChecks WHERE PackagePath = h.PackagePath);

-- Stored Procedures for common operations
CREATE PROCEDURE dbo.sp_GetPackageExecutionHistory
    @PackagePath NVARCHAR(500) = NULL,
    @StartDate DATETIME2 = NULL,
    @EndDate DATETIME2 = NULL,
    @Status NVARCHAR(20) = NULL
AS
BEGIN
    SET NOCOUNT ON;
    
    SELECT 
        ExecutionID,
        PackagePath,
        Status,
        StartTime,
        EndTime,
        ExecutionTimeSeconds,
        ExitCode,
        RecordsProcessed,
        ServerName
    FROM dbo.SSISExecutionHistory
    WHERE (@PackagePath IS NULL OR PackagePath = @PackagePath)
        AND (@StartDate IS NULL OR StartTime >= @StartDate)
        AND (@EndDate IS NULL OR StartTime <= @EndDate)
        AND (@Status IS NULL OR Status = @Status)
    ORDER BY StartTime DESC;
END
GO

CREATE PROCEDURE dbo.sp_GetPackagePerformanceMetrics
    @PackagePath NVARCHAR(500),
    @Days INT = 30
AS
BEGIN
    SET NOCOUNT ON;
    
    SELECT 
        MetricDate,
        MetricHour,
        TotalExecutions,
        SuccessfulExecutions,
        FailedExecutions,
        AvgExecutionTimeSeconds,
        MaxExecutionTimeSeconds,
        TotalRecordsProcessed,
        AvgRecordsProcessed
    FROM dbo.SSISPerformanceMetrics
    WHERE PackagePath = @PackagePath
        AND MetricDate >= DATEADD(day, -@Days, GETDATE())
    ORDER BY MetricDate DESC, MetricHour DESC;
END
GO

CREATE PROCEDURE dbo.sp_UpdatePerformanceMetrics
AS
BEGIN
    SET NOCOUNT ON;
    
    -- Update hourly performance metrics
    MERGE dbo.SSISPerformanceMetrics AS target
    USING (
        SELECT 
            PackagePath,
            CAST(StartTime AS DATE) as MetricDate,
            DATEPART(HOUR, StartTime) as MetricHour,
            COUNT(*) as TotalExecutions,
            SUM(CASE WHEN Status = 'Success' THEN 1 ELSE 0 END) as SuccessfulExecutions,
            SUM(CASE WHEN Status = 'Failed' THEN 1 ELSE 0 END) as FailedExecutions,
            AVG(ExecutionTimeSeconds) as AvgExecutionTimeSeconds,
            MAX(ExecutionTimeSeconds) as MaxExecutionTimeSeconds,
            MIN(ExecutionTimeSeconds) as MinExecutionTimeSeconds,
            SUM(RecordsProcessed) as TotalRecordsProcessed,
            AVG(RecordsProcessed) as AvgRecordsProcessed
        FROM dbo.SSISExecutionHistory
        WHERE StartTime >= DATEADD(day, -1, GETDATE())
        GROUP BY PackagePath, CAST(StartTime AS DATE), DATEPART(HOUR, StartTime)
    ) AS source
    ON target.PackagePath = source.PackagePath 
        AND target.MetricDate = source.MetricDate 
        AND target.MetricHour = source.MetricHour
    WHEN MATCHED THEN
        UPDATE SET 
            TotalExecutions = source.TotalExecutions,
            SuccessfulExecutions = source.SuccessfulExecutions,
            FailedExecutions = source.FailedExecutions,
            AvgExecutionTimeSeconds = source.AvgExecutionTimeSeconds,
            MaxExecutionTimeSeconds = source.MaxExecutionTimeSeconds,
            MinExecutionTimeSeconds = source.MinExecutionTimeSeconds,
            TotalRecordsProcessed = source.TotalRecordsProcessed,
            AvgRecordsProcessed = source.AvgRecordsProcessed
    WHEN NOT MATCHED THEN
        INSERT (PackagePath, MetricDate, MetricHour, TotalExecutions, SuccessfulExecutions, FailedExecutions, 
                AvgExecutionTimeSeconds, MaxExecutionTimeSeconds, MinExecutionTimeSeconds, TotalRecordsProcessed, AvgRecordsProcessed)
        VALUES (source.PackagePath, source.MetricDate, source.MetricHour, source.TotalExecutions, source.SuccessfulExecutions, source.FailedExecutions,
                source.AvgExecutionTimeSeconds, source.MaxExecutionTimeSeconds, source.MinExecutionTimeSeconds, source.TotalRecordsProcessed, source.AvgRecordsProcessed);
END
GO

CREATE PROCEDURE dbo.sp_UpdateHealthChecks
AS
BEGIN
    SET NOCOUNT ON;
    
    -- Update health checks for all packages
    INSERT INTO dbo.SSISHealthChecks (PackagePath, HealthStatus, LastExecutionDate, LastExecutionStatus, 
                                     AvgExecutionTimeLast30Days, SuccessRateLast30Days, Issues, Recommendations)
    SELECT 
        s.PackagePath,
        CASE 
            WHEN s.SuccessRateLast30Days < 0.8 THEN 'Critical'
            WHEN s.SuccessRateLast30Days < 0.95 THEN 'Warning'
            WHEN s.AvgExecutionTime > 3600 THEN 'Degraded' -- More than 1 hour
            ELSE 'Healthy'
        END as HealthStatus,
        s.LastExecution,
        (SELECT TOP 1 Status FROM dbo.SSISExecutionHistory WHERE PackagePath = s.PackagePath ORDER BY StartTime DESC) as LastExecutionStatus,
        s.AvgExecutionTime,
        s.SuccessRateLast30Days,
        CASE 
            WHEN s.SuccessRateLast30Days < 0.8 THEN '["High failure rate detected"]'
            WHEN s.AvgExecutionTime > 3600 THEN '["Long execution times detected"]'
            ELSE '[]'
        END as Issues,
        CASE 
            WHEN s.SuccessRateLast30Days < 0.8 THEN '["Review package logic and error handling"]'
            WHEN s.AvgExecutionTime > 3600 THEN '["Consider chunking or optimization"]'
            ELSE '["Package performing well"]'
        END as Recommendations
    FROM dbo.vw_SSISExecutionSummary s
    WHERE s.TotalExecutions > 0;
END
GO

-- Sample data for testing
INSERT INTO dbo.SSISPackageConfigurations (PackagePath, Environment, ConnectionStrings, Variables, Parameters, ExecutionSettings)
VALUES 
('C:\SSIS\Packages\Extract_Customers.dtsx', 'Development', 
 '{"SourceDB": "Data Source=dev-server;Initial Catalog=SourceDB;Integrated Security=SSPI;", "TargetDB": "Data Source=dev-server;Initial Catalog=TargetDB;Integrated Security=SSPI;"}',
 '{"BatchSize": "10000", "TimeoutMinutes": "30"}',
 '{"LastRunDate": "2024-01-01", "ProcessingMode": "Incremental"}',
 '{"MaxRetries": "3", "RetryDelaySeconds": "60"}'),

('C:\SSIS\Packages\Extract_Orders.dtsx', 'Production',
 '{"SourceDB": "Data Source=prod-server;Initial Catalog=SourceDB;Integrated Security=SSPI;", "TargetDB": "Data Source=prod-server;Initial Catalog=TargetDB;Integrated Security=SSPI;"}',
 '{"BatchSize": "50000", "TimeoutMinutes": "60"}',
 '{"LastRunDate": "2024-01-01", "ProcessingMode": "Full"}',
 '{"MaxRetries": "5", "RetryDelaySeconds": "120"}');

-- Sample dependencies
INSERT INTO dbo.SSISPackageDependencies (PackagePath, DependentPackagePath, DependencyType)
VALUES 
('C:\SSIS\Packages\Extract_Customers.dtsx', 'C:\SSIS\Packages\Transform_CustomerData.dtsx', 'Data'),
('C:\SSIS\Packages\Extract_Orders.dtsx', 'C:\SSIS\Packages\Transform_OrderData.dtsx', 'Data'),
('C:\SSIS\Packages\Transform_CustomerData.dtsx', 'C:\SSIS\Packages\Load_CustomerDimension.dtsx', 'Data');

-- Sample chunking configuration
INSERT INTO dbo.SSISChunkingConfigurations (PackagePath, ChunkingStrategy, NumberOfChunks, MaxConcurrentChunks, SourceQuery)
VALUES 
('C:\SSIS\Packages\Extract_LargeTable.dtsx', 'Range', 10, 5, 'SELECT * FROM dbo.LargeTable WHERE LastModified > @LastRunDate');

-- Create SQL Server Agent job to update metrics and health checks
IF NOT EXISTS (SELECT name FROM msdb.dbo.sysjobs WHERE name = 'SSIS_Monitoring_Maintenance')
BEGIN
    EXEC msdb.dbo.sp_add_job
        @job_name = N'SSIS_Monitoring_Maintenance',
        @enabled = 1,
        @description = N'Update SSIS performance metrics and health checks';

    EXEC msdb.dbo.sp_add_jobstep
        @job_name = N'SSIS_Monitoring_Maintenance',
        @step_name = N'Update Metrics',
        @subsystem = N'TSQL',
        @command = N'EXEC SSISMonitoring.dbo.sp_UpdatePerformanceMetrics',
        @database_name = N'SSISMonitoring';

    EXEC msdb.dbo.sp_add_jobstep
        @job_name = N'SSIS_Monitoring_Maintenance',
        @step_name = N'Update Health Checks',
        @subsystem = N'TSQL',
        @command = N'EXEC SSISMonitoring.dbo.sp_UpdateHealthChecks',
        @database_name = N'SSISMonitoring';

    EXEC msdb.dbo.sp_add_schedule
        @schedule_name = N'SSIS_Monitoring_Schedule',
        @freq_type = 4, -- Daily
        @freq_interval = 1,
        @active_start_time = 010000; -- 1:00 AM

    EXEC msdb.dbo.sp_attach_schedule
        @job_name = N'SSIS_Monitoring_Maintenance',
        @schedule_name = N'SSIS_Monitoring_Schedule';
END
GO

PRINT 'SSIS Monitoring and Scaling database schema created successfully!';
PRINT 'Tables created:';
PRINT '- SSISPackageConfigurations';
PRINT '- SSISExecutionHistory';
PRINT '- SSISPackageDependencies';
PRINT '- SSISChunkingConfigurations';
PRINT '- SSISChunkExecutionHistory';
PRINT '- SSISPerformanceMetrics';
PRINT '- SSISHealthChecks';
PRINT '';
PRINT 'Views created:';
PRINT '- vw_SSISExecutionSummary';
PRINT '- vw_SSISHealthStatus';
PRINT '';
PRINT 'Stored Procedures created:';
PRINT '- sp_GetPackageExecutionHistory';
PRINT '- sp_GetPackagePerformanceMetrics';
PRINT '- sp_UpdatePerformanceMetrics';
PRINT '- sp_UpdateHealthChecks';
PRINT '';
PRINT 'SQL Server Agent job created: SSIS_Monitoring_Maintenance';
