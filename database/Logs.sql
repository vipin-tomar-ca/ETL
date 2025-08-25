-- =============================================
-- ETL Framework Metadata Database Schema
-- Logs Table
-- =============================================

-- Create the Logs table to store ETL execution logs
CREATE TABLE Logs (
    LogID INT IDENTITY(1,1) PRIMARY KEY,
    JobID NVARCHAR(100) NOT NULL, -- Unique identifier for the ETL job
    DatabaseID INT NOT NULL,
    QueryID INT NULL, -- Optional reference to specific query
    Status NVARCHAR(50) NOT NULL, -- SUCCESS, FAILED, RUNNING, CANCELLED
    RowsProcessed BIGINT DEFAULT 0 NOT NULL,
    RowsInserted BIGINT DEFAULT 0 NOT NULL,
    RowsUpdated BIGINT DEFAULT 0 NOT NULL,
    RowsDeleted BIGINT DEFAULT 0 NOT NULL,
    ErrorMessage NVARCHAR(MAX) NULL,
    StartTime DATETIME2 NOT NULL,
    EndTime DATETIME2 NULL,
    DurationSeconds AS DATEDIFF(SECOND, StartTime, EndTime) PERSISTED,
    ExecutionPlan NVARCHAR(MAX) NULL, -- Store execution plan if available
    Parameters NVARCHAR(MAX) NULL, -- JSON string of parameters used
    CreatedDate DATETIME2 DEFAULT GETDATE() NOT NULL,
    CreatedBy NVARCHAR(100) DEFAULT SYSTEM_USER NOT NULL,
    
    -- Foreign key constraints
    CONSTRAINT FK_Logs_Connections FOREIGN KEY (DatabaseID) 
        REFERENCES Connections(DatabaseID) ON DELETE CASCADE,
    CONSTRAINT FK_Logs_Queries FOREIGN KEY (QueryID) 
        REFERENCES Queries(QueryID) ON DELETE SET NULL,
    
    -- Check constraints
    CONSTRAINT CK_Logs_Status CHECK (Status IN ('SUCCESS', 'FAILED', 'RUNNING', 'CANCELLED', 'WARNING')),
    CONSTRAINT CK_Logs_StartTime CHECK (StartTime <= ISNULL(EndTime, GETDATE())),
    CONSTRAINT CK_Logs_RowsProcessed CHECK (RowsProcessed >= 0),
    CONSTRAINT CK_Logs_RowsInserted CHECK (RowsInserted >= 0),
    CONSTRAINT CK_Logs_RowsUpdated CHECK (RowsUpdated >= 0),
    CONSTRAINT CK_Logs_RowsDeleted CHECK (RowsDeleted >= 0)
);

-- Add comments to the table
EXEC sp_addextendedproperty 
    @name = N'MS_Description',
    @value = N'Stores ETL execution logs and performance metrics',
    @level0type = N'SCHEMA',
    @level0name = N'dbo',
    @level1type = N'TABLE',
    @level1name = N'Logs';

-- Add comments to columns
EXEC sp_addextendedproperty 
    @name = N'MS_Description',
    @value = N'Primary key identifier for the log entry',
    @level0type = N'SCHEMA',
    @level0name = N'dbo',
    @level1type = N'TABLE',
    @level1name = N'Logs',
    @level2type = N'COLUMN',
    @level2name = N'LogID';

EXEC sp_addextendedproperty 
    @name = N'MS_Description',
    @value = N'Unique identifier for the ETL job execution',
    @level0type = N'SCHEMA',
    @level0name = N'dbo',
    @level1type = N'TABLE',
    @level1name = N'Logs',
    @level2type = N'COLUMN',
    @level2name = N'JobID';

EXEC sp_addextendedproperty 
    @name = N'MS_Description',
    @value = N'Foreign key reference to Connections table',
    @level0type = N'SCHEMA',
    @level0name = N'dbo',
    @level1type = N'TABLE',
    @level1name = N'Logs',
    @level2type = N'COLUMN',
    @level2name = N'DatabaseID';

EXEC sp_addextendedproperty 
    @name = N'MS_Description',
    @value = N'Foreign key reference to Queries table (optional)',
    @level0type = N'SCHEMA',
    @level0name = N'dbo',
    @level1type = N'TABLE',
    @level1name = N'Logs',
    @level2type = N'COLUMN',
    @level2name = N'QueryID';

EXEC sp_addextendedproperty 
    @name = N'MS_Description',
    @value = N'Status of the ETL job execution (SUCCESS, FAILED, RUNNING, CANCELLED, WARNING)',
    @level0type = N'SCHEMA',
    @level0name = N'dbo',
    @level1type = N'TABLE',
    @level1name = N'Logs',
    @level2type = N'COLUMN',
    @level2name = N'Status';

EXEC sp_addextendedproperty 
    @name = N'MS_Description',
    @value = N'Total number of rows processed during the ETL operation',
    @level0type = N'SCHEMA',
    @level0name = N'dbo',
    @level1type = N'TABLE',
    @level1name = N'Logs',
    @level2type = N'COLUMN',
    @level2name = N'RowsProcessed';

EXEC sp_addextendedproperty 
    @name = N'MS_Description',
    @value = N'Number of rows inserted during the ETL operation',
    @level0type = N'SCHEMA',
    @level0name = N'dbo',
    @level1type = N'TABLE',
    @level1name = N'Logs',
    @level2type = N'COLUMN',
    @level2name = N'RowsInserted';

EXEC sp_addextendedproperty 
    @name = N'MS_Description',
    @value = N'Number of rows updated during the ETL operation',
    @level0type = N'SCHEMA',
    @level0name = N'dbo',
    @level1type = N'TABLE',
    @level1name = N'Logs',
    @level2type = N'COLUMN',
    @level2name = N'RowsUpdated';

EXEC sp_addextendedproperty 
    @name = N'MS_Description',
    @value = N'Number of rows deleted during the ETL operation',
    @level0type = N'SCHEMA',
    @level0name = N'dbo',
    @level1type = N'TABLE',
    @level1name = N'Logs',
    @level2type = N'COLUMN',
    @level2name = N'RowsDeleted';

EXEC sp_addextendedproperty 
    @name = N'MS_Description',
    @value = N'Error message if the ETL operation failed',
    @level0type = N'SCHEMA',
    @level0name = N'dbo',
    @level1type = N'TABLE',
    @level1name = N'Logs',
    @level2type = N'COLUMN',
    @level2name = N'ErrorMessage';

EXEC sp_addextendedproperty 
    @name = N'MS_Description',
    @value = N'Start time of the ETL operation',
    @level0type = N'SCHEMA',
    @level0name = N'dbo',
    @level1type = N'TABLE',
    @level1name = N'Logs',
    @level2type = N'COLUMN',
    @level2name = N'StartTime';

EXEC sp_addextendedproperty 
    @name = N'MS_Description',
    @value = N'End time of the ETL operation (NULL if still running)',
    @level0type = N'SCHEMA',
    @level0name = N'dbo',
    @level1type = N'TABLE',
    @level1name = N'Logs',
    @level2type = N'COLUMN',
    @level2name = N'EndTime';

EXEC sp_addextendedproperty 
    @name = N'MS_Description',
    @value = N'Calculated duration in seconds (computed column)',
    @level0type = N'SCHEMA',
    @level0name = N'dbo',
    @level1type = N'TABLE',
    @level1name = N'Logs',
    @level2type = N'COLUMN',
    @level2name = N'DurationSeconds';

EXEC sp_addextendedproperty 
    @name = N'MS_Description',
    @value = N'Execution plan details if available',
    @level0type = N'SCHEMA',
    @level0name = N'dbo',
    @level1type = N'TABLE',
    @level1name = N'Logs',
    @level2type = N'COLUMN',
    @level2name = N'ExecutionPlan';

EXEC sp_addextendedproperty 
    @name = N'MS_Description',
    @value = N'JSON string containing parameters used in the ETL operation',
    @level0type = N'SCHEMA',
    @level0name = N'dbo',
    @level1type = N'TABLE',
    @level1name = N'Logs',
    @level2type = N'COLUMN',
    @level2name = N'Parameters';

EXEC sp_addextendedproperty 
    @name = N'MS_Description',
    @value = N'Date when the log record was created',
    @level0type = N'SCHEMA',
    @level0name = N'dbo',
    @level1type = N'TABLE',
    @level1name = N'Logs',
    @level2type = N'COLUMN',
    @level2name = N'CreatedDate';

EXEC sp_addextendedproperty 
    @name = N'MS_Description',
    @value = N'User or system that created the log record',
    @level0type = N'SCHEMA',
    @level0name = N'dbo',
    @level1type = N'TABLE',
    @level1name = N'Logs',
    @level2type = N'COLUMN',
    @level2name = N'CreatedBy';

-- Create indexes for better performance
CREATE INDEX IX_Logs_JobID ON Logs(JobID);
CREATE INDEX IX_Logs_DatabaseID ON Logs(DatabaseID);
CREATE INDEX IX_Logs_QueryID ON Logs(QueryID);
CREATE INDEX IX_Logs_Status ON Logs(Status);
CREATE INDEX IX_Logs_StartTime ON Logs(StartTime);
CREATE INDEX IX_Logs_EndTime ON Logs(EndTime);
CREATE INDEX IX_Logs_Status_StartTime ON Logs(Status, StartTime);
CREATE INDEX IX_Logs_DatabaseID_StartTime ON Logs(DatabaseID, StartTime);

-- Create a view for recent logs
CREATE VIEW vw_RecentLogs AS
SELECT 
    l.LogID,
    l.JobID,
    c.DatabaseName,
    c.DatabaseType,
    q.QueryName,
    l.Status,
    l.RowsProcessed,
    l.RowsInserted,
    l.RowsUpdated,
    l.RowsDeleted,
    l.ErrorMessage,
    l.StartTime,
    l.EndTime,
    l.DurationSeconds,
    l.CreatedDate
FROM Logs l
INNER JOIN Connections c ON l.DatabaseID = c.DatabaseID
LEFT JOIN Queries q ON l.QueryID = q.QueryID
WHERE l.StartTime >= DATEADD(DAY, -30, GETDATE()); -- Last 30 days

-- Add comment to the view
EXEC sp_addextendedproperty 
    @name = N'MS_Description',
    @value = N'View showing recent ETL logs with database and query information',
    @level0type = N'SCHEMA',
    @level0name = N'dbo',
    @level1type = N'VIEW',
    @level1name = N'vw_RecentLogs';
