-- =============================================
-- ETL Framework Metadata Database Schema
-- Queries Table
-- =============================================

-- Create the Queries table to store SQL queries for each database
CREATE TABLE Queries (
    QueryID INT IDENTITY(1,1) PRIMARY KEY,
    DatabaseID INT NOT NULL,
    QueryName NVARCHAR(200) NOT NULL,
    QueryText NVARCHAR(MAX) NOT NULL,
    QueryType NVARCHAR(50) NOT NULL, -- SELECT, INSERT, UPDATE, DELETE, MERGE, etc.
    Description NVARCHAR(500) NULL,
    IsActive BIT DEFAULT 1 NOT NULL,
    ExecutionOrder INT DEFAULT 0 NOT NULL,
    TimeoutSeconds INT DEFAULT 300 NOT NULL, -- Default 5 minutes timeout
    CreatedDate DATETIME2 DEFAULT GETDATE() NOT NULL,
    ModifiedDate DATETIME2 DEFAULT GETDATE() NOT NULL,
    CreatedBy NVARCHAR(100) DEFAULT SYSTEM_USER NOT NULL,
    ModifiedBy NVARCHAR(100) DEFAULT SYSTEM_USER NOT NULL,
    
    -- Foreign key constraint to Connections table
    CONSTRAINT FK_Queries_Connections FOREIGN KEY (DatabaseID) 
        REFERENCES Connections(DatabaseID) ON DELETE CASCADE
);

-- Add comments to the table
EXEC sp_addextendedproperty 
    @name = N'MS_Description',
    @value = N'Stores SQL queries for ETL operations per database',
    @level0type = N'SCHEMA',
    @level0name = N'dbo',
    @level1type = N'TABLE',
    @level1name = N'Queries';

-- Add comments to columns
EXEC sp_addextendedproperty 
    @name = N'MS_Description',
    @value = N'Primary key identifier for the query',
    @level0type = N'SCHEMA',
    @level0name = N'dbo',
    @level1type = N'TABLE',
    @level1name = N'Queries',
    @level2type = N'COLUMN',
    @level2name = N'QueryID';

EXEC sp_addextendedproperty 
    @name = N'MS_Description',
    @value = N'Foreign key reference to Connections table',
    @level0type = N'SCHEMA',
    @level0name = N'dbo',
    @level1type = N'TABLE',
    @level1name = N'Queries',
    @level2type = N'COLUMN',
    @level2name = N'DatabaseID';

EXEC sp_addextendedproperty 
    @name = N'MS_Description',
    @value = N'Name or identifier for the query',
    @level0type = N'SCHEMA',
    @level0name = N'dbo',
    @level1type = N'TABLE',
    @level1name = N'Queries',
    @level2type = N'COLUMN',
    @level2name = N'QueryName';

EXEC sp_addextendedproperty 
    @name = N'MS_Description',
    @value = N'The actual SQL query text',
    @level0type = N'SCHEMA',
    @level0name = N'dbo',
    @level1type = N'TABLE',
    @level1name = N'Queries',
    @level2type = N'COLUMN',
    @level2name = N'QueryText';

EXEC sp_addextendedproperty 
    @name = N'MS_Description',
    @value = N'Type of SQL operation (SELECT, INSERT, UPDATE, DELETE, MERGE, etc.)',
    @level0type = N'SCHEMA',
    @level0name = N'dbo',
    @level1type = N'TABLE',
    @level1name = N'Queries',
    @level2type = N'COLUMN',
    @level2name = N'QueryType';

EXEC sp_addextendedproperty 
    @name = N'MS_Description',
    @value = N'Description of what the query does',
    @level0type = N'SCHEMA',
    @level0name = N'dbo',
    @level1type = N'TABLE',
    @level1name = N'Queries',
    @level2type = N'COLUMN',
    @level2name = N'Description';

EXEC sp_addextendedproperty 
    @name = N'MS_Description',
    @value = N'Flag indicating if the query is active',
    @level0type = N'SCHEMA',
    @level0name = N'dbo',
    @level1type = N'TABLE',
    @level1name = N'Queries',
    @level2type = N'COLUMN',
    @level2name = N'IsActive';

EXEC sp_addextendedproperty 
    @name = N'MS_Description',
    @value = N'Order in which queries should be executed',
    @level0type = N'SCHEMA',
    @level0name = N'dbo',
    @level1type = N'TABLE',
    @level1name = N'Queries',
    @level2type = N'COLUMN',
    @level2name = N'ExecutionOrder';

EXEC sp_addextendedproperty 
    @name = N'MS_Description',
    @value = N'Timeout in seconds for query execution',
    @level0type = N'SCHEMA',
    @level0name = N'dbo',
    @level1type = N'TABLE',
    @level1name = N'Queries',
    @level2type = N'COLUMN',
    @level2name = N'TimeoutSeconds';

EXEC sp_addextendedproperty 
    @name = N'MS_Description',
    @value = N'Date when the query record was created',
    @level0type = N'SCHEMA',
    @level0name = N'dbo',
    @level1type = N'TABLE',
    @level1name = N'Queries',
    @level2type = N'COLUMN',
    @level2name = N'CreatedDate';

EXEC sp_addextendedproperty 
    @name = N'MS_Description',
    @value = N'Date when the query record was last modified',
    @level0type = N'SCHEMA',
    @level0name = N'dbo',
    @level1type = N'TABLE',
    @level1name = N'Queries',
    @level2type = N'COLUMN',
    @level2name = N'ModifiedDate';

EXEC sp_addextendedproperty 
    @name = N'MS_Description',
    @value = N'User who created the query record',
    @level0type = N'SCHEMA',
    @level0name = N'dbo',
    @level1type = N'TABLE',
    @level1name = N'Queries',
    @level2type = N'COLUMN',
    @level2name = N'CreatedBy';

EXEC sp_addextendedproperty 
    @name = N'MS_Description',
    @value = N'User who last modified the query record',
    @level0type = N'SCHEMA',
    @level0name = N'dbo',
    @level1type = N'TABLE',
    @level1name = N'Queries',
    @level2type = N'COLUMN',
    @level2name = N'ModifiedBy';

-- Create indexes for better performance
CREATE INDEX IX_Queries_DatabaseID ON Queries(DatabaseID);
CREATE INDEX IX_Queries_QueryType ON Queries(QueryType);
CREATE INDEX IX_Queries_IsActive ON Queries(IsActive);
CREATE INDEX IX_Queries_ExecutionOrder ON Queries(ExecutionOrder);
CREATE INDEX IX_Queries_QueryName ON Queries(QueryName);

-- Create a trigger to update ModifiedDate and ModifiedBy
CREATE TRIGGER TR_Queries_UpdateModified
ON Queries
AFTER UPDATE
AS
BEGIN
    SET NOCOUNT ON;
    
    UPDATE Queries
    SET ModifiedDate = GETDATE(),
        ModifiedBy = SYSTEM_USER
    FROM Queries q
    INNER JOIN inserted i ON q.QueryID = i.QueryID;
END;
