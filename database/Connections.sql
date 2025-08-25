-- =============================================
-- ETL Framework Metadata Database Schema
-- Connections Table
-- =============================================

-- Create the Connections table to store database connection information
CREATE TABLE Connections (
    DatabaseID INT IDENTITY(1,1) PRIMARY KEY,
    ConnectionString NVARCHAR(1000) NOT NULL,
    DatabaseType NVARCHAR(50) NOT NULL,
    DatabaseName NVARCHAR(100) NOT NULL,
    ServerName NVARCHAR(255) NOT NULL,
    Port INT NULL,
    IsActive BIT DEFAULT 1 NOT NULL,
    CreatedDate DATETIME2 DEFAULT GETDATE() NOT NULL,
    ModifiedDate DATETIME2 DEFAULT GETDATE() NOT NULL,
    CreatedBy NVARCHAR(100) DEFAULT SYSTEM_USER NOT NULL,
    ModifiedBy NVARCHAR(100) DEFAULT SYSTEM_USER NOT NULL
);

-- Add comments to the table
EXEC sp_addextendedproperty 
    @name = N'MS_Description',
    @value = N'Stores database connection information for ETL framework',
    @level0type = N'SCHEMA',
    @level0name = N'dbo',
    @level1type = N'TABLE',
    @level1name = N'Connections';

-- Add comments to columns
EXEC sp_addextendedproperty 
    @name = N'MS_Description',
    @value = N'Primary key identifier for the database connection',
    @level0type = N'SCHEMA',
    @level0name = N'dbo',
    @level1type = N'TABLE',
    @level1name = N'Connections',
    @level2type = N'COLUMN',
    @level2name = N'DatabaseID';

EXEC sp_addextendedproperty 
    @name = N'MS_Description',
    @value = N'Connection string for the database',
    @level0type = N'SCHEMA',
    @level0name = N'dbo',
    @level1type = N'TABLE',
    @level1name = N'Connections',
    @level2type = N'COLUMN',
    @level2name = N'ConnectionString';

EXEC sp_addextendedproperty 
    @name = N'MS_Description',
    @value = N'Type of database (SQL Server, Oracle, MySQL, etc.)',
    @level0type = N'SCHEMA',
    @level0name = N'dbo',
    @level1type = N'TABLE',
    @level1name = N'Connections',
    @level2type = N'COLUMN',
    @level2name = N'DatabaseType';

EXEC sp_addextendedproperty 
    @name = N'MS_Description',
    @value = N'Name of the database',
    @level0type = N'SCHEMA',
    @level0name = N'dbo',
    @level1type = N'TABLE',
    @level1name = N'Connections',
    @level2type = N'COLUMN',
    @level2name = N'DatabaseName';

EXEC sp_addextendedproperty 
    @name = N'MS_Description',
    @value = N'Server name or host address',
    @level0type = N'SCHEMA',
    @level0name = N'dbo',
    @level1type = N'TABLE',
    @level1name = N'Connections',
    @level2type = N'COLUMN',
    @level2name = N'ServerName';

EXEC sp_addextendedproperty 
    @name = N'MS_Description',
    @value = N'Port number for the database connection',
    @level0type = N'SCHEMA',
    @level0name = N'dbo',
    @level1type = N'TABLE',
    @level1name = N'Connections',
    @level2type = N'COLUMN',
    @level2name = N'Port';

EXEC sp_addextendedproperty 
    @name = N'MS_Description',
    @value = N'Flag indicating if the connection is active',
    @level0type = N'SCHEMA',
    @level0name = N'dbo',
    @level1type = N'TABLE',
    @level1name = N'Connections',
    @level2type = N'COLUMN',
    @level2name = N'IsActive';

EXEC sp_addextendedproperty 
    @name = N'MS_Description',
    @value = N'Date when the connection record was created',
    @level0type = N'SCHEMA',
    @level0name = N'dbo',
    @level1type = N'TABLE',
    @level1name = N'Connections',
    @level2type = N'COLUMN',
    @level2name = N'CreatedDate';

EXEC sp_addextendedproperty 
    @name = N'MS_Description',
    @value = N'Date when the connection record was last modified',
    @level0type = N'SCHEMA',
    @level0name = N'dbo',
    @level1type = N'TABLE',
    @level1name = N'Connections',
    @level2type = N'COLUMN',
    @level2name = N'ModifiedDate';

EXEC sp_addextendedproperty 
    @name = N'MS_Description',
    @value = N'User who created the connection record',
    @level0type = N'SCHEMA',
    @level0name = N'dbo',
    @level1type = N'TABLE',
    @level1name = N'Connections',
    @level2type = N'COLUMN',
    @level2name = N'CreatedBy';

EXEC sp_addextendedproperty 
    @name = N'MS_Description',
    @value = N'User who last modified the connection record',
    @level0type = N'SCHEMA',
    @level0name = N'dbo',
    @level1type = N'TABLE',
    @level1name = N'Connections',
    @level2type = N'COLUMN',
    @level2name = N'ModifiedBy';

-- Create indexes for better performance
CREATE INDEX IX_Connections_DatabaseType ON Connections(DatabaseType);
CREATE INDEX IX_Connections_IsActive ON Connections(IsActive);
CREATE INDEX IX_Connections_ServerName ON Connections(ServerName);

-- Create a trigger to update ModifiedDate and ModifiedBy
CREATE TRIGGER TR_Connections_UpdateModified
ON Connections
AFTER UPDATE
AS
BEGIN
    SET NOCOUNT ON;
    
    UPDATE Connections
    SET ModifiedDate = GETDATE(),
        ModifiedBy = SYSTEM_USER
    FROM Connections c
    INNER JOIN inserted i ON c.DatabaseID = i.DatabaseID;
END;
