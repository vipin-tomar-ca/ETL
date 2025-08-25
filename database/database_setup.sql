-- =============================================
-- ETL Metadata Database Setup Script
-- =============================================

USE master;
GO

-- Create ETL Metadata Database
IF NOT EXISTS (SELECT name FROM sys.databases WHERE name = 'ETL_Metadata')
BEGIN
    CREATE DATABASE ETL_Metadata;
END
GO

USE ETL_Metadata;
GO

-- Create Connections table to store database connection information
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[Connections]') AND type in (N'U'))
BEGIN
    CREATE TABLE [dbo].[Connections](
        [ConnectionID] [int] IDENTITY(1,1) NOT NULL,
        [ConnectionName] [nvarchar](100) NOT NULL,
        [DatabaseType] [nvarchar](50) NOT NULL, -- SQLSERVER, ORACLE, MYSQL
        [ConnectionString] [nvarchar](max) NOT NULL,
        [ServerName] [nvarchar](100) NULL,
        [DatabaseName] [nvarchar](100) NULL,
        [UserName] [nvarchar](100) NULL,
        [Password] [nvarchar](100) NULL,
        [IsActive] [bit] NOT NULL DEFAULT(1),
        [CreatedDate] [datetime] NOT NULL DEFAULT(GETDATE()),
        [ModifiedDate] [datetime] NOT NULL DEFAULT(GETDATE()),
        CONSTRAINT [PK_Connections] PRIMARY KEY CLUSTERED ([ConnectionID] ASC)
    );
END
GO

-- Create Tables table to store table extraction configuration
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[Tables]') AND type in (N'U'))
BEGIN
    CREATE TABLE [dbo].[Tables](
        [TableID] [int] IDENTITY(1,1) NOT NULL,
        [ConnectionID] [int] NOT NULL,
        [TableName] [nvarchar](200) NOT NULL,
        [SchemaName] [nvarchar](100) NULL,
        [ExtractionQuery] [nvarchar](max) NULL, -- Custom query if needed
        [OutputType] [nvarchar](50) NOT NULL DEFAULT('PARQUET'), -- PARQUET, SQLSERVER
        [StagingTableName] [nvarchar](200) NULL, -- For SQL Server output
        [ParquetFilePath] [nvarchar](500) NULL, -- For Parquet output
        [BatchSize] [int] NOT NULL DEFAULT(100000),
        [IsActive] [bit] NOT NULL DEFAULT(1),
        [LastExtractionDate] [datetime] NULL,
        [CreatedDate] [datetime] NOT NULL DEFAULT(GETDATE()),
        [ModifiedDate] [datetime] NOT NULL DEFAULT(GETDATE()),
        CONSTRAINT [PK_Tables] PRIMARY KEY CLUSTERED ([TableID] ASC),
        CONSTRAINT [FK_Tables_Connections] FOREIGN KEY([ConnectionID]) REFERENCES [dbo].[Connections] ([ConnectionID])
    );
END
GO

-- Create Logs table for error logging
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[Logs]') AND type in (N'U'))
BEGIN
    CREATE TABLE [dbo].[Logs](
        [LogID] [bigint] IDENTITY(1,1) NOT NULL,
        [LogDate] [datetime] NOT NULL DEFAULT(GETDATE()),
        [LogLevel] [nvarchar](20) NOT NULL, -- INFO, WARNING, ERROR
        [Source] [nvarchar](200) NULL,
        [Message] [nvarchar](max) NULL,
        [ErrorCode] [int] NULL,
        [ErrorDescription] [nvarchar](max) NULL,
        [ConnectionID] [int] NULL,
        [TableID] [int] NULL,
        [RowsProcessed] [bigint] NULL,
        [Duration] [int] NULL, -- Duration in seconds
        CONSTRAINT [PK_Logs] PRIMARY KEY CLUSTERED ([LogID] ASC),
        CONSTRAINT [FK_Logs_Connections] FOREIGN KEY([ConnectionID]) REFERENCES [dbo].[Connections] ([ConnectionID]),
        CONSTRAINT [FK_Logs_Tables] FOREIGN KEY([TableID]) REFERENCES [dbo].[Tables] ([TableID])
    );
END
GO

-- Create indexes for better performance
CREATE NONCLUSTERED INDEX [IX_Connections_DatabaseType] ON [dbo].[Connections] ([DatabaseType]) WHERE [IsActive] = 1;
CREATE NONCLUSTERED INDEX [IX_Tables_ConnectionID] ON [dbo].[Tables] ([ConnectionID]) WHERE [IsActive] = 1;
CREATE NONCLUSTERED INDEX [IX_Logs_LogDate] ON [dbo].[Logs] ([LogDate]);
CREATE NONCLUSTERED INDEX [IX_Logs_LogLevel] ON [dbo].[Logs] ([LogLevel]) WHERE [LogLevel] = 'ERROR';

-- Insert sample connection data
INSERT INTO [dbo].[Connections] ([ConnectionName], [DatabaseType], [ConnectionString], [ServerName], [DatabaseName], [UserName], [Password])
VALUES 
    ('SQLServer_Source1', 'SQLSERVER', 'Data Source=SQLSRV01;Initial Catalog=AdventureWorks;Integrated Security=SSPI;', 'SQLSRV01', 'AdventureWorks', NULL, NULL),
    ('Oracle_Source1', 'ORACLE', 'Data Source=ORACLE01;User Id=etl_user;Password=etl_password;', 'ORACLE01', 'ORCL', 'etl_user', 'etl_password'),
    ('MySQL_Source1', 'MYSQL', 'Server=mysql01;Database=sakila;Uid=etl_user;Pwd=etl_password;', 'mysql01', 'sakila', 'etl_user', 'etl_password');

-- Insert sample table configurations
INSERT INTO [dbo].[Tables] ([ConnectionID], [TableName], [SchemaName], [OutputType], [StagingTableName], [ParquetFilePath], [BatchSize])
VALUES 
    (1, 'Sales.SalesOrderHeader', 'Sales', 'SQLSERVER', 'stg_SalesOrderHeader', NULL, 50000),
    (1, 'Production.Product', 'Production', 'SQLSERVER', 'stg_Product', NULL, 10000),
    (2, 'CUSTOMERS', NULL, 'PARQUET', NULL, 'C:\ETL\Parquet\Oracle\customers.parquet', 25000),
    (2, 'ORDERS', NULL, 'PARQUET', NULL, 'C:\ETL\Parquet\Oracle\orders.parquet', 50000),
    (3, 'customer', NULL, 'PARQUET', NULL, 'C:\ETL\Parquet\MySQL\customer.parquet', 15000),
    (3, 'rental', NULL, 'PARQUET', NULL, 'C:\ETL\Parquet\MySQL\rental.parquet', 30000);

-- Create stored procedure to get active connections and tables
CREATE PROCEDURE [dbo].[GetActiveExtractions]
AS
BEGIN
    SET NOCOUNT ON;
    
    SELECT 
        c.ConnectionID,
        c.ConnectionName,
        c.DatabaseType,
        c.ConnectionString,
        c.ServerName,
        c.DatabaseName,
        t.TableID,
        t.TableName,
        t.SchemaName,
        t.ExtractionQuery,
        t.OutputType,
        t.StagingTableName,
        t.ParquetFilePath,
        t.BatchSize
    FROM [dbo].[Connections] c
    INNER JOIN [dbo].[Tables] t ON c.ConnectionID = t.ConnectionID
    WHERE c.IsActive = 1 AND t.IsActive = 1
    ORDER BY c.ConnectionID, t.TableID;
END
GO

-- Create stored procedure to log extraction results
CREATE PROCEDURE [dbo].[LogExtractionResult]
    @ConnectionID int,
    @TableID int,
    @LogLevel nvarchar(20),
    @Message nvarchar(max),
    @ErrorCode int = NULL,
    @ErrorDescription nvarchar(max) = NULL,
    @RowsProcessed bigint = NULL,
    @Duration int = NULL
AS
BEGIN
    SET NOCOUNT ON;
    
    INSERT INTO [dbo].[Logs] (
        [LogLevel], [Source], [Message], [ErrorCode], [ErrorDescription], 
        [ConnectionID], [TableID], [RowsProcessed], [Duration]
    )
    VALUES (
        @LogLevel, 
        (SELECT ConnectionName FROM [dbo].[Connections] WHERE ConnectionID = @ConnectionID) + '.' + 
        (SELECT TableName FROM [dbo].[Tables] WHERE TableID = @TableID),
        @Message, @ErrorCode, @ErrorDescription, @ConnectionID, @TableID, @RowsProcessed, @Duration
    );
    
    -- Update last extraction date for successful extractions
    IF @LogLevel = 'INFO' AND @ErrorCode IS NULL
    BEGIN
        UPDATE [dbo].[Tables] 
        SET LastExtractionDate = GETDATE(), ModifiedDate = GETDATE()
        WHERE TableID = @TableID;
    END
END
GO

-- Create ETL Staging Database
USE master;
GO

IF NOT EXISTS (SELECT name FROM sys.databases WHERE name = 'ETL_Staging')
BEGIN
    CREATE DATABASE ETL_Staging;
END
GO

USE ETL_Staging;
GO

-- Create sample staging tables
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[stg_SalesOrderHeader]') AND type in (N'U'))
BEGIN
    CREATE TABLE [dbo].[stg_SalesOrderHeader](
        [SalesOrderID] [int] NOT NULL,
        [RevisionNumber] [tinyint] NOT NULL,
        [OrderDate] [datetime] NOT NULL,
        [DueDate] [datetime] NOT NULL,
        [ShipDate] [datetime] NULL,
        [Status] [tinyint] NOT NULL,
        [OnlineOrderFlag] [bit] NOT NULL,
        [SalesOrderNumber] [nvarchar](25) NOT NULL,
        [PurchaseOrderNumber] [nvarchar](25) NULL,
        [AccountNumber] [nvarchar](15) NULL,
        [CustomerID] [int] NOT NULL,
        [SalesPersonID] [int] NULL,
        [TerritoryID] [int] NULL,
        [BillToAddressID] [int] NOT NULL,
        [ShipToAddressID] [int] NOT NULL,
        [ShipMethodID] [int] NOT NULL,
        [CreditCardID] [int] NULL,
        [CreditCardApprovalCode] [varchar](15) NULL,
        [CurrencyRateID] [int] NULL,
        [SubTotal] [money] NOT NULL,
        [TaxAmt] [money] NOT NULL,
        [Freight] [money] NOT NULL,
        [TotalDue] [money] NOT NULL,
        [Comment] [nvarchar](128) NULL,
        [rowguid] [uniqueidentifier] NOT NULL,
        [ModifiedDate] [datetime] NOT NULL,
        [ExtractionDate] [datetime] NOT NULL DEFAULT(GETDATE())
    );
END
GO

IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[stg_Product]') AND type in (N'U'))
BEGIN
    CREATE TABLE [dbo].[stg_Product](
        [ProductID] [int] NOT NULL,
        [Name] [nvarchar](50) NOT NULL,
        [ProductNumber] [nvarchar](25) NOT NULL,
        [MakeFlag] [bit] NOT NULL,
        [FinishedGoodsFlag] [bit] NOT NULL,
        [Color] [nvarchar](15) NULL,
        [SafetyStockLevel] [smallint] NOT NULL,
        [ReorderPoint] [smallint] NOT NULL,
        [StandardCost] [money] NOT NULL,
        [ListPrice] [money] NOT NULL,
        [Size] [nvarchar](5) NULL,
        [SizeUnitMeasureCode] [nchar](3) NULL,
        [WeightUnitMeasureCode] [nchar](3) NULL,
        [Weight] [decimal](8, 2) NULL,
        [DaysToManufacture] [int] NOT NULL,
        [ProductLine] [nchar](2) NULL,
        [Class] [nchar](2) NULL,
        [Style] [nchar](2) NULL,
        [ProductSubcategoryID] [int] NULL,
        [ProductModelID] [int] NULL,
        [SellStartDate] [datetime] NOT NULL,
        [SellEndDate] [datetime] NULL,
        [DiscontinuedDate] [datetime] NULL,
        [rowguid] [uniqueidentifier] NOT NULL,
        [ModifiedDate] [datetime] NOT NULL,
        [ExtractionDate] [datetime] NOT NULL DEFAULT(GETDATE())
    );
END
GO

-- Create indexes on staging tables
CREATE NONCLUSTERED INDEX [IX_stg_SalesOrderHeader_ExtractionDate] ON [dbo].[stg_SalesOrderHeader] ([ExtractionDate]);
CREATE NONCLUSTERED INDEX [IX_stg_Product_ExtractionDate] ON [dbo].[stg_Product] ([ExtractionDate]);

PRINT 'ETL Metadata and Staging databases setup completed successfully!';
