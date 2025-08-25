-- =============================================
-- Customer Database Sample Data Initialization
-- =============================================
-- This script creates sample customer data for testing the ETL application

USE CustomerDB;
GO

-- Create Customers table
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[Customers]') AND type in (N'U'))
BEGIN
    CREATE TABLE [dbo].[Customers](
        [CustomerId] [int] IDENTITY(1,1) NOT NULL,
        [CustomerName] [varchar](100) NOT NULL,
        [Email] [varchar](100) NOT NULL,
        [Phone] [varchar](20) NULL,
        [Address] [varchar](200) NULL,
        [City] [varchar](50) NULL,
        [State] [varchar](50) NULL,
        [PostalCode] [varchar](10) NULL,
        [Country] [varchar](50) NULL,
        [IsActive] [bit] NOT NULL DEFAULT(1),
        [CreatedDate] [datetime] NOT NULL DEFAULT(GETDATE()),
        [ModifiedDate] [datetime] NOT NULL DEFAULT(GETDATE()),
        CONSTRAINT [PK_Customers] PRIMARY KEY CLUSTERED ([CustomerId] ASC)
    );
END
GO

-- Create CustomerAddresses table
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[CustomerAddresses]') AND type in (N'U'))
BEGIN
    CREATE TABLE [dbo].[CustomerAddresses](
        [AddressId] [int] IDENTITY(1,1) NOT NULL,
        [CustomerId] [int] NOT NULL,
        [AddressLine1] [varchar](100) NOT NULL,
        [AddressLine2] [varchar](100) NULL,
        [City] [varchar](50) NOT NULL,
        [State] [varchar](50) NOT NULL,
        [PostalCode] [varchar](10) NOT NULL,
        [Country] [varchar](50) NOT NULL,
        [IsPrimary] [bit] NOT NULL DEFAULT(0),
        [AddressType] [varchar](20) NOT NULL DEFAULT('HOME'),
        [CreatedDate] [datetime] NOT NULL DEFAULT(GETDATE()),
        CONSTRAINT [PK_CustomerAddresses] PRIMARY KEY CLUSTERED ([AddressId] ASC),
        CONSTRAINT [FK_CustomerAddresses_Customers] FOREIGN KEY([CustomerId]) REFERENCES [dbo].[Customers] ([CustomerId])
    );
END
GO

-- Insert sample customer data
IF NOT EXISTS (SELECT * FROM [dbo].[Customers] WHERE [CustomerId] = 1)
BEGIN
    INSERT INTO [dbo].[Customers] ([CustomerName], [Email], [Phone], [Address], [City], [State], [PostalCode], [Country], [IsActive])
    VALUES 
    ('John Smith', 'john.smith@email.com', '+1-555-0101', '123 Main St', 'New York', 'NY', '10001', 'USA', 1),
    ('Jane Doe', 'jane.doe@email.com', '+1-555-0102', '456 Oak Ave', 'Los Angeles', 'CA', '90210', 'USA', 1),
    ('Bob Johnson', 'bob.johnson@email.com', '+1-555-0103', '789 Pine Rd', 'Chicago', 'IL', '60601', 'USA', 1),
    ('Alice Brown', 'alice.brown@email.com', '+1-555-0104', '321 Elm St', 'Houston', 'TX', '77001', 'USA', 1),
    ('Charlie Wilson', 'charlie.wilson@email.com', '+1-555-0105', '654 Maple Dr', 'Phoenix', 'AZ', '85001', 'USA', 1),
    ('Diana Davis', 'diana.davis@email.com', '+1-555-0106', '987 Cedar Ln', 'Philadelphia', 'PA', '19101', 'USA', 1),
    ('Edward Miller', 'edward.miller@email.com', '+1-555-0107', '147 Birch Way', 'San Antonio', 'TX', '78201', 'USA', 1),
    ('Fiona Garcia', 'fiona.garcia@email.com', '+1-555-0108', '258 Spruce Ct', 'San Diego', 'CA', '92101', 'USA', 1),
    ('George Martinez', 'george.martinez@email.com', '+1-555-0109', '369 Willow Pl', 'Dallas', 'TX', '75201', 'USA', 1),
    ('Helen Rodriguez', 'helen.rodriguez@email.com', '+1-555-0110', '741 Aspen Blvd', 'San Jose', 'CA', '95101', 'USA', 1);
END
GO

-- Insert sample customer addresses
IF NOT EXISTS (SELECT * FROM [dbo].[CustomerAddresses] WHERE [AddressId] = 1)
BEGIN
    INSERT INTO [dbo].[CustomerAddresses] ([CustomerId], [AddressLine1], [AddressLine2], [City], [State], [PostalCode], [Country], [IsPrimary], [AddressType])
    VALUES 
    (1, '123 Main St', 'Apt 4B', 'New York', 'NY', '10001', 'USA', 1, 'HOME'),
    (1, '456 Business Ave', 'Suite 100', 'New York', 'NY', '10002', 'USA', 0, 'WORK'),
    (2, '456 Oak Ave', NULL, 'Los Angeles', 'CA', '90210', 'USA', 1, 'HOME'),
    (3, '789 Pine Rd', 'Unit 2C', 'Chicago', 'IL', '60601', 'USA', 1, 'HOME'),
    (4, '321 Elm St', NULL, 'Houston', 'TX', '77001', 'USA', 1, 'HOME'),
    (5, '654 Maple Dr', 'Building A', 'Phoenix', 'AZ', '85001', 'USA', 1, 'HOME'),
    (6, '987 Cedar Ln', 'Floor 3', 'Philadelphia', 'PA', '19101', 'USA', 1, 'HOME'),
    (7, '147 Birch Way', NULL, 'San Antonio', 'TX', '78201', 'USA', 1, 'HOME'),
    (8, '258 Spruce Ct', 'Apt 7D', 'San Diego', 'CA', '92101', 'USA', 1, 'HOME'),
    (9, '369 Willow Pl', NULL, 'Dallas', 'TX', '75201', 'USA', 1, 'HOME'),
    (10, '741 Aspen Blvd', 'Suite 500', 'San Jose', 'CA', '95101', 'USA', 1, 'HOME');
END
GO

-- Create indexes for better performance
IF NOT EXISTS (SELECT * FROM sys.indexes WHERE name = 'IX_Customers_Email')
BEGIN
    CREATE NONCLUSTERED INDEX [IX_Customers_Email] ON [dbo].[Customers] ([Email] ASC);
END

IF NOT EXISTS (SELECT * FROM sys.indexes WHERE name = 'IX_Customers_IsActive')
BEGIN
    CREATE NONCLUSTERED INDEX [IX_Customers_IsActive] ON [dbo].[Customers] ([IsActive] ASC);
END

IF NOT EXISTS (SELECT * FROM sys.indexes WHERE name = 'IX_CustomerAddresses_CustomerId')
BEGIN
    CREATE NONCLUSTERED INDEX [IX_CustomerAddresses_CustomerId] ON [dbo].[CustomerAddresses] ([CustomerId] ASC);
END

IF NOT EXISTS (SELECT * FROM sys.indexes WHERE name = 'IX_CustomerAddresses_IsPrimary')
BEGIN
    CREATE NONCLUSTERED INDEX [IX_CustomerAddresses_IsPrimary] ON [dbo].[CustomerAddresses] ([IsPrimary] ASC);
END

-- Create a view for easy data access
IF EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[vw_CustomerWithAddress]') AND type in (N'V'))
    DROP VIEW [dbo].[vw_CustomerWithAddress]
GO

CREATE VIEW [dbo].[vw_CustomerWithAddress] AS
SELECT 
    c.[CustomerId],
    c.[CustomerName],
    c.[Email],
    c.[Phone],
    c.[IsActive],
    c.[CreatedDate],
    ca.[AddressLine1],
    ca.[AddressLine2],
    ca.[City],
    ca.[State],
    ca.[PostalCode],
    ca.[Country],
    ca.[AddressType]
FROM [dbo].[Customers] c
LEFT JOIN [dbo].[CustomerAddresses] ca ON c.[CustomerId] = ca.[CustomerId] AND ca.[IsPrimary] = 1
WHERE c.[IsActive] = 1;
GO

-- Verify the data
SELECT 'Customers' as TableName, COUNT(*) as RecordCount FROM [dbo].[Customers]
UNION ALL
SELECT 'CustomerAddresses' as TableName, COUNT(*) as RecordCount FROM [dbo].[CustomerAddresses];

SELECT TOP 5 * FROM [dbo].[vw_CustomerWithAddress];

PRINT 'Customer database sample data initialized successfully!';
