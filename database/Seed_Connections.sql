-- =============================================
-- ETL Framework Metadata Database Schema
-- Seed Data for Connections Table
-- =============================================

-- Insert sample database connections for different database types
-- Note: Replace connection strings with actual values for your environment

-- SQL Server Database Connection
INSERT INTO Connections (
    ConnectionString,
    DatabaseType,
    DatabaseName,
    ServerName,
    Port,
    IsActive,
    CreatedDate,
    ModifiedDate,
    CreatedBy,
    ModifiedBy
) VALUES (
    'Server=SQLSRV01;Database=ETLSourceDB;Trusted_Connection=true;',
    'SQL Server',
    'ETLSourceDB',
    'SQLSRV01',
    1433,
    1,
    GETDATE(),
    GETDATE(),
    'ETL_Admin',
    'ETL_Admin'
);

-- Oracle Database Connection
INSERT INTO Connections (
    ConnectionString,
    DatabaseType,
    DatabaseName,
    ServerName,
    Port,
    IsActive,
    CreatedDate,
    ModifiedDate,
    CreatedBy,
    ModifiedBy
) VALUES (
    'Data Source=(DESCRIPTION=(ADDRESS=(PROTOCOL=TCP)(HOST=ORACLE01)(PORT=1521))(CONNECT_DATA=(SERVICE_NAME=ORCL)));User Id=etl_user;Password=SecurePass123;',
    'Oracle',
    'ORCL',
    'ORACLE01',
    1521,
    1,
    GETDATE(),
    GETDATE(),
    'ETL_Admin',
    'ETL_Admin'
);

-- MySQL Database Connection
INSERT INTO Connections (
    ConnectionString,
    DatabaseType,
    DatabaseName,
    ServerName,
    Port,
    IsActive,
    CreatedDate,
    ModifiedDate,
    CreatedBy,
    ModifiedBy
) VALUES (
    'Server=MYSQL01;Database=etl_source;Uid=etl_user;Pwd=SecurePass123;CharSet=utf8;',
    'MySQL',
    'etl_source',
    'MYSQL01',
    3306,
    1,
    GETDATE(),
    GETDATE(),
    'ETL_Admin',
    'ETL_Admin'
);

-- Additional SQL Server Database (Data Warehouse)
INSERT INTO Connections (
    ConnectionString,
    DatabaseType,
    DatabaseName,
    ServerName,
    Port,
    IsActive,
    CreatedDate,
    ModifiedDate,
    CreatedBy,
    ModifiedBy
) VALUES (
    'Server=SQLSRV02;Database=ETLDataWarehouse;Trusted_Connection=true;',
    'SQL Server',
    'ETLDataWarehouse',
    'SQLSRV02',
    1433,
    1,
    GETDATE(),
    GETDATE(),
    'ETL_Admin',
    'ETL_Admin'
);

-- PostgreSQL Database Connection
INSERT INTO Connections (
    ConnectionString,
    DatabaseType,
    DatabaseName,
    ServerName,
    Port,
    IsActive,
    CreatedDate,
    ModifiedDate,
    CreatedBy,
    ModifiedBy
) VALUES (
    'Host=POSTGRES01;Database=etl_source;Username=etl_user;Password=SecurePass123;',
    'PostgreSQL',
    'etl_source',
    'POSTGRES01',
    5432,
    1,
    GETDATE(),
    GETDATE(),
    'ETL_Admin',
    'ETL_Admin'
);

-- Inactive Connection Example (for testing)
INSERT INTO Connections (
    ConnectionString,
    DatabaseType,
    DatabaseName,
    ServerName,
    Port,
    IsActive,
    CreatedDate,
    ModifiedDate,
    CreatedBy,
    ModifiedBy
) VALUES (
    'Server=OLDSRV01;Database=LegacyDB;Trusted_Connection=true;',
    'SQL Server',
    'LegacyDB',
    'OLDSRV01',
    1433,
    0, -- Inactive
    GETDATE(),
    GETDATE(),
    'ETL_Admin',
    'ETL_Admin'
);

-- Verify the inserted data
SELECT 
    DatabaseID,
    DatabaseType,
    DatabaseName,
    ServerName,
    Port,
    IsActive,
    CreatedDate
FROM Connections
ORDER BY DatabaseID;

-- Display connection count by database type
SELECT 
    DatabaseType,
    COUNT(*) as ConnectionCount,
    SUM(CASE WHEN IsActive = 1 THEN 1 ELSE 0 END) as ActiveConnections
FROM Connections
GROUP BY DatabaseType
ORDER BY DatabaseType;
