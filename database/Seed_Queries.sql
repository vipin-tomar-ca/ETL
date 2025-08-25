-- =============================================
-- ETL Framework Metadata Database Schema
-- Seed Data for Queries Table
-- =============================================

-- Insert sample SQL queries for different database types and ETL operations
-- Note: These queries reference DatabaseID from the Connections table

-- SQL Server Queries (DatabaseID = 1 - ETLSourceDB)
INSERT INTO Queries (
    DatabaseID,
    QueryName,
    QueryText,
    QueryType,
    Description,
    IsActive,
    ExecutionOrder,
    TimeoutSeconds,
    CreatedDate,
    ModifiedDate,
    CreatedBy,
    ModifiedBy
) VALUES 
-- Extract customer data
(1, 'Extract_Customers', 
'SELECT 
    CustomerID,
    CustomerName,
    Email,
    Phone,
    Address,
    City,
    State,
    PostalCode,
    Country,
    CreatedDate,
    ModifiedDate
FROM Customers 
WHERE ModifiedDate >= @LastExtractDate', 
'SELECT', 
'Extract customer data for ETL processing', 
1, 1, 300, GETDATE(), GETDATE(), 'ETL_Admin', 'ETL_Admin'),

-- Extract order data
(1, 'Extract_Orders', 
'SELECT 
    OrderID,
    CustomerID,
    OrderDate,
    TotalAmount,
    Status,
    ShippingAddress,
    BillingAddress
FROM Orders 
WHERE OrderDate >= @StartDate AND OrderDate <= @EndDate', 
'SELECT', 
'Extract order data for ETL processing', 
1, 2, 600, GETDATE(), GETDATE(), 'ETL_Admin', 'ETL_Admin'),

-- Extract product data
(1, 'Extract_Products', 
'SELECT 
    ProductID,
    ProductName,
    CategoryID,
    UnitPrice,
    UnitsInStock,
    Discontinued,
    SupplierID
FROM Products 
WHERE Discontinued = 0', 
'SELECT', 
'Extract active product data', 
1, 3, 300, GETDATE(), GETDATE(), 'ETL_Admin', 'ETL_Admin');

-- Oracle Queries (DatabaseID = 2 - ORCL)
INSERT INTO Queries (
    DatabaseID,
    QueryName,
    QueryText,
    QueryType,
    Description,
    IsActive,
    ExecutionOrder,
    TimeoutSeconds,
    CreatedDate,
    ModifiedDate,
    CreatedBy,
    ModifiedBy
) VALUES 
-- Extract employee data
(2, 'Extract_Employees', 
'SELECT 
    EMPLOYEE_ID,
    FIRST_NAME,
    LAST_NAME,
    EMAIL,
    PHONE_NUMBER,
    HIRE_DATE,
    JOB_ID,
    SALARY,
    DEPARTMENT_ID
FROM EMPLOYEES 
WHERE HIRE_DATE >= :last_extract_date', 
'SELECT', 
'Extract employee data from Oracle HR schema', 
1, 1, 300, GETDATE(), GETDATE(), 'ETL_Admin', 'ETL_Admin'),

-- Extract department data
(2, 'Extract_Departments', 
'SELECT 
    DEPARTMENT_ID,
    DEPARTMENT_NAME,
    MANAGER_ID,
    LOCATION_ID
FROM DEPARTMENTS', 
'SELECT', 
'Extract department data from Oracle HR schema', 
1, 2, 180, GETDATE(), GETDATE(), 'ETL_Admin', 'ETL_Admin'),

-- Extract sales data
(2, 'Extract_Sales', 
'SELECT 
    SALE_ID,
    CUSTOMER_ID,
    PRODUCT_ID,
    SALE_DATE,
    QUANTITY,
    UNIT_PRICE,
    TOTAL_AMOUNT
FROM SALES 
WHERE SALE_DATE BETWEEN :start_date AND :end_date', 
'SELECT', 
'Extract sales data for reporting', 
1, 3, 600, GETDATE(), GETDATE(), 'ETL_Admin', 'ETL_Admin');

-- MySQL Queries (DatabaseID = 3 - etl_source)
INSERT INTO Queries (
    DatabaseID,
    QueryName,
    QueryText,
    QueryType,
    Description,
    IsActive,
    ExecutionOrder,
    TimeoutSeconds,
    CreatedDate,
    ModifiedDate,
    CreatedBy,
    ModifiedBy
) VALUES 
-- Extract user data
(3, 'Extract_Users', 
'SELECT 
    user_id,
    username,
    email,
    first_name,
    last_name,
    created_at,
    updated_at,
    is_active
FROM users 
WHERE updated_at >= ?', 
'SELECT', 
'Extract user data from MySQL application database', 
1, 1, 300, GETDATE(), GETDATE(), 'ETL_Admin', 'ETL_Admin'),

-- Extract transaction data
(3, 'Extract_Transactions', 
'SELECT 
    transaction_id,
    user_id,
    amount,
    currency,
    transaction_type,
    status,
    created_at,
    processed_at
FROM transactions 
WHERE created_at BETWEEN ? AND ?', 
'SELECT', 
'Extract transaction data for financial reporting', 
1, 2, 900, GETDATE(), GETDATE(), 'ETL_Admin', 'ETL_Admin'),

-- Extract product catalog
(3, 'Extract_Products', 
'SELECT 
    product_id,
    product_name,
    description,
    price,
    category_id,
    stock_quantity,
    created_at
FROM products 
WHERE is_active = 1', 
'SELECT', 
'Extract active product catalog data', 
1, 3, 300, GETDATE(), GETDATE(), 'ETL_Admin', 'ETL_Admin');

-- Data Warehouse Queries (DatabaseID = 4 - ETLDataWarehouse)
INSERT INTO Queries (
    DatabaseID,
    QueryName,
    QueryText,
    QueryType,
    Description,
    IsActive,
    ExecutionOrder,
    TimeoutSeconds,
    CreatedDate,
    ModifiedDate,
    CreatedBy,
    ModifiedBy
) VALUES 
-- Load customer dimension
(4, 'Load_Customer_Dimension', 
'MERGE INTO DimCustomer AS target
USING (SELECT * FROM #TempCustomerData) AS source
ON target.CustomerID = source.CustomerID
WHEN MATCHED THEN
    UPDATE SET 
        target.CustomerName = source.CustomerName,
        target.Email = source.Email,
        target.ModifiedDate = GETDATE()
WHEN NOT MATCHED THEN
    INSERT (CustomerID, CustomerName, Email, CreatedDate)
    VALUES (source.CustomerID, source.CustomerName, source.Email, GETDATE());', 
'MERGE', 
'Load customer dimension table using MERGE operation', 
1, 1, 600, GETDATE(), GETDATE(), 'ETL_Admin', 'ETL_Admin'),

-- Load fact table
(4, 'Load_Sales_Fact', 
'INSERT INTO FactSales (
    OrderID,
    CustomerID,
    ProductID,
    OrderDate,
    Quantity,
    UnitPrice,
    TotalAmount,
    LoadDate
)
SELECT 
    o.OrderID,
    o.CustomerID,
    od.ProductID,
    o.OrderDate,
    od.Quantity,
    od.UnitPrice,
    od.Quantity * od.UnitPrice as TotalAmount,
    GETDATE() as LoadDate
FROM #TempOrders o
JOIN #TempOrderDetails od ON o.OrderID = od.OrderID;', 
'INSERT', 
'Load sales fact table with order and order details data', 
1, 2, 900, GETDATE(), GETDATE(), 'ETL_Admin', 'ETL_Admin'),

-- Update statistics
(4, 'Update_Statistics', 
'UPDATE STATISTICS DimCustomer;
UPDATE STATISTICS FactSales;
UPDATE STATISTICS DimProduct;', 
'UPDATE', 
'Update statistics for better query performance', 
1, 3, 300, GETDATE(), GETDATE(), 'ETL_Admin', 'ETL_Admin');

-- PostgreSQL Queries (DatabaseID = 5 - etl_source)
INSERT INTO Queries (
    DatabaseID,
    QueryName,
    QueryText,
    QueryType,
    Description,
    IsActive,
    ExecutionOrder,
    TimeoutSeconds,
    CreatedDate,
    ModifiedDate,
    CreatedBy,
    ModifiedBy
) VALUES 
-- Extract inventory data
(5, 'Extract_Inventory', 
'SELECT 
    inventory_id,
    product_id,
    warehouse_id,
    quantity_on_hand,
    reorder_level,
    last_updated
FROM inventory 
WHERE last_updated >= $1', 
'SELECT', 
'Extract inventory data from PostgreSQL warehouse system', 
1, 1, 300, GETDATE(), GETDATE(), 'ETL_Admin', 'ETL_Admin'),

-- Extract supplier data
(5, 'Extract_Suppliers', 
'SELECT 
    supplier_id,
    supplier_name,
    contact_name,
    email,
    phone,
    address,
    city,
    country
FROM suppliers 
WHERE is_active = true', 
'SELECT', 
'Extract active supplier data', 
1, 2, 180, GETDATE(), GETDATE(), 'ETL_Admin', 'ETL_Admin');

-- Verify the inserted data
SELECT 
    q.QueryID,
    c.DatabaseType,
    c.DatabaseName,
    q.QueryName,
    q.QueryType,
    q.Description,
    q.ExecutionOrder,
    q.IsActive
FROM Queries q
INNER JOIN Connections c ON q.DatabaseID = c.DatabaseID
ORDER BY c.DatabaseType, q.ExecutionOrder;

-- Display query count by database type
SELECT 
    c.DatabaseType,
    COUNT(*) as QueryCount,
    SUM(CASE WHEN q.IsActive = 1 THEN 1 ELSE 0 END) as ActiveQueries
FROM Queries q
INNER JOIN Connections c ON q.DatabaseID = c.DatabaseID
GROUP BY c.DatabaseType
ORDER BY c.DatabaseType;

-- Display query count by query type
SELECT 
    QueryType,
    COUNT(*) as QueryCount
FROM Queries
GROUP BY QueryType
ORDER BY QueryType;
