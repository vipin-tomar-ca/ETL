--@name: MultipleQueries
--@description: Multiple SQL queries separated by GO statements
--@type: MIXED
--@timeout: 60
--@database: SQL Server
--@author: ETL Team
--@version: 1.0
--@tags: multiple,queries,batch

-- Query 1: Select active customers
SELECT CustomerID, CustomerName, Email 
FROM Customers 
WHERE IsActive = 1;

GO

-- Query 2: Select recent orders
SELECT OrderID, CustomerID, OrderDate, TotalAmount 
FROM Orders 
WHERE OrderDate >= DATEADD(day, -30, GETDATE());

GO

-- Query 3: Select active products
SELECT ProductID, ProductName, Price, CategoryID 
FROM Products 
WHERE Discontinued = 0;
