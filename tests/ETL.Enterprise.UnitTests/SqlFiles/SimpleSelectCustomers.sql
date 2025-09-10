--@name: SimpleSelectCustomers
--@description: Simple SELECT query to retrieve active customers
--@type: SELECT
--@timeout: 30
--@database: SQL Server
--@author: ETL Team
--@version: 1.0
--@tags: customer,select,active

SELECT 
    CustomerID,
    CustomerName,
    Email,
    Phone,
    CreatedDate,
    ModifiedDate
FROM Customers 
WHERE IsActive = 1
ORDER BY CustomerName;
