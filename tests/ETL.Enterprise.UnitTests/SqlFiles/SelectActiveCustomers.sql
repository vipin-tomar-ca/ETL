--@name: SelectActiveCustomers
--@description: Select active customers with additional filtering
--@type: SELECT
--@timeout: 30
--@database: SQL Server
--@author: ETL Team
--@version: 1.0
--@tags: customer,active,filter

SELECT 
    CustomerID,
    CustomerName,
    Email,
    Phone,
    CreatedDate
FROM Customers 
WHERE IsActive = 1
  AND CreatedDate >= DATEADD(year, -1, GETDATE())
ORDER BY CustomerName;
