--@name: GetCustomersByCity
--@description: Get customers by city
--@type: SELECT
--@timeout: 30
--@database: SQL Server
--@author: ETL Team
--@version: 1.0
--@tags: customer,by-city

SELECT 
    CustomerID,
    CustomerName,
    Email,
    Phone,
    Address,
    City,
    State,
    PostalCode,
    Country
FROM Customers 
WHERE City = @City
  AND IsActive = 1
ORDER BY CustomerName;
