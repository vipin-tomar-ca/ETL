--@name: GetCustomerById
--@description: Get customer by ID
--@type: SELECT
--@timeout: 30
--@database: SQL Server
--@author: ETL Team
--@version: 1.0
--@tags: customer,by-id

SELECT 
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
    ModifiedDate,
    IsActive
FROM Customers 
WHERE CustomerID = @CustomerID;
