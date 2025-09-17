--@name: SelectCustomersWithPagination
--@description: SQL Server specific query with OFFSET/FETCH pagination
--@type: SELECT
--@timeout: 30
--@database: SQL Server
--@author: ETL Team
--@version: 1.0
--@tags: customer,select,pagination,sqlserver

SELECT 
    CustomerID,
    CustomerName,
    Email,
    Phone,
    CreatedDate,
    ModifiedDate,
    IsActive
FROM Customers 
WHERE IsActive = 1
ORDER BY CustomerName
OFFSET @Offset ROWS
FETCH NEXT @PageSize ROWS ONLY;
