--@name: InvalidSyntaxQuery
--@description: Query with invalid SQL syntax for testing error handling
--@type: SELECT
--@timeout: 30
--@database: SQL Server
--@author: ETL Team
--@version: 1.0
--@tags: invalid,syntax,error,testing

-- This query has invalid syntax for testing purposes
SELCT CustomerID, CustomerName  -- Invalid keyword SELCT instead of SELECT
FROM Customers 
WHERE IsActive = 1
  AND (CreatedDate >= @StartDate  -- Missing closing parenthesis
ORDER BY CustomerName;
