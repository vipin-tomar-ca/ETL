--@name: CustomerAggregateQuery
--@description: Aggregate query to calculate customer statistics
--@type: SELECT
--@timeout: 90
--@database: SQL Server
--@author: ETL Team
--@version: 1.0
--@tags: aggregate,customer,statistics,reporting

SELECT 
    c.CustomerID,
    c.CustomerName,
    c.Email,
    COUNT(o.OrderID) as OrderCount,
    SUM(o.TotalAmount) as TotalSpent,
    AVG(o.TotalAmount) as AverageOrderValue,
    MIN(o.OrderDate) as FirstOrderDate,
    MAX(o.OrderDate) as LastOrderDate,
    DATEDIFF(day, MIN(o.OrderDate), MAX(o.OrderDate)) as CustomerLifespanDays
FROM Customers c
LEFT JOIN Orders o ON c.CustomerID = o.CustomerID
WHERE c.IsActive = 1
  AND (@StartDate IS NULL OR o.OrderDate >= @StartDate)
GROUP BY c.CustomerID, c.CustomerName, c.Email
HAVING COUNT(o.OrderID) > @MinOrderCount
ORDER BY TotalSpent DESC;
