--@name: PerformanceTestQuery
--@description: Query designed for performance testing with large datasets
--@type: SELECT
--@timeout: 300
--@database: SQL Server
--@author: ETL Team
--@version: 1.0
--@tags: performance,large,dataset,testing

SELECT 
    c.CustomerID,
    c.CustomerName,
    c.Email,
    COUNT(o.OrderID) as OrderCount,
    SUM(o.TotalAmount) as TotalSpent,
    AVG(o.TotalAmount) as AverageOrderValue
FROM Customers c
LEFT JOIN Orders o ON c.CustomerID = o.CustomerID
WHERE c.IsActive = 1
GROUP BY c.CustomerID, c.CustomerName, c.Email
ORDER BY TotalSpent DESC;
