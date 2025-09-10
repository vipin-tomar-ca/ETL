--@name: GetTopCustomers
--@description: Get top customers by order count and total spent
--@type: SELECT
--@timeout: 60
--@database: SQL Server
--@author: ETL Team
--@version: 1.0
--@tags: customer,top,ranking

SELECT TOP (@TopCount)
    c.CustomerID,
    c.CustomerName,
    c.Email,
    COUNT(o.OrderID) as OrderCount,
    SUM(o.TotalAmount) as TotalSpent,
    AVG(o.TotalAmount) as AverageOrderValue
FROM Customers c
INNER JOIN Orders o ON c.CustomerID = o.CustomerID
WHERE c.IsActive = 1
  AND o.Status = 'Completed'
GROUP BY c.CustomerID, c.CustomerName, c.Email
ORDER BY TotalSpent DESC;
