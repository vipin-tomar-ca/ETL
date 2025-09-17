--@name: CustomerOrderSummary
--@description: SQL Server view providing customer order summary information
--@type: VIEW
--@timeout: 30
--@database: SQL Server
--@author: ETL Team
--@version: 1.0
--@tags: customer,orders,view,sqlserver

CREATE OR ALTER VIEW [dbo].[CustomerOrderSummary]
AS
SELECT 
    c.CustomerID,
    c.CustomerName,
    c.Email,
    c.Phone,
    c.CreatedDate as CustomerSince,
    COUNT(o.OrderID) as TotalOrders,
    COUNT(CASE WHEN o.Status = 'Completed' THEN 1 END) as CompletedOrders,
    COUNT(CASE WHEN o.Status = 'Pending' THEN 1 END) as PendingOrders,
    COUNT(CASE WHEN o.Status = 'Cancelled' THEN 1 END) as CancelledOrders,
    ISNULL(SUM(CASE WHEN o.Status = 'Completed' THEN o.TotalAmount END), 0) as TotalSpent,
    ISNULL(AVG(CASE WHEN o.Status = 'Completed' THEN o.TotalAmount END), 0) as AverageOrderValue,
    MAX(o.OrderDate) as LastOrderDate,
    MIN(o.OrderDate) as FirstOrderDate,
    DATEDIFF(day, MIN(o.OrderDate), MAX(o.OrderDate)) as CustomerLifespanDays,
    CASE 
        WHEN COUNT(o.OrderID) = 0 THEN 'New'
        WHEN COUNT(o.OrderID) = 1 THEN 'Single Purchase'
        WHEN COUNT(o.OrderID) BETWEEN 2 AND 5 THEN 'Regular'
        WHEN COUNT(o.OrderID) BETWEEN 6 AND 20 THEN 'Frequent'
        ELSE 'VIP'
    END as CustomerType,
    CASE 
        WHEN ISNULL(SUM(CASE WHEN o.Status = 'Completed' THEN o.TotalAmount END), 0) = 0 THEN 'Bronze'
        WHEN ISNULL(SUM(CASE WHEN o.Status = 'Completed' THEN o.TotalAmount END), 0) < 1000 THEN 'Silver'
        WHEN ISNULL(SUM(CASE WHEN o.Status = 'Completed' THEN o.TotalAmount END), 0) < 5000 THEN 'Gold'
        ELSE 'Platinum'
    END as CustomerTier
FROM Customers c
LEFT JOIN Orders o ON c.CustomerID = o.CustomerID
WHERE c.IsActive = 1
GROUP BY 
    c.CustomerID,
    c.CustomerName,
    c.Email,
    c.Phone,
    c.CreatedDate
