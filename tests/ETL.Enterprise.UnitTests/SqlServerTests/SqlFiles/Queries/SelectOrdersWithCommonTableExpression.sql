--@name: SelectOrdersWithCommonTableExpression
--@description: SQL Server query using Common Table Expression (CTE) for complex data processing
--@type: SELECT
--@timeout: 30
--@database: SQL Server
--@author: ETL Team
--@version: 1.0
--@tags: orders,select,cte,sqlserver

WITH OrderSummary AS (
    SELECT 
        o.OrderID,
        o.CustomerID,
        o.OrderDate,
        o.TotalAmount,
        o.Status,
        c.CustomerName,
        c.Email,
        ROW_NUMBER() OVER (PARTITION BY o.CustomerID ORDER BY o.OrderDate DESC) as OrderRank
    FROM Orders o
    INNER JOIN Customers c ON o.CustomerID = c.CustomerID
    WHERE o.OrderDate >= @StartDate
        AND o.OrderDate <= @EndDate
),
OrderDetails AS (
    SELECT 
        od.OrderID,
        COUNT(*) as ItemCount,
        SUM(od.Quantity * od.UnitPrice) as LineTotal,
        AVG(od.UnitPrice) as AverageUnitPrice
    FROM OrderDetails od
    GROUP BY od.OrderID
),
CustomerOrderStats AS (
    SELECT 
        CustomerID,
        COUNT(*) as TotalOrders,
        SUM(TotalAmount) as TotalSpent,
        AVG(TotalAmount) as AverageOrderValue,
        MAX(OrderDate) as LastOrderDate
    FROM Orders
    WHERE OrderDate >= DATEADD(month, -12, GETDATE())
    GROUP BY CustomerID
)
SELECT 
    os.OrderID,
    os.CustomerID,
    os.CustomerName,
    os.Email,
    os.OrderDate,
    os.TotalAmount,
    os.Status,
    os.OrderRank,
    od.ItemCount,
    od.LineTotal,
    od.AverageUnitPrice,
    cos.TotalOrders,
    cos.TotalSpent,
    cos.AverageOrderValue,
    cos.LastOrderDate,
    CASE 
        WHEN cos.TotalSpent > 10000 THEN 'VIP'
        WHEN cos.TotalSpent > 5000 THEN 'Gold'
        WHEN cos.TotalSpent > 1000 THEN 'Silver'
        ELSE 'Bronze'
    END as CustomerTier
FROM OrderSummary os
LEFT JOIN OrderDetails od ON os.OrderID = od.OrderID
LEFT JOIN CustomerOrderStats cos ON os.CustomerID = cos.CustomerID
WHERE os.OrderRank <= 5  -- Top 5 orders per customer
ORDER BY os.CustomerID, os.OrderDate DESC;
