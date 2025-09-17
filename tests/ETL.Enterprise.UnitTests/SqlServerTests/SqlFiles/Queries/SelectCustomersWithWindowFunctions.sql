--@name: SelectCustomersWithWindowFunctions
--@description: SQL Server query using window functions for ranking and aggregation
--@type: SELECT
--@timeout: 30
--@database: SQL Server
--@author: ETL Team
--@version: 1.0
--@tags: customer,select,window-functions,sqlserver

SELECT 
    CustomerID,
    CustomerName,
    Email,
    TotalOrders,
    TotalSpent,
    ROW_NUMBER() OVER (ORDER BY TotalSpent DESC) as CustomerRank,
    RANK() OVER (ORDER BY TotalSpent DESC) as CustomerRankWithTies,
    DENSE_RANK() OVER (ORDER BY TotalSpent DESC) as DenseCustomerRank,
    LAG(TotalSpent, 1, 0) OVER (ORDER BY TotalSpent DESC) as PreviousCustomerSpent,
    LEAD(TotalSpent, 1, 0) OVER (ORDER BY TotalSpent DESC) as NextCustomerSpent,
    PERCENT_RANK() OVER (ORDER BY TotalSpent DESC) as PercentileRank
FROM (
    SELECT 
        c.CustomerID,
        c.CustomerName,
        c.Email,
        COUNT(o.OrderID) as TotalOrders,
        ISNULL(SUM(o.TotalAmount), 0) as TotalSpent
    FROM Customers c
    LEFT JOIN Orders o ON c.CustomerID = o.CustomerID
    WHERE c.IsActive = 1
    GROUP BY c.CustomerID, c.CustomerName, c.Email
) CustomerStats
ORDER BY TotalSpent DESC;
