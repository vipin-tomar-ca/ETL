--@name: GetCustomerOrderHistory
--@description: SQL Server table-valued function to get customer order history
--@type: FUNCTION
--@timeout: 30
--@database: SQL Server
--@author: ETL Team
--@version: 1.0
--@tags: customer,orders,function,sqlserver

CREATE OR ALTER FUNCTION [dbo].[GetCustomerOrderHistory](
    @CustomerID INT,
    @StartDate DATETIME2 = NULL,
    @EndDate DATETIME2 = NULL
)
RETURNS TABLE
AS
RETURN
(
    SELECT 
        o.OrderID,
        o.OrderDate,
        o.RequiredDate,
        o.ShippedDate,
        o.Status,
        o.TotalAmount,
        COUNT(od.OrderDetailID) as ItemCount,
        SUM(od.Quantity) as TotalQuantity,
        AVG(od.UnitPrice) as AverageUnitPrice,
        MIN(od.UnitPrice) as MinUnitPrice,
        MAX(od.UnitPrice) as MaxUnitPrice,
        CASE 
            WHEN o.Status = 'Completed' THEN DATEDIFF(day, o.OrderDate, o.ShippedDate)
            ELSE NULL
        END as DaysToShip,
        CASE 
            WHEN o.Status = 'Completed' THEN DATEDIFF(day, o.RequiredDate, o.ShippedDate)
            ELSE NULL
        END as DaysEarlyOrLate
    FROM Orders o
    LEFT JOIN OrderDetails od ON o.OrderID = od.OrderID
    WHERE o.CustomerID = @CustomerID
        AND (@StartDate IS NULL OR o.OrderDate >= @StartDate)
        AND (@EndDate IS NULL OR o.OrderDate <= @EndDate)
    GROUP BY 
        o.OrderID,
        o.OrderDate,
        o.RequiredDate,
        o.ShippedDate,
        o.Status,
        o.TotalAmount
)
