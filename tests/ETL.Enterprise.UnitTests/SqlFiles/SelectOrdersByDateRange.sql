--@name: SelectOrdersByDateRange
--@description: Parameterized query to select orders within a date range
--@type: SELECT
--@timeout: 60
--@database: SQL Server
--@author: ETL Team
--@version: 1.0
--@tags: orders,date,range,parameterized

SELECT 
    OrderID,
    CustomerID,
    OrderDate,
    TotalAmount,
    Status,
    ShippingAddress,
    BillingAddress
FROM Orders 
WHERE OrderDate BETWEEN @StartDate AND @EndDate
ORDER BY OrderDate DESC;
