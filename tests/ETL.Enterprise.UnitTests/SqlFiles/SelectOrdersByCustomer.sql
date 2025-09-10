--@name: SelectOrdersByCustomer
--@description: Select orders for a specific customer with status filter
--@type: SELECT
--@timeout: 45
--@database: SQL Server
--@author: ETL Team
--@version: 1.0
--@tags: orders,customer,status,parameterized

SELECT 
    o.OrderID,
    o.CustomerID,
    c.CustomerName,
    o.OrderDate,
    o.TotalAmount,
    o.Status,
    o.ShippingAddress
FROM Orders o
INNER JOIN Customers c ON o.CustomerID = c.CustomerID
WHERE o.CustomerID = @CustomerID
  AND (@OrderStatus IS NULL OR o.Status = @OrderStatus)
ORDER BY o.OrderDate DESC;
