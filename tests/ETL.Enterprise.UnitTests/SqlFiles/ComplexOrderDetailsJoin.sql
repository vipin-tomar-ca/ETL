--@name: ComplexOrderDetailsJoin
--@description: Complex JOIN query to retrieve order details with customer and product information
--@type: SELECT
--@timeout: 120
--@database: SQL Server
--@author: ETL Team
--@version: 1.0
--@tags: complex,join,orders,customers,products

SELECT 
    c.CustomerID,
    c.CustomerName,
    c.Email,
    o.OrderID,
    o.OrderDate,
    o.TotalAmount,
    o.Status,
    p.ProductID,
    p.ProductName,
    p.Price,
    od.Quantity,
    od.UnitPrice,
    (od.Quantity * od.UnitPrice) as LineTotal
FROM Customers c
INNER JOIN Orders o ON c.CustomerID = o.CustomerID
INNER JOIN OrderDetails od ON o.OrderID = od.OrderID
INNER JOIN Products p ON od.ProductID = p.ProductID
WHERE o.OrderDate >= @StartDate
  AND o.Status = 'Completed'
ORDER BY o.OrderDate DESC, c.CustomerName;
