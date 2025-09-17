--@name: GetCustomerOrders
--@description: SQL Server stored procedure to retrieve customer orders with optional filtering
--@type: STORED_PROCEDURE
--@timeout: 30
--@database: SQL Server
--@author: ETL Team
--@version: 1.0
--@tags: customer,orders,stored-procedure,sqlserver

CREATE OR ALTER PROCEDURE [dbo].[GetCustomerOrders]
    @CustomerID INT = NULL,
    @StartDate DATETIME2 = NULL,
    @EndDate DATETIME2 = NULL,
    @OrderStatus NVARCHAR(50) = NULL,
    @PageNumber INT = 1,
    @PageSize INT = 50,
    @TotalRecords INT OUTPUT
AS
BEGIN
    SET NOCOUNT ON;
    
    -- Validate input parameters
    IF @PageNumber < 1 SET @PageNumber = 1;
    IF @PageSize < 1 OR @PageSize > 1000 SET @PageSize = 50;
    
    -- Set default date range if not provided
    IF @StartDate IS NULL SET @StartDate = DATEADD(month, -12, GETDATE());
    IF @EndDate IS NULL SET @EndDate = GETDATE();
    
    -- Get total count for pagination
    SELECT @TotalRecords = COUNT(*)
    FROM Orders o
    INNER JOIN Customers c ON o.CustomerID = c.CustomerID
    WHERE (@CustomerID IS NULL OR o.CustomerID = @CustomerID)
        AND o.OrderDate >= @StartDate
        AND o.OrderDate <= @EndDate
        AND (@OrderStatus IS NULL OR o.Status = @OrderStatus)
        AND c.IsActive = 1;
    
    -- Get paginated results
    SELECT 
        o.OrderID,
        o.CustomerID,
        c.CustomerName,
        c.Email,
        o.OrderDate,
        o.RequiredDate,
        o.ShippedDate,
        o.Status,
        o.TotalAmount,
        o.ShippingAddress,
        o.BillingAddress,
        o.Notes,
        od.ItemCount,
        od.TotalItems
    FROM (
        SELECT 
            o.OrderID,
            o.CustomerID,
            o.OrderDate,
            o.RequiredDate,
            o.ShippedDate,
            o.Status,
            o.TotalAmount,
            o.ShippingAddress,
            o.BillingAddress,
            o.Notes,
            ROW_NUMBER() OVER (ORDER BY o.OrderDate DESC) as RowNum
        FROM Orders o
        INNER JOIN Customers c ON o.CustomerID = c.CustomerID
        WHERE (@CustomerID IS NULL OR o.CustomerID = @CustomerID)
            AND o.OrderDate >= @StartDate
            AND o.OrderDate <= @EndDate
            AND (@OrderStatus IS NULL OR o.Status = @OrderStatus)
            AND c.IsActive = 1
    ) o
    LEFT JOIN (
        SELECT 
            OrderID,
            COUNT(*) as ItemCount,
            SUM(Quantity) as TotalItems
        FROM OrderDetails
        GROUP BY OrderID
    ) od ON o.OrderID = od.OrderID
    INNER JOIN Customers c ON o.CustomerID = c.CustomerID
    WHERE o.RowNum BETWEEN ((@PageNumber - 1) * @PageSize + 1) 
        AND (@PageNumber * @PageSize)
    ORDER BY o.OrderDate DESC;
    
    RETURN 0;
END
