--@name: CalculateOrderTotal
--@description: SQL Server scalar function to calculate order total with tax and shipping
--@type: FUNCTION
--@timeout: 30
--@database: SQL Server
--@author: ETL Team
--@version: 1.0
--@tags: orders,calculation,function,sqlserver

CREATE OR ALTER FUNCTION [dbo].[CalculateOrderTotal](
    @OrderID INT,
    @TaxRate DECIMAL(5,4) = 0.08,
    @ShippingCost DECIMAL(10,2) = 0.00
)
RETURNS DECIMAL(10,2)
AS
BEGIN
    DECLARE @Subtotal DECIMAL(10,2) = 0;
    DECLARE @TaxAmount DECIMAL(10,2) = 0;
    DECLARE @Total DECIMAL(10,2) = 0;
    
    -- Calculate subtotal from order details
    SELECT @Subtotal = ISNULL(SUM(Quantity * UnitPrice), 0)
    FROM OrderDetails
    WHERE OrderID = @OrderID;
    
    -- Calculate tax amount
    SET @TaxAmount = @Subtotal * @TaxRate;
    
    -- Calculate total
    SET @Total = @Subtotal + @TaxAmount + @ShippingCost;
    
    RETURN @Total;
END
