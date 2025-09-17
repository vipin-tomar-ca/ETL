--@name: UpdateCustomerOrderStatus
--@description: SQL Server stored procedure to update order status with validation and audit trail
--@type: STORED_PROCEDURE
--@timeout: 30
--@database: SQL Server
--@author: ETL Team
--@version: 1.0
--@tags: orders,update,stored-procedure,sqlserver

CREATE OR ALTER PROCEDURE [dbo].[UpdateCustomerOrderStatus]
    @OrderID INT,
    @NewStatus NVARCHAR(50),
    @UpdatedBy NVARCHAR(100),
    @Notes NVARCHAR(MAX) = NULL,
    @Success BIT OUTPUT,
    @ErrorMessage NVARCHAR(MAX) OUTPUT
AS
BEGIN
    SET NOCOUNT ON;
    
    -- Initialize output parameters
    SET @Success = 0;
    SET @ErrorMessage = '';
    
    BEGIN TRY
        BEGIN TRANSACTION;
        
        -- Validate order exists
        IF NOT EXISTS (SELECT 1 FROM Orders WHERE OrderID = @OrderID)
        BEGIN
            SET @ErrorMessage = 'Order not found';
            ROLLBACK TRANSACTION;
            RETURN;
        END
        
        -- Validate status transition
        DECLARE @CurrentStatus NVARCHAR(50);
        SELECT @CurrentStatus = Status FROM Orders WHERE OrderID = @OrderID;
        
        -- Define valid status transitions
        IF @CurrentStatus = 'Cancelled' AND @NewStatus != 'Cancelled'
        BEGIN
            SET @ErrorMessage = 'Cannot change status of cancelled order';
            ROLLBACK TRANSACTION;
            RETURN;
        END
        
        IF @CurrentStatus = 'Completed' AND @NewStatus NOT IN ('Completed', 'Cancelled')
        BEGIN
            SET @ErrorMessage = 'Cannot change status of completed order';
            ROLLBACK TRANSACTION;
            RETURN;
        END
        
        -- Update order status
        UPDATE Orders 
        SET 
            Status = @NewStatus,
            ModifiedDate = GETDATE(),
            ModifiedBy = @UpdatedBy
        WHERE OrderID = @OrderID;
        
        -- Insert audit record
        INSERT INTO OrderStatusHistory (
            OrderID,
            PreviousStatus,
            NewStatus,
            ChangedBy,
            ChangedDate,
            Notes
        )
        VALUES (
            @OrderID,
            @CurrentStatus,
            @NewStatus,
            @UpdatedBy,
            GETDATE(),
            @Notes
        );
        
        -- Update related records based on status
        IF @NewStatus = 'Shipped'
        BEGIN
            UPDATE Orders 
            SET ShippedDate = GETDATE()
            WHERE OrderID = @OrderID AND ShippedDate IS NULL;
        END
        
        IF @NewStatus = 'Completed'
        BEGIN
            UPDATE Orders 
            SET CompletedDate = GETDATE()
            WHERE OrderID = @OrderID AND CompletedDate IS NULL;
        END
        
        COMMIT TRANSACTION;
        SET @Success = 1;
        
    END TRY
    BEGIN CATCH
        IF @@TRANCOUNT > 0
            ROLLBACK TRANSACTION;
            
        SET @ErrorMessage = ERROR_MESSAGE();
        SET @Success = 0;
    END CATCH
END
