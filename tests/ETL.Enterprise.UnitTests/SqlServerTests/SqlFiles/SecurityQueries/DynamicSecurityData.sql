--@name: DynamicSecurityData
--@description: Extract dynamic security data for payroll analytics - multi-tenant support
--@type: SELECT
--@timeout: 60
--@database: SQL Server
--@author: Payroll ETL Team
--@version: 1.0
--@tags: payroll,security,dynamic,access,multitenant,extraction
--@tenant: CLIENT_DATABASE
--@security: ENCRYPTED

SELECT 
    s.SecurityID,
    s.EmployeeID,
    e.EmployeeNumber,
    e.FirstName,
    e.LastName,
    e.Email,
    e.DepartmentID,
    d.DepartmentName,
    e.PositionID,
    p.PositionTitle,
    e.ManagerID,
    m.FirstName + ' ' + m.LastName as ManagerName,
    -- Security Details
    s.SecurityLevel,
    s.AccessLevel,
    s.PermissionSet,
    s.RoleID,
    r.RoleName,
    r.RoleDescription,
    s.SecurityGroupID,
    sg.SecurityGroupName,
    sg.SecurityGroupDescription,
    -- Access Control
    s.CanViewSalary,
    s.CanViewPII,
    s.CanViewFinancial,
    s.CanViewHR,
    s.CanViewPayroll,
    s.CanViewReports,
    s.CanExportData,
    s.CanModifyData,
    s.CanDeleteData,
    s.CanApprove,
    s.CanReject,
    -- Data Access Scope
    s.DepartmentAccess,
    s.EmployeeAccess,
    s.DataAccessScope,
    s.GeographicAccess,
    s.TimeBasedAccess,
    s.IPRestrictions,
    s.DeviceRestrictions,
    -- Security Status
    s.IsActive,
    s.IsSuspended,
    s.SuspensionReason,
    s.SuspensionDate,
    s.SuspensionEndDate,
    s.LastAccessDate,
    s.LastAccessIP,
    s.LastAccessDevice,
    s.FailedLoginAttempts,
    s.LastFailedLogin,
    s.PasswordExpiryDate,
    s.TwoFactorEnabled,
    s.SSOEnabled,
    -- Audit Information
    s.CreatedDate,
    s.CreatedBy,
    s.ModifiedDate,
    s.ModifiedBy,
    s.ApprovedBy,
    s.ApprovedDate,
    s.ReviewDate,
    s.NextReviewDate,
    -- Multi-tenant fields
    @TenantID as TenantID,
    @ClientID as ClientID,
    @ExtractionDate as ExtractionDate,
    @BatchID as BatchID
FROM EmployeeSecurity s
INNER JOIN Employees e ON s.EmployeeID = e.EmployeeID
LEFT JOIN Departments d ON e.DepartmentID = d.DepartmentID
LEFT JOIN Positions p ON e.PositionID = p.PositionID
LEFT JOIN Employees m ON e.ManagerID = m.EmployeeID
LEFT JOIN SecurityRoles r ON s.RoleID = r.RoleID
LEFT JOIN SecurityGroups sg ON s.SecurityGroupID = sg.SecurityGroupID
WHERE s.IsActive = 1
    AND e.IsActive = 1
    AND (@DepartmentID IS NULL OR e.DepartmentID = @DepartmentID)
    AND (@RoleID IS NULL OR s.RoleID = @RoleID)
    AND (@SecurityLevel IS NULL OR s.SecurityLevel = @SecurityLevel)
    AND (@AccessLevel IS NULL OR s.AccessLevel = @AccessLevel)
    AND (@CanViewPayroll IS NULL OR s.CanViewPayroll = @CanViewPayroll)
ORDER BY e.DepartmentID, e.EmployeeID;
