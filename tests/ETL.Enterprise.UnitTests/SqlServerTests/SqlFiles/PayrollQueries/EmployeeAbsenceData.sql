--@name: EmployeeAbsenceData
--@description: Extract employee absence data for payroll analytics - multi-tenant support
--@type: SELECT
--@timeout: 90
--@database: SQL Server
--@author: Payroll ETL Team
--@version: 1.0
--@tags: payroll,employee,absence,leave,attendance,multitenant,extraction
--@tenant: CLIENT_DATABASE

SELECT 
    a.AbsenceID,
    a.EmployeeID,
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
    -- Absence Details
    a.AbsenceType,
    a.AbsenceCategory,
    a.AbsenceReason,
    a.StartDate,
    a.EndDate,
    a.StartTime,
    a.EndTime,
    a.TotalDays,
    a.TotalHours,
    a.IsFullDay,
    a.IsHalfDay,
    a.IsApproved,
    a.ApprovedBy,
    a.ApprovedDate,
    a.ApprovalNotes,
    a.IsPaid,
    a.PayRate,
    a.DeductionAmount,
    a.CompensationAmount,
    -- Leave Balance
    a.LeaveBalanceBefore,
    a.LeaveBalanceAfter,
    a.LeaveBalanceUsed,
    -- Additional Information
    a.DocumentationRequired,
    a.DocumentationProvided,
    a.DocumentationNotes,
    a.MedicalCertificate,
    a.EmergencyContact,
    a.EmergencyPhone,
    a.ReturnToWorkDate,
    a.ReturnToWorkNotes,
    a.CreatedDate,
    a.CreatedBy,
    a.ModifiedDate,
    a.ModifiedBy,
    -- Multi-tenant fields
    @TenantID as TenantID,
    @ClientID as ClientID,
    @ExtractionDate as ExtractionDate,
    @BatchID as BatchID
FROM EmployeeAbsences a
INNER JOIN Employees e ON a.EmployeeID = e.EmployeeID
LEFT JOIN Departments d ON e.DepartmentID = d.DepartmentID
LEFT JOIN Positions p ON e.PositionID = p.PositionID
LEFT JOIN Employees m ON e.ManagerID = m.EmployeeID
WHERE a.StartDate >= @StartDate
    AND a.StartDate <= @EndDate
    AND e.IsActive = 1
    AND (@DepartmentID IS NULL OR e.DepartmentID = @DepartmentID)
    AND (@AbsenceType IS NULL OR a.AbsenceType = @AbsenceType)
    AND (@AbsenceCategory IS NULL OR a.AbsenceCategory = @AbsenceCategory)
    AND (@IsApproved IS NULL OR a.IsApproved = @IsApproved)
    AND (@IsPaid IS NULL OR a.IsPaid = @IsPaid)
ORDER BY a.StartDate DESC, e.EmployeeID;
