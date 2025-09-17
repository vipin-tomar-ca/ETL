--@name: EmployeeExitData
--@description: Extract employee exit/termination data for payroll analytics - multi-tenant support
--@type: SELECT
--@timeout: 60
--@database: SQL Server
--@author: Payroll ETL Team
--@version: 1.0
--@tags: payroll,employee,exit,termination,multitenant,extraction
--@tenant: CLIENT_DATABASE

SELECT 
    e.EmployeeID,
    e.EmployeeNumber,
    e.FirstName,
    e.LastName,
    e.MiddleName,
    e.Email,
    e.Phone,
    e.StartDate,
    e.EndDate,
    e.TerminationDate,
    e.TerminationReason,
    e.TerminationType,
    e.TerminationCategory,
    e.NoticePeriod,
    e.NoticePeriodUnit,
    e.LastWorkingDate,
    e.ExitInterviewDate,
    e.ExitInterviewConducted,
    e.ExitInterviewNotes,
    e.FinalSettlementDate,
    e.FinalSettlementAmount,
    e.OutstandingAmount,
    e.DepartmentID,
    d.DepartmentName,
    e.PositionID,
    p.PositionTitle,
    e.ManagerID,
    m.FirstName + ' ' + m.LastName as ManagerName,
    e.SalaryGrade,
    e.JobLevel,
    e.EmploymentStatus,
    e.CostCenter,
    e.BusinessUnit,
    e.Region,
    e.Country,
    e.CreatedDate,
    e.CreatedBy,
    e.ModifiedDate,
    e.ModifiedBy,
    -- Multi-tenant fields
    @TenantID as TenantID,
    @ClientID as ClientID,
    @ExtractionDate as ExtractionDate,
    @BatchID as BatchID
FROM Employees e
LEFT JOIN Departments d ON e.DepartmentID = d.DepartmentID
LEFT JOIN Positions p ON e.PositionID = p.PositionID
LEFT JOIN Employees m ON e.ManagerID = m.EmployeeID
WHERE e.EndDate >= @StartDate
    AND e.EndDate <= @EndDate
    AND e.IsActive = 0
    AND (@DepartmentID IS NULL OR e.DepartmentID = @DepartmentID)
    AND (@TerminationReason IS NULL OR e.TerminationReason = @TerminationReason)
    AND (@TerminationType IS NULL OR e.TerminationType = @TerminationType)
ORDER BY e.EndDate DESC, e.EmployeeID;
