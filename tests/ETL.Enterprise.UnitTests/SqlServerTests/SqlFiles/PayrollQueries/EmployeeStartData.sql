--@name: EmployeeStartData
--@description: Extract employee start data for payroll analytics - multi-tenant support
--@type: SELECT
--@timeout: 60
--@database: SQL Server
--@author: Payroll ETL Team
--@version: 1.0
--@tags: payroll,employee,start,multitenant,extraction
--@tenant: CLIENT_DATABASE

SELECT 
    e.EmployeeID,
    e.EmployeeNumber,
    e.FirstName,
    e.LastName,
    e.MiddleName,
    e.Email,
    e.Phone,
    e.DateOfBirth,
    e.Gender,
    e.MaritalStatus,
    e.Nationality,
    e.PassportNumber,
    e.SocialSecurityNumber,
    e.TaxID,
    e.StartDate,
    e.ProbationEndDate,
    e.ContractType,
    e.EmploymentType,
    e.WorkLocation,
    e.DepartmentID,
    d.DepartmentName,
    e.PositionID,
    p.PositionTitle,
    e.ManagerID,
    m.FirstName + ' ' + m.LastName as ManagerName,
    e.SalaryGrade,
    e.JobLevel,
    e.EmploymentStatus,
    e.HireSource,
    e.ReferralEmployeeID,
    e.RecruitmentAgency,
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
WHERE e.StartDate >= @StartDate
    AND e.StartDate <= @EndDate
    AND e.IsActive = 1
    AND (@DepartmentID IS NULL OR e.DepartmentID = @DepartmentID)
    AND (@PositionID IS NULL OR e.PositionID = @PositionID)
    AND (@EmploymentType IS NULL OR e.EmploymentType = @EmploymentType)
ORDER BY e.StartDate DESC, e.EmployeeID;
