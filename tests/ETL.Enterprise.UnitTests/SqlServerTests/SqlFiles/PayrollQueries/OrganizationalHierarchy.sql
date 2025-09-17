--@name: OrganizationalHierarchy
--@description: Extract organizational hierarchy data for payroll analytics - multi-tenant support
--@type: SELECT
--@timeout: 90
--@database: SQL Server
--@author: Payroll ETL Team
--@version: 1.0
--@tags: payroll,organizational,hierarchy,structure,multitenant,extraction
--@tenant: CLIENT_DATABASE

WITH OrganizationalHierarchy AS (
    -- Get all employees with their hierarchy levels
    SELECT 
        e.EmployeeID,
        e.EmployeeNumber,
        e.FirstName,
        e.LastName,
        e.Email,
        e.DepartmentID,
        d.DepartmentName,
        e.PositionID,
        p.PositionTitle,
        e.ManagerID,
        e.SalaryGrade,
        e.JobLevel,
        e.EmploymentStatus,
        e.CostCenter,
        e.BusinessUnit,
        e.Region,
        e.Country,
        e.StartDate,
        e.EndDate,
        e.IsActive,
        -- Hierarchy Level
        0 as HierarchyLevel,
        CAST(e.EmployeeID AS VARCHAR(MAX)) as HierarchyPath,
        e.EmployeeID as RootManagerID,
        e.FirstName + ' ' + e.LastName as RootManagerName
    FROM Employees e
    LEFT JOIN Departments d ON e.DepartmentID = d.DepartmentID
    LEFT JOIN Positions p ON e.PositionID = p.PositionID
    WHERE e.ManagerID IS NULL
        AND e.IsActive = 1
    
    UNION ALL
    
    SELECT 
        e.EmployeeID,
        e.EmployeeNumber,
        e.FirstName,
        e.LastName,
        e.Email,
        e.DepartmentID,
        d.DepartmentName,
        e.PositionID,
        p.PositionTitle,
        e.ManagerID,
        e.SalaryGrade,
        e.JobLevel,
        e.EmploymentStatus,
        e.CostCenter,
        e.BusinessUnit,
        e.Region,
        e.Country,
        e.StartDate,
        e.EndDate,
        e.IsActive,
        -- Hierarchy Level
        oh.HierarchyLevel + 1,
        oh.HierarchyPath + ' -> ' + CAST(e.EmployeeID AS VARCHAR(MAX)),
        oh.RootManagerID,
        oh.RootManagerName
    FROM Employees e
    INNER JOIN OrganizationalHierarchy oh ON e.ManagerID = oh.EmployeeID
    LEFT JOIN Departments d ON e.DepartmentID = d.DepartmentID
    LEFT JOIN Positions p ON e.PositionID = p.PositionID
    WHERE e.IsActive = 1
        AND oh.HierarchyLevel < 10 -- Prevent infinite recursion
),
EmployeeHierarchy AS (
    SELECT 
        oh.*,
        m.FirstName + ' ' + m.LastName as ManagerName,
        m.Email as ManagerEmail,
        m.PositionTitle as ManagerPosition,
        -- Count direct reports
        (SELECT COUNT(*) FROM Employees e2 WHERE e2.ManagerID = oh.EmployeeID AND e2.IsActive = 1) as DirectReportsCount,
        -- Count total reports (including indirect)
        (SELECT COUNT(*) FROM OrganizationalHierarchy oh2 WHERE oh2.HierarchyPath LIKE oh.HierarchyPath + '%' AND oh2.EmployeeID != oh.EmployeeID) as TotalReportsCount
    FROM OrganizationalHierarchy oh
    LEFT JOIN Employees m ON oh.ManagerID = m.EmployeeID
)
SELECT 
    EmployeeID,
    EmployeeNumber,
    FirstName,
    LastName,
    Email,
    DepartmentID,
    DepartmentName,
    PositionID,
    PositionTitle,
    ManagerID,
    ManagerName,
    ManagerEmail,
    ManagerPosition,
    SalaryGrade,
    JobLevel,
    EmploymentStatus,
    CostCenter,
    BusinessUnit,
    Region,
    Country,
    StartDate,
    EndDate,
    IsActive,
    HierarchyLevel,
    HierarchyPath,
    RootManagerID,
    RootManagerName,
    DirectReportsCount,
    TotalReportsCount,
    -- Multi-tenant fields
    @TenantID as TenantID,
    @ClientID as ClientID,
    @ExtractionDate as ExtractionDate,
    @BatchID as BatchID
FROM EmployeeHierarchy
WHERE (@DepartmentID IS NULL OR DepartmentID = @DepartmentID)
    AND (@ManagerID IS NULL OR ManagerID = @ManagerID)
    AND (@HierarchyLevel IS NULL OR HierarchyLevel = @HierarchyLevel)
    AND (@JobLevel IS NULL OR JobLevel = @JobLevel)
ORDER BY HierarchyLevel, DepartmentName, LastName, FirstName;
