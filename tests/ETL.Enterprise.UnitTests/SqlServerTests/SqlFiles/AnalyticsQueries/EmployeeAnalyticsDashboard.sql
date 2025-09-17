--@name: EmployeeAnalyticsDashboard
--@description: Analytics query for employee dashboard data - destination analytics database
--@type: SELECT
--@timeout: 90
--@database: SQL Server
--@author: Payroll Analytics Team
--@version: 1.0
--@tags: payroll,analytics,dashboard,employee,destination
--@tenant: ANALYTICS_DATABASE

WITH EmployeeMetrics AS (
    SELECT 
        TenantID,
        ClientID,
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
        SalaryGrade,
        JobLevel,
        EmploymentStatus,
        BusinessUnit,
        Region,
        Country,
        -- Compensation Metrics
        BaseSalary,
        AnnualSalary,
        TotalAllowances,
        TotalDeductions,
        GrossSalary,
        NetSalary,
        -- Absence Metrics
        TotalAbsenceDays,
        SickLeaveDays,
        VacationDays,
        PersonalLeaveDays,
        -- Hierarchy Metrics
        HierarchyLevel,
        DirectReportsCount,
        TotalReportsCount,
        -- Security Metrics
        SecurityLevel,
        AccessLevel,
        CanViewPayroll,
        CanViewPII,
        -- Tenure Metrics
        DATEDIFF(month, StartDate, GETDATE()) as TenureMonths,
        DATEDIFF(year, StartDate, GETDATE()) as TenureYears,
        -- Performance Metrics (if available)
        PerformanceRating,
        LastPerformanceReviewDate,
        -- Multi-tenant fields
        ExtractionDate,
        BatchID
    FROM PayrollAnalyticsData
    WHERE ExtractionDate >= @StartDate
        AND ExtractionDate <= @EndDate
        AND (@TenantID IS NULL OR TenantID = @TenantID)
        AND (@ClientID IS NULL OR ClientID = @ClientID)
        AND (@DepartmentID IS NULL OR DepartmentID = @DepartmentID)
        AND (@EmployeeID IS NULL OR EmployeeID = @EmployeeID)
),
EmployeeRankings AS (
    SELECT 
        *,
        -- Salary Rankings
        ROW_NUMBER() OVER (PARTITION BY TenantID, ClientID, DepartmentID ORDER BY BaseSalary DESC) as SalaryRank,
        RANK() OVER (PARTITION BY TenantID, ClientID, DepartmentID ORDER BY BaseSalary DESC) as SalaryRankWithTies,
        DENSE_RANK() OVER (PARTITION BY TenantID, ClientID, DepartmentID ORDER BY BaseSalary DESC) as DenseSalaryRank,
        -- Absence Rankings
        ROW_NUMBER() OVER (PARTITION BY TenantID, ClientID, DepartmentID ORDER BY TotalAbsenceDays DESC) as AbsenceRank,
        RANK() OVER (PARTITION BY TenantID, ClientID, DepartmentID ORDER BY TotalAbsenceDays DESC) as AbsenceRankWithTies,
        -- Tenure Rankings
        ROW_NUMBER() OVER (PARTITION BY TenantID, ClientID, DepartmentID ORDER BY TenureMonths DESC) as TenureRank,
        RANK() OVER (PARTITION BY TenantID, ClientID, DepartmentID ORDER BY TenureMonths DESC) as TenureRankWithTies,
        -- Percentile Rankings
        PERCENT_RANK() OVER (PARTITION BY TenantID, ClientID, DepartmentID ORDER BY BaseSalary) as SalaryPercentile,
        PERCENT_RANK() OVER (PARTITION BY TenantID, ClientID, DepartmentID ORDER BY TotalAbsenceDays) as AbsencePercentile,
        PERCENT_RANK() OVER (PARTITION BY TenantID, ClientID, DepartmentID ORDER BY TenureMonths) as TenurePercentile
    FROM EmployeeMetrics
),
EmployeeComparisons AS (
    SELECT 
        *,
        -- Department Averages
        AVG(BaseSalary) OVER (PARTITION BY TenantID, ClientID, DepartmentID) as DepartmentAvgSalary,
        AVG(TotalAbsenceDays) OVER (PARTITION BY TenantID, ClientID, DepartmentID) as DepartmentAvgAbsenceDays,
        AVG(TenureMonths) OVER (PARTITION BY TenantID, ClientID, DepartmentID) as DepartmentAvgTenureMonths,
        -- Company Averages
        AVG(BaseSalary) OVER (PARTITION BY TenantID, ClientID) as CompanyAvgSalary,
        AVG(TotalAbsenceDays) OVER (PARTITION BY TenantID, ClientID) as CompanyAvgAbsenceDays,
        AVG(TenureMonths) OVER (PARTITION BY TenantID, ClientID) as CompanyAvgTenureMonths,
        -- Manager Averages
        AVG(BaseSalary) OVER (PARTITION BY TenantID, ClientID, ManagerID) as ManagerAvgSalary,
        AVG(TotalAbsenceDays) OVER (PARTITION BY TenantID, ClientID, ManagerID) as ManagerAvgAbsenceDays,
        AVG(TenureMonths) OVER (PARTITION BY TenantID, ClientID, ManagerID) as ManagerAvgTenureMonths
    FROM EmployeeRankings
)
SELECT 
    TenantID,
    ClientID,
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
    SalaryGrade,
    JobLevel,
    EmploymentStatus,
    BusinessUnit,
    Region,
    Country,
    -- Compensation Data
    BaseSalary,
    AnnualSalary,
    TotalAllowances,
    TotalDeductions,
    GrossSalary,
    NetSalary,
    -- Absence Data
    TotalAbsenceDays,
    SickLeaveDays,
    VacationDays,
    PersonalLeaveDays,
    -- Hierarchy Data
    HierarchyLevel,
    DirectReportsCount,
    TotalReportsCount,
    -- Security Data
    SecurityLevel,
    AccessLevel,
    CanViewPayroll,
    CanViewPII,
    -- Tenure Data
    TenureMonths,
    TenureYears,
    -- Performance Data
    PerformanceRating,
    LastPerformanceReviewDate,
    -- Rankings
    SalaryRank,
    SalaryRankWithTies,
    DenseSalaryRank,
    AbsenceRank,
    AbsenceRankWithTies,
    TenureRank,
    TenureRankWithTies,
    -- Percentiles
    SalaryPercentile,
    AbsencePercentile,
    TenurePercentile,
    -- Comparisons
    DepartmentAvgSalary,
    DepartmentAvgAbsenceDays,
    DepartmentAvgTenureMonths,
    CompanyAvgSalary,
    CompanyAvgAbsenceDays,
    CompanyAvgTenureMonths,
    ManagerAvgSalary,
    ManagerAvgAbsenceDays,
    ManagerAvgTenureMonths,
    -- Calculated Comparisons
    CASE 
        WHEN DepartmentAvgSalary > 0 THEN ((BaseSalary - DepartmentAvgSalary) * 100.0 / DepartmentAvgSalary)
        ELSE 0 
    END as SalaryVsDepartmentPercent,
    CASE 
        WHEN CompanyAvgSalary > 0 THEN ((BaseSalary - CompanyAvgSalary) * 100.0 / CompanyAvgSalary)
        ELSE 0 
    END as SalaryVsCompanyPercent,
    CASE 
        WHEN ManagerAvgSalary > 0 THEN ((BaseSalary - ManagerAvgSalary) * 100.0 / ManagerAvgSalary)
        ELSE 0 
    END as SalaryVsManagerPercent,
    -- Multi-tenant fields
    ExtractionDate,
    BatchID
FROM EmployeeComparisons
ORDER BY TenantID, ClientID, DepartmentID, SalaryRank;
