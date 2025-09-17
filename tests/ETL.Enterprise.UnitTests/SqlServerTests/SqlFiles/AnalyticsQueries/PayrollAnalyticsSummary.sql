--@name: PayrollAnalyticsSummary
--@description: Analytics query for payroll summary data - destination analytics database
--@type: SELECT
--@timeout: 120
--@database: SQL Server
--@author: Payroll Analytics Team
--@version: 1.0
--@tags: payroll,analytics,summary,aggregation,destination
--@tenant: ANALYTICS_DATABASE

WITH PayrollSummary AS (
    SELECT 
        TenantID,
        ClientID,
        DepartmentID,
        DepartmentName,
        BusinessUnit,
        Region,
        Country,
        -- Employee Counts
        COUNT(DISTINCT EmployeeID) as TotalEmployees,
        COUNT(DISTINCT CASE WHEN IsActive = 1 THEN EmployeeID END) as ActiveEmployees,
        COUNT(DISTINCT CASE WHEN IsActive = 0 THEN EmployeeID END) as InactiveEmployees,
        -- Compensation Summary
        SUM(BaseSalary) as TotalBaseSalary,
        AVG(BaseSalary) as AverageBaseSalary,
        MIN(BaseSalary) as MinimumBaseSalary,
        MAX(BaseSalary) as MaximumBaseSalary,
        SUM(TotalAllowances) as TotalAllowances,
        SUM(TotalDeductions) as TotalDeductions,
        SUM(GrossSalary) as TotalGrossSalary,
        SUM(NetSalary) as TotalNetSalary,
        -- Absence Summary
        SUM(TotalDays) as TotalAbsenceDays,
        AVG(TotalDays) as AverageAbsenceDays,
        COUNT(DISTINCT CASE WHEN AbsenceType = 'Sick Leave' THEN EmployeeID END) as SickLeaveEmployees,
        COUNT(DISTINCT CASE WHEN AbsenceType = 'Vacation' THEN EmployeeID END) as VacationEmployees,
        COUNT(DISTINCT CASE WHEN AbsenceType = 'Personal Leave' THEN EmployeeID END) as PersonalLeaveEmployees,
        -- Hierarchy Summary
        AVG(HierarchyLevel) as AverageHierarchyLevel,
        MAX(HierarchyLevel) as MaximumHierarchyLevel,
        COUNT(DISTINCT CASE WHEN DirectReportsCount > 0 THEN EmployeeID END) as ManagersCount,
        SUM(DirectReportsCount) as TotalDirectReports,
        -- Security Summary
        COUNT(DISTINCT CASE WHEN CanViewPayroll = 1 THEN EmployeeID END) as PayrollAccessEmployees,
        COUNT(DISTINCT CASE WHEN CanViewPII = 1 THEN EmployeeID END) as PIIAccessEmployees,
        COUNT(DISTINCT CASE WHEN SecurityLevel = 'High' THEN EmployeeID END) as HighSecurityEmployees,
        COUNT(DISTINCT CASE WHEN SecurityLevel = 'Medium' THEN EmployeeID END) as MediumSecurityEmployees,
        COUNT(DISTINCT CASE WHEN SecurityLevel = 'Low' THEN EmployeeID END) as LowSecurityEmployees
    FROM PayrollAnalyticsData
    WHERE ExtractionDate >= @StartDate
        AND ExtractionDate <= @EndDate
        AND (@TenantID IS NULL OR TenantID = @TenantID)
        AND (@ClientID IS NULL OR ClientID = @ClientID)
        AND (@DepartmentID IS NULL OR DepartmentID = @DepartmentID)
        AND (@BusinessUnit IS NULL OR BusinessUnit = @BusinessUnit)
        AND (@Region IS NULL OR Region = @Region)
    GROUP BY 
        TenantID,
        ClientID,
        DepartmentID,
        DepartmentName,
        BusinessUnit,
        Region,
        Country
),
PayrollTrends AS (
    SELECT 
        TenantID,
        ClientID,
        DepartmentID,
        DepartmentName,
        BusinessUnit,
        Region,
        Country,
        -- Month-over-Month Trends
        LAG(TotalEmployees, 1) OVER (PARTITION BY TenantID, ClientID, DepartmentID ORDER BY ExtractionDate) as PreviousMonthEmployees,
        LAG(TotalGrossSalary, 1) OVER (PARTITION BY TenantID, ClientID, DepartmentID ORDER BY ExtractionDate) as PreviousMonthGrossSalary,
        LAG(TotalAbsenceDays, 1) OVER (PARTITION BY TenantID, ClientID, DepartmentID ORDER BY ExtractionDate) as PreviousMonthAbsenceDays,
        -- Year-over-Year Trends
        LAG(TotalEmployees, 12) OVER (PARTITION BY TenantID, ClientID, DepartmentID ORDER BY ExtractionDate) as PreviousYearEmployees,
        LAG(TotalGrossSalary, 12) OVER (PARTITION BY TenantID, ClientID, DepartmentID ORDER BY ExtractionDate) as PreviousYearGrossSalary,
        LAG(TotalAbsenceDays, 12) OVER (PARTITION BY TenantID, ClientID, DepartmentID ORDER BY ExtractionDate) as PreviousYearAbsenceDays
    FROM PayrollSummary
)
SELECT 
    ps.*,
    -- Month-over-Month Changes
    CASE 
        WHEN ps.TotalEmployees > 0 AND pt.PreviousMonthEmployees > 0 
        THEN ((ps.TotalEmployees - pt.PreviousMonthEmployees) * 100.0 / pt.PreviousMonthEmployees)
        ELSE 0 
    END as EmployeeCountChangePercent,
    CASE 
        WHEN ps.TotalGrossSalary > 0 AND pt.PreviousMonthGrossSalary > 0 
        THEN ((ps.TotalGrossSalary - pt.PreviousMonthGrossSalary) * 100.0 / pt.PreviousMonthGrossSalary)
        ELSE 0 
    END as GrossSalaryChangePercent,
    CASE 
        WHEN ps.TotalAbsenceDays > 0 AND pt.PreviousMonthAbsenceDays > 0 
        THEN ((ps.TotalAbsenceDays - pt.PreviousMonthAbsenceDays) * 100.0 / pt.PreviousMonthAbsenceDays)
        ELSE 0 
    END as AbsenceDaysChangePercent,
    -- Year-over-Year Changes
    CASE 
        WHEN ps.TotalEmployees > 0 AND pt.PreviousYearEmployees > 0 
        THEN ((ps.TotalEmployees - pt.PreviousYearEmployees) * 100.0 / pt.PreviousYearEmployees)
        ELSE 0 
    END as EmployeeCountYoYChangePercent,
    CASE 
        WHEN ps.TotalGrossSalary > 0 AND pt.PreviousYearGrossSalary > 0 
        THEN ((ps.TotalGrossSalary - pt.PreviousYearGrossSalary) * 100.0 / pt.PreviousYearGrossSalary)
        ELSE 0 
    END as GrossSalaryYoYChangePercent,
    CASE 
        WHEN ps.TotalAbsenceDays > 0 AND pt.PreviousYearAbsenceDays > 0 
        THEN ((ps.TotalAbsenceDays - pt.PreviousYearAbsenceDays) * 100.0 / pt.PreviousYearAbsenceDays)
        ELSE 0 
    END as AbsenceDaysYoYChangePercent,
    -- Calculated Fields
    CASE 
        WHEN ps.TotalEmployees > 0 THEN (ps.TotalGrossSalary / ps.TotalEmployees)
        ELSE 0 
    END as CostPerEmployee,
    CASE 
        WHEN ps.TotalEmployees > 0 THEN (ps.TotalAbsenceDays / ps.TotalEmployees)
        ELSE 0 
    END as AbsenceDaysPerEmployee,
    CASE 
        WHEN ps.TotalGrossSalary > 0 THEN (ps.TotalDeductions * 100.0 / ps.TotalGrossSalary)
        ELSE 0 
    END as DeductionPercentage,
    CASE 
        WHEN ps.TotalGrossSalary > 0 THEN (ps.TotalAllowances * 100.0 / ps.TotalGrossSalary)
        ELSE 0 
    END as AllowancePercentage,
    -- Multi-tenant fields
    @ExtractionDate as ExtractionDate,
    @BatchID as BatchID
FROM PayrollSummary ps
LEFT JOIN PayrollTrends pt ON ps.TenantID = pt.TenantID 
    AND ps.ClientID = pt.ClientID 
    AND ps.DepartmentID = pt.DepartmentID
ORDER BY ps.TenantID, ps.ClientID, ps.DepartmentID;
