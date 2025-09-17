--@name: EmployeeCompensationData
--@description: Extract employee compensation data for payroll analytics - multi-tenant support
--@type: SELECT
--@timeout: 90
--@database: SQL Server
--@author: Payroll ETL Team
--@version: 1.0
--@tags: payroll,employee,compensation,salary,multitenant,extraction
--@tenant: CLIENT_DATABASE

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
    e.SalaryGrade,
    e.JobLevel,
    -- Basic Compensation
    c.BaseSalary,
    c.AnnualSalary,
    c.HourlyRate,
    c.Currency,
    c.SalaryFrequency,
    c.EffectiveDate,
    c.EndDate,
    -- Allowances
    c.HousingAllowance,
    c.TransportAllowance,
    c.MealAllowance,
    c.MedicalAllowance,
    c.CommunicationAllowance,
    c.OtherAllowances,
    c.TotalAllowances,
    -- Benefits
    c.HealthInsurance,
    c.LifeInsurance,
    c.RetirementContribution,
    c.ProvidentFund,
    c.Gratuity,
    c.Bonus,
    c.Commission,
    c.OvertimeRate,
    c.ShiftAllowance,
    c.NightShiftAllowance,
    c.WeekendAllowance,
    c.HolidayAllowance,
    -- Deductions
    c.IncomeTax,
    c.SocialSecurity,
    c.HealthInsuranceDeduction,
    c.LoanDeduction,
    c.AdvanceDeduction,
    c.OtherDeductions,
    c.TotalDeductions,
    -- Net Pay
    c.GrossSalary,
    c.NetSalary,
    c.TakeHomePay,
    -- Additional Information
    c.PayrollGroup,
    c.PayrollCategory,
    c.CostCenter,
    c.BusinessUnit,
    c.Region,
    c.Country,
    c.CreatedDate,
    c.CreatedBy,
    c.ModifiedDate,
    c.ModifiedBy,
    -- Multi-tenant fields
    @TenantID as TenantID,
    @ClientID as ClientID,
    @ExtractionDate as ExtractionDate,
    @BatchID as BatchID
FROM Employees e
INNER JOIN EmployeeCompensation c ON e.EmployeeID = c.EmployeeID
LEFT JOIN Departments d ON e.DepartmentID = d.DepartmentID
LEFT JOIN Positions p ON e.PositionID = p.PositionID
WHERE c.EffectiveDate <= @ExtractionDate
    AND (c.EndDate IS NULL OR c.EndDate >= @ExtractionDate)
    AND e.IsActive = 1
    AND (@DepartmentID IS NULL OR e.DepartmentID = @DepartmentID)
    AND (@SalaryGrade IS NULL OR e.SalaryGrade = @SalaryGrade)
    AND (@PayrollGroup IS NULL OR c.PayrollGroup = @PayrollGroup)
    AND (@CostCenter IS NULL OR c.CostCenter = @CostCenter)
ORDER BY e.EmployeeID, c.EffectiveDate DESC;
