--@name: GenerateComprehensiveTestData
--@description: Generate comprehensive test data for payroll testing covering various scenarios
--@type: DML
--@timeout: 600
--@database: SQL Server
--@author: Payroll ETL Team
--@version: 1.0
--@tags: testdata,generation,payroll,comprehensive

USE PayrollTestDB;
GO

-- Clear existing data
DELETE FROM EmployeeSecurity;
DELETE FROM EmployeeAbsences;
DELETE FROM EmployeeCompensation;
DELETE FROM EmployeeBankAccounts;
DELETE FROM EmployeeAddresses;
DELETE FROM Employees;
DELETE FROM SecurityGroups;
DELETE FROM SecurityRoles;
DELETE FROM Positions;
DELETE FROM Departments;

-- Reset identity columns
DBCC CHECKIDENT ('Departments', RESEED, 0);
DBCC CHECKIDENT ('Positions', RESEED, 0);
DBCC CHECKIDENT ('Employees', RESEED, 0);
DBCC CHECKIDENT ('EmployeeAddresses', RESEED, 0);
DBCC CHECKIDENT ('EmployeeBankAccounts', RESEED, 0);
DBCC CHECKIDENT ('EmployeeCompensation', RESEED, 0);
DBCC CHECKIDENT ('EmployeeAbsences', RESEED, 0);
DBCC CHECKIDENT ('SecurityRoles', RESEED, 0);
DBCC CHECKIDENT ('SecurityGroups', RESEED, 0);
DBCC CHECKIDENT ('EmployeeSecurity', RESEED, 0);

-- Insert Departments
INSERT INTO Departments (DepartmentName, DepartmentCode, BusinessUnit, Region, Country, CostCenter)
VALUES 
    ('Engineering', 'ENG', 'Technology', 'North America', 'USA', 'CC001'),
    ('Sales', 'SAL', 'Sales', 'North America', 'USA', 'CC002'),
    ('Marketing', 'MKT', 'Marketing', 'North America', 'USA', 'CC003'),
    ('Human Resources', 'HR', 'Administration', 'North America', 'USA', 'CC004'),
    ('Finance', 'FIN', 'Finance', 'North America', 'USA', 'CC005'),
    ('Operations', 'OPS', 'Operations', 'North America', 'USA', 'CC006'),
    ('Customer Support', 'CS', 'Operations', 'North America', 'USA', 'CC007'),
    ('Quality Assurance', 'QA', 'Technology', 'North America', 'USA', 'CC008'),
    ('Product Management', 'PM', 'Product', 'North America', 'USA', 'CC009'),
    ('Legal', 'LEG', 'Administration', 'North America', 'USA', 'CC010');

-- Insert Positions
INSERT INTO Positions (PositionTitle, PositionCode, JobLevel, SalaryGrade)
VALUES 
    ('Software Engineer', 'SE', 'L3', 'SE1'),
    ('Senior Software Engineer', 'SSE', 'L4', 'SE2'),
    ('Lead Software Engineer', 'LSE', 'L5', 'SE3'),
    ('Principal Software Engineer', 'PSE', 'L6', 'SE4'),
    ('Software Architect', 'SA', 'L7', 'SE5'),
    ('Sales Representative', 'SR', 'L3', 'SR1'),
    ('Senior Sales Representative', 'SSR', 'L4', 'SR2'),
    ('Sales Manager', 'SM', 'L5', 'SR3'),
    ('Regional Sales Manager', 'RSM', 'L6', 'SR4'),
    ('Marketing Specialist', 'MS', 'L3', 'MS1'),
    ('Senior Marketing Specialist', 'SMS', 'L4', 'MS2'),
    ('Marketing Manager', 'MM', 'L5', 'MS3'),
    ('HR Specialist', 'HRS', 'L3', 'HRS1'),
    ('Senior HR Specialist', 'SHRS', 'L4', 'HRS2'),
    ('HR Manager', 'HRM', 'L5', 'HRS3'),
    ('Financial Analyst', 'FA', 'L3', 'FA1'),
    ('Senior Financial Analyst', 'SFA', 'L4', 'FA2'),
    ('Finance Manager', 'FM', 'L5', 'FA3'),
    ('Operations Specialist', 'OS', 'L3', 'OS1'),
    ('Operations Manager', 'OM', 'L5', 'OS2'),
    ('Customer Support Representative', 'CSR', 'L2', 'CSR1'),
    ('Senior Customer Support Representative', 'SCSR', 'L3', 'CSR2'),
    ('QA Engineer', 'QAE', 'L3', 'QAE1'),
    ('Senior QA Engineer', 'SQAE', 'L4', 'QAE2'),
    ('Product Manager', 'PM', 'L5', 'PM1'),
    ('Senior Product Manager', 'SPM', 'L6', 'PM2'),
    ('Legal Counsel', 'LC', 'L5', 'LC1'),
    ('Senior Legal Counsel', 'SLC', 'L6', 'LC2');

-- Insert Security Roles
INSERT INTO SecurityRoles (RoleName, RoleDescription)
VALUES 
    ('Admin', 'Full system access'),
    ('HR Manager', 'HR and payroll access'),
    ('Finance Manager', 'Financial data access'),
    ('Department Manager', 'Department-specific access'),
    ('Employee', 'Basic employee access'),
    ('Payroll Admin', 'Payroll-specific access'),
    ('Security Admin', 'Security management access');

-- Insert Security Groups
INSERT INTO SecurityGroups (SecurityGroupName, SecurityGroupDescription)
VALUES 
    ('Executive', 'Executive level access'),
    ('Management', 'Management level access'),
    ('Senior Staff', 'Senior staff access'),
    ('Staff', 'Regular staff access'),
    ('Contractor', 'Contractor access'),
    ('Intern', 'Intern access');

-- Insert Employees with comprehensive data
DECLARE @EmployeeCount INT = 1000;
DECLARE @Counter INT = 1;
DECLARE @DepartmentCount INT = (SELECT COUNT(*) FROM Departments);
DECLARE @PositionCount INT = (SELECT COUNT(*) FROM Positions);
DECLARE @RoleCount INT = (SELECT COUNT(*) FROM SecurityRoles);
DECLARE @GroupCount INT = (SELECT COUNT(*) FROM SecurityGroups);

WHILE @Counter <= @EmployeeCount
BEGIN
    DECLARE @EmployeeID INT;
    DECLARE @EmployeeNumber NVARCHAR(20) = 'EMP' + RIGHT('00000' + CAST(@Counter AS NVARCHAR(10)), 5);
    DECLARE @FirstName NVARCHAR(50) = 'FirstName' + CAST(@Counter AS NVARCHAR(10));
    DECLARE @LastName NVARCHAR(50) = 'LastName' + CAST(@Counter AS NVARCHAR(10));
    DECLARE @Email NVARCHAR(100) = 'employee' + CAST(@Counter AS NVARCHAR(10)) + '@company.com';
    DECLARE @Phone NVARCHAR(20) = '555-' + RIGHT('0000' + CAST(@Counter AS NVARCHAR(10)), 4);
    DECLARE @DateOfBirth DATE = DATEADD(DAY, -RAND() * 15000, '1990-01-01');
    DECLARE @Gender NVARCHAR(10) = CASE WHEN @Counter % 2 = 0 THEN 'Female' ELSE 'Male' END;
    DECLARE @StartDate DATE = DATEADD(DAY, -RAND() * 2000, '2020-01-01');
    DECLARE @EndDate DATE = CASE WHEN @Counter % 10 = 0 THEN DATEADD(DAY, RAND() * 1000, @StartDate) ELSE NULL END;
    DECLARE @DepartmentID INT = (@Counter % @DepartmentCount) + 1;
    DECLARE @PositionID INT = (@Counter % @PositionCount) + 1;
    DECLARE @ManagerID INT = CASE WHEN @Counter > 10 THEN (@Counter % 10) + 1 ELSE NULL END;
    DECLARE @EmploymentStatus NVARCHAR(50) = CASE WHEN @EndDate IS NULL THEN 'Active' ELSE 'Terminated' END;
    DECLARE @IsActive BIT = CASE WHEN @EndDate IS NULL THEN 1 ELSE 0 END;
    DECLARE @BusinessUnit NVARCHAR(50) = (SELECT BusinessUnit FROM Departments WHERE DepartmentID = @DepartmentID);
    DECLARE @Region NVARCHAR(50) = (SELECT Region FROM Departments WHERE DepartmentID = @DepartmentID);
    DECLARE @Country NVARCHAR(50) = (SELECT Country FROM Departments WHERE DepartmentID = @DepartmentID);
    DECLARE @SalaryGrade NVARCHAR(20) = (SELECT SalaryGrade FROM Positions WHERE PositionID = @PositionID);
    DECLARE @JobLevel NVARCHAR(20) = (SELECT JobLevel FROM Positions WHERE PositionID = @PositionID);

    INSERT INTO Employees (
        EmployeeNumber, FirstName, LastName, Email, Phone, DateOfBirth, Gender,
        StartDate, EndDate, TerminationDate, TerminationReason, TerminationType,
        DepartmentID, PositionID, ManagerID, SalaryGrade, JobLevel, EmploymentStatus,
        BusinessUnit, Region, Country, IsActive, ContractType, EmploymentType
    )
    VALUES (
        @EmployeeNumber, @FirstName, @LastName, @Email, @Phone, @DateOfBirth, @Gender,
        @StartDate, @EndDate, 
        CASE WHEN @EndDate IS NOT NULL THEN @EndDate ELSE NULL END,
        CASE WHEN @EndDate IS NOT NULL THEN 'Resignation' ELSE NULL END,
        CASE WHEN @EndDate IS NOT NULL THEN 'Voluntary' ELSE NULL END,
        @DepartmentID, @PositionID, @ManagerID, @SalaryGrade, @JobLevel, @EmploymentStatus,
        @BusinessUnit, @Region, @Country, @IsActive, 'Full-Time', 'Permanent'
    );

    SET @EmployeeID = SCOPE_IDENTITY();

    -- Insert Employee Address
    INSERT INTO EmployeeAddresses (EmployeeID, AddressType, AddressLine1, City, State, PostalCode, Country, IsPrimary)
    VALUES (
        @EmployeeID, 'Home', 
        CAST(@Counter AS NVARCHAR(10)) + ' Main Street', 
        'City' + CAST((@Counter % 50) + 1 AS NVARCHAR(10)),
        'State' + CAST((@Counter % 10) + 1 AS NVARCHAR(10)),
        CAST(10000 + (@Counter % 90000) AS NVARCHAR(10)),
        'USA', 1
    );

    -- Insert Employee Bank Account
    INSERT INTO EmployeeBankAccounts (EmployeeID, BankName, BankAccountNumber, AccountHolderName, AccountType, IsPrimary)
    VALUES (
        @EmployeeID, 'First National Bank',
        '123456789' + RIGHT('000' + CAST(@Counter AS NVARCHAR(10)), 3),
        @FirstName + ' ' + @LastName,
        'Checking', 1
    );

    -- Insert Employee Compensation
    DECLARE @BaseSalary DECIMAL(18,2) = 50000 + (RAND() * 100000);
    DECLARE @HousingAllowance DECIMAL(18,2) = @BaseSalary * 0.1;
    DECLARE @TransportAllowance DECIMAL(18,2) = @BaseSalary * 0.05;
    DECLARE @TotalAllowances DECIMAL(18,2) = @HousingAllowance + @TransportAllowance;
    DECLARE @IncomeTax DECIMAL(18,2) = @BaseSalary * 0.2;
    DECLARE @SocialSecurity DECIMAL(18,2) = @BaseSalary * 0.062;
    DECLARE @TotalDeductions DECIMAL(18,2) = @IncomeTax + @SocialSecurity;
    DECLARE @GrossSalary DECIMAL(18,2) = @BaseSalary + @TotalAllowances;
    DECLARE @NetSalary DECIMAL(18,2) = @GrossSalary - @TotalDeductions;

    INSERT INTO EmployeeCompensation (
        EmployeeID, BaseSalary, AnnualSalary, Currency, EffectiveDate, EndDate,
        HousingAllowance, TransportAllowance, TotalAllowances,
        IncomeTax, SocialSecurity, TotalDeductions,
        GrossSalary, NetSalary, TakeHomePay,
        BusinessUnit, Region, Country
    )
    VALUES (
        @EmployeeID, @BaseSalary, @BaseSalary * 12, 'USD', @StartDate, NULL,
        @HousingAllowance, @TransportAllowance, @TotalAllowances,
        @IncomeTax, @SocialSecurity, @TotalDeductions,
        @GrossSalary, @NetSalary, @NetSalary,
        @BusinessUnit, @Region, @Country
    );

    -- Insert Employee Absences (for active employees)
    IF @IsActive = 1
    BEGIN
        DECLARE @AbsenceCount INT = CAST(RAND() * 5 AS INT);
        DECLARE @AbsenceCounter INT = 1;
        
        WHILE @AbsenceCounter <= @AbsenceCount
        BEGIN
            DECLARE @AbsenceStartDate DATE = DATEADD(DAY, RAND() * 365, @StartDate);
            DECLARE @AbsenceEndDate DATE = DATEADD(DAY, CAST(RAND() * 10 AS INT), @AbsenceStartDate);
            DECLARE @AbsenceType NVARCHAR(50) = CASE 
                WHEN @AbsenceCounter % 4 = 1 THEN 'Sick Leave'
                WHEN @AbsenceCounter % 4 = 2 THEN 'Vacation'
                WHEN @AbsenceCounter % 4 = 3 THEN 'Personal Leave'
                ELSE 'Bereavement Leave'
            END;
            DECLARE @TotalDays DECIMAL(5,2) = DATEDIFF(DAY, @AbsenceStartDate, @AbsenceEndDate) + 1;
            DECLARE @IsApproved BIT = CASE WHEN @AbsenceCounter % 3 = 0 THEN 0 ELSE 1 END;
            DECLARE @IsPaid BIT = CASE WHEN @AbsenceType = 'Sick Leave' OR @AbsenceType = 'Vacation' THEN 1 ELSE 0 END;

            INSERT INTO EmployeeAbsences (
                EmployeeID, AbsenceType, AbsenceCategory, StartDate, EndDate,
                TotalDays, IsApproved, IsPaid, ApprovedBy
            )
            VALUES (
                @EmployeeID, @AbsenceType, 'Leave', @AbsenceStartDate, @AbsenceEndDate,
                @TotalDays, @IsApproved, @IsPaid,
                CASE WHEN @IsApproved = 1 THEN @ManagerID ELSE NULL END
            );

            SET @AbsenceCounter = @AbsenceCounter + 1;
        END;
    END;

    -- Insert Employee Security
    DECLARE @SecurityLevel NVARCHAR(20) = CASE 
        WHEN @Counter % 100 = 0 THEN 'High'
        WHEN @Counter % 50 = 0 THEN 'Medium'
        ELSE 'Low'
    END;
    DECLARE @AccessLevel NVARCHAR(20) = CASE 
        WHEN @Counter % 100 = 0 THEN 'Admin'
        WHEN @Counter % 50 = 0 THEN 'Manager'
        ELSE 'Employee'
    END;
    DECLARE @RoleID INT = (@Counter % @RoleCount) + 1;
    DECLARE @GroupID INT = (@Counter % @GroupCount) + 1;
    DECLARE @CanViewPayroll BIT = CASE WHEN @AccessLevel IN ('Admin', 'Manager') THEN 1 ELSE 0 END;
    DECLARE @CanViewPII BIT = CASE WHEN @AccessLevel = 'Admin' THEN 1 ELSE 0 END;

    INSERT INTO EmployeeSecurity (
        EmployeeID, SecurityLevel, AccessLevel, RoleID, SecurityGroupID,
        CanViewSalary, CanViewPII, CanViewFinancial, CanViewHR, CanViewPayroll,
        CanViewReports, CanExportData, CanModifyData, CanDeleteData, CanApprove
    )
    VALUES (
        @EmployeeID, @SecurityLevel, @AccessLevel, @RoleID, @GroupID,
        @CanViewPayroll, @CanViewPII, @CanViewPII, @CanViewPII, @CanViewPayroll,
        @CanViewPayroll, @CanViewPII, @CanViewPII, @CanViewPII, @CanViewPayroll
    );

    SET @Counter = @Counter + 1;
END;

-- Insert some edge case employees
INSERT INTO Employees (
    EmployeeNumber, FirstName, LastName, Email, Phone, DateOfBirth, Gender,
    StartDate, EndDate, TerminationDate, TerminationReason, TerminationType,
    DepartmentID, PositionID, ManagerID, SalaryGrade, JobLevel, EmploymentStatus,
    BusinessUnit, Region, Country, IsActive, ContractType, EmploymentType,
    SocialSecurityNumber, TaxID, PassportNumber
)
VALUES 
    ('EMP99999', 'John', 'Doe', 'john.doe@company.com', '555-9999', '1990-01-01', 'Male',
     '2024-01-01', NULL, NULL, NULL, NULL,
     1, 1, NULL, 'SE1', 'L3', 'Active',
     'Technology', 'North America', 'USA', 1, 'Full-Time', 'Permanent',
     '123-45-6789', 'TAX123456', 'PASS123456'),
    
    ('EMP99998', 'Jane', 'Smith', 'jane.smith@company.com', '555-9998', '1985-05-15', 'Female',
     '2023-06-01', '2024-08-15', '2024-08-15', 'Resignation', 'Voluntary',
     2, 6, NULL, 'SR1', 'L3', 'Terminated',
     'Sales', 'North America', 'USA', 0, 'Full-Time', 'Permanent',
     '987-65-4321', 'TAX654321', 'PASS654321');

-- Insert edge case compensation
INSERT INTO EmployeeCompensation (
    EmployeeID, BaseSalary, AnnualSalary, Currency, EffectiveDate, EndDate,
    HousingAllowance, TransportAllowance, TotalAllowances,
    IncomeTax, SocialSecurity, TotalDeductions,
    GrossSalary, NetSalary, TakeHomePay,
    BusinessUnit, Region, Country
)
VALUES 
    (1001, 150000, 1800000, 'USD', '2024-01-01', NULL,
     15000, 7500, 22500,
     30000, 9300, 39300,
     172500, 133200, 133200,
     'Technology', 'North America', 'USA'),
    
    (1002, 45000, 540000, 'USD', '2023-06-01', '2024-08-15',
     4500, 2250, 6750,
     9000, 2790, 11790,
     51750, 39960, 39960,
     'Sales', 'North America', 'USA');

-- Insert edge case absences
INSERT INTO EmployeeAbsences (
    EmployeeID, AbsenceType, AbsenceCategory, StartDate, EndDate,
    TotalDays, IsApproved, IsPaid, ApprovedBy
)
VALUES 
    (1001, 'Vacation', 'Leave', '2024-07-01', '2024-07-15', 15, 1, 1, NULL),
    (1001, 'Sick Leave', 'Leave', '2024-08-01', '2024-08-03', 3, 1, 1, NULL),
    (1002, 'Personal Leave', 'Leave', '2024-07-20', '2024-07-25', 6, 0, 0, NULL);

PRINT 'Comprehensive test data generated successfully!';
PRINT 'Total Employees: ' + CAST(@EmployeeCount AS NVARCHAR(10));
PRINT 'Total Departments: ' + CAST(@DepartmentCount AS NVARCHAR(10));
PRINT 'Total Positions: ' + CAST(@PositionCount AS NVARCHAR(10));
PRINT 'Total Security Roles: ' + CAST(@RoleCount AS NVARCHAR(10));
PRINT 'Total Security Groups: ' + CAST(@GroupCount AS NVARCHAR(10));
