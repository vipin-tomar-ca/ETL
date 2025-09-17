--@name: CreatePayrollTestDatabase
--@description: Create comprehensive payroll test database with all required tables and relationships
--@type: DDL
--@timeout: 300
--@database: SQL Server
--@author: Payroll ETL Team
--@version: 1.0
--@tags: database,setup,payroll,test

-- Create test database
IF NOT EXISTS (SELECT name FROM sys.databases WHERE name = 'PayrollTestDB')
BEGIN
    CREATE DATABASE PayrollTestDB;
END
GO

USE PayrollTestDB;
GO

-- Create Departments table
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[Departments]') AND type in (N'U'))
BEGIN
    CREATE TABLE [dbo].[Departments] (
        [DepartmentID] INT IDENTITY(1,1) NOT NULL,
        [DepartmentName] NVARCHAR(100) NOT NULL,
        [DepartmentCode] NVARCHAR(20) NOT NULL,
        [BusinessUnit] NVARCHAR(50) NOT NULL,
        [Region] NVARCHAR(50) NOT NULL,
        [Country] NVARCHAR(50) NOT NULL,
        [CostCenter] NVARCHAR(20) NULL,
        [IsActive] BIT NOT NULL DEFAULT 1,
        [CreatedDate] DATETIME2 NOT NULL DEFAULT GETDATE(),
        [CreatedBy] NVARCHAR(100) NOT NULL DEFAULT 'SYSTEM',
        [ModifiedDate] DATETIME2 NULL,
        [ModifiedBy] NVARCHAR(100) NULL,
        CONSTRAINT [PK_Departments] PRIMARY KEY CLUSTERED ([DepartmentID] ASC)
    );
END
GO

-- Create Positions table
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[Positions]') AND type in (N'U'))
BEGIN
    CREATE TABLE [dbo].[Positions] (
        [PositionID] INT IDENTITY(1,1) NOT NULL,
        [PositionTitle] NVARCHAR(100) NOT NULL,
        [PositionCode] NVARCHAR(20) NOT NULL,
        [JobLevel] NVARCHAR(20) NOT NULL,
        [SalaryGrade] NVARCHAR(20) NOT NULL,
        [IsActive] BIT NOT NULL DEFAULT 1,
        [CreatedDate] DATETIME2 NOT NULL DEFAULT GETDATE(),
        [CreatedBy] NVARCHAR(100) NOT NULL DEFAULT 'SYSTEM',
        [ModifiedDate] DATETIME2 NULL,
        [ModifiedBy] NVARCHAR(100) NULL,
        CONSTRAINT [PK_Positions] PRIMARY KEY CLUSTERED ([PositionID] ASC)
    );
END
GO

-- Create Employees table
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[Employees]') AND type in (N'U'))
BEGIN
    CREATE TABLE [dbo].[Employees] (
        [EmployeeID] INT IDENTITY(1,1) NOT NULL,
        [EmployeeNumber] NVARCHAR(20) NOT NULL,
        [FirstName] NVARCHAR(50) NOT NULL,
        [LastName] NVARCHAR(50) NOT NULL,
        [MiddleName] NVARCHAR(50) NULL,
        [Email] NVARCHAR(100) NOT NULL,
        [Phone] NVARCHAR(20) NULL,
        [MobilePhone] NVARCHAR(20) NULL,
        [DateOfBirth] DATE NOT NULL,
        [Gender] NVARCHAR(10) NOT NULL,
        [MaritalStatus] NVARCHAR(20) NULL,
        [Nationality] NVARCHAR(50) NULL,
        [PassportNumber] NVARCHAR(50) NULL,
        [SocialSecurityNumber] NVARCHAR(20) NULL,
        [TaxID] NVARCHAR(20) NULL,
        [DrivingLicenseNumber] NVARCHAR(50) NULL,
        [EmergencyContactName] NVARCHAR(100) NULL,
        [EmergencyContactPhone] NVARCHAR(20) NULL,
        [EmergencyContactRelationship] NVARCHAR(50) NULL,
        [BloodGroup] NVARCHAR(10) NULL,
        [MedicalConditions] NVARCHAR(500) NULL,
        [Allergies] NVARCHAR(500) NULL,
        [EmergencyMedicalContact] NVARCHAR(100) NULL,
        [EmergencyMedicalPhone] NVARCHAR(20) NULL,
        [StartDate] DATE NOT NULL,
        [EndDate] DATE NULL,
        [TerminationDate] DATE NULL,
        [TerminationReason] NVARCHAR(100) NULL,
        [TerminationType] NVARCHAR(50) NULL,
        [TerminationCategory] NVARCHAR(50) NULL,
        [NoticePeriod] INT NULL,
        [NoticePeriodUnit] NVARCHAR(20) NULL,
        [LastWorkingDate] DATE NULL,
        [ExitInterviewDate] DATE NULL,
        [ExitInterviewConducted] BIT NULL,
        [ExitInterviewNotes] NVARCHAR(1000) NULL,
        [FinalSettlementDate] DATE NULL,
        [FinalSettlementAmount] DECIMAL(18,2) NULL,
        [OutstandingAmount] DECIMAL(18,2) NULL,
        [ProbationEndDate] DATE NULL,
        [ContractType] NVARCHAR(50) NOT NULL,
        [EmploymentType] NVARCHAR(50) NOT NULL,
        [WorkLocation] NVARCHAR(100) NULL,
        [DepartmentID] INT NOT NULL,
        [PositionID] INT NOT NULL,
        [ManagerID] INT NULL,
        [SalaryGrade] NVARCHAR(20) NOT NULL,
        [JobLevel] NVARCHAR(20) NOT NULL,
        [EmploymentStatus] NVARCHAR(50) NOT NULL,
        [HireSource] NVARCHAR(100) NULL,
        [ReferralEmployeeID] INT NULL,
        [RecruitmentAgency] NVARCHAR(100) NULL,
        [CostCenter] NVARCHAR(20) NULL,
        [BusinessUnit] NVARCHAR(50) NOT NULL,
        [Region] NVARCHAR(50) NOT NULL,
        [Country] NVARCHAR(50) NOT NULL,
        [IsActive] BIT NOT NULL DEFAULT 1,
        [CreatedDate] DATETIME2 NOT NULL DEFAULT GETDATE(),
        [CreatedBy] NVARCHAR(100) NOT NULL DEFAULT 'SYSTEM',
        [ModifiedDate] DATETIME2 NULL,
        [ModifiedBy] NVARCHAR(100) NULL,
        CONSTRAINT [PK_Employees] PRIMARY KEY CLUSTERED ([EmployeeID] ASC),
        CONSTRAINT [FK_Employees_Departments] FOREIGN KEY ([DepartmentID]) REFERENCES [dbo].[Departments] ([DepartmentID]),
        CONSTRAINT [FK_Employees_Positions] FOREIGN KEY ([PositionID]) REFERENCES [dbo].[Positions] ([PositionID]),
        CONSTRAINT [FK_Employees_Managers] FOREIGN KEY ([ManagerID]) REFERENCES [dbo].[Employees] ([EmployeeID])
    );
END
GO

-- Create EmployeeAddresses table
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[EmployeeAddresses]') AND type in (N'U'))
BEGIN
    CREATE TABLE [dbo].[EmployeeAddresses] (
        [AddressID] INT IDENTITY(1,1) NOT NULL,
        [EmployeeID] INT NOT NULL,
        [AddressType] NVARCHAR(20) NOT NULL,
        [AddressLine1] NVARCHAR(100) NOT NULL,
        [AddressLine2] NVARCHAR(100) NULL,
        [City] NVARCHAR(50) NOT NULL,
        [State] NVARCHAR(50) NOT NULL,
        [PostalCode] NVARCHAR(20) NOT NULL,
        [Country] NVARCHAR(50) NOT NULL,
        [IsPrimary] BIT NOT NULL DEFAULT 0,
        [IsActive] BIT NOT NULL DEFAULT 1,
        [CreatedDate] DATETIME2 NOT NULL DEFAULT GETDATE(),
        [CreatedBy] NVARCHAR(100) NOT NULL DEFAULT 'SYSTEM',
        [ModifiedDate] DATETIME2 NULL,
        [ModifiedBy] NVARCHAR(100) NULL,
        CONSTRAINT [PK_EmployeeAddresses] PRIMARY KEY CLUSTERED ([AddressID] ASC),
        CONSTRAINT [FK_EmployeeAddresses_Employees] FOREIGN KEY ([EmployeeID]) REFERENCES [dbo].[Employees] ([EmployeeID])
    );
END
GO

-- Create EmployeeBankAccounts table
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[EmployeeBankAccounts]') AND type in (N'U'))
BEGIN
    CREATE TABLE [dbo].[EmployeeBankAccounts] (
        [BankAccountID] INT IDENTITY(1,1) NOT NULL,
        [EmployeeID] INT NOT NULL,
        [BankName] NVARCHAR(100) NOT NULL,
        [BankAccountNumber] NVARCHAR(50) NOT NULL,
        [BankRoutingNumber] NVARCHAR(20) NULL,
        [IBAN] NVARCHAR(50) NULL,
        [SWIFTCode] NVARCHAR(20) NULL,
        [AccountHolderName] NVARCHAR(100) NOT NULL,
        [AccountType] NVARCHAR(20) NOT NULL,
        [IsPrimary] BIT NOT NULL DEFAULT 0,
        [IsActive] BIT NOT NULL DEFAULT 1,
        [CreatedDate] DATETIME2 NOT NULL DEFAULT GETDATE(),
        [CreatedBy] NVARCHAR(100) NOT NULL DEFAULT 'SYSTEM',
        [ModifiedDate] DATETIME2 NULL,
        [ModifiedBy] NVARCHAR(100) NULL,
        CONSTRAINT [PK_EmployeeBankAccounts] PRIMARY KEY CLUSTERED ([BankAccountID] ASC),
        CONSTRAINT [FK_EmployeeBankAccounts_Employees] FOREIGN KEY ([EmployeeID]) REFERENCES [dbo].[Employees] ([EmployeeID])
    );
END
GO

-- Create EmployeeCompensation table
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[EmployeeCompensation]') AND type in (N'U'))
BEGIN
    CREATE TABLE [dbo].[EmployeeCompensation] (
        [CompensationID] INT IDENTITY(1,1) NOT NULL,
        [EmployeeID] INT NOT NULL,
        [BaseSalary] DECIMAL(18,2) NOT NULL,
        [AnnualSalary] DECIMAL(18,2) NOT NULL,
        [HourlyRate] DECIMAL(18,2) NULL,
        [Currency] NVARCHAR(3) NOT NULL DEFAULT 'USD',
        [SalaryFrequency] NVARCHAR(20) NOT NULL DEFAULT 'MONTHLY',
        [EffectiveDate] DATE NOT NULL,
        [EndDate] DATE NULL,
        [HousingAllowance] DECIMAL(18,2) NULL,
        [TransportAllowance] DECIMAL(18,2) NULL,
        [MealAllowance] DECIMAL(18,2) NULL,
        [MedicalAllowance] DECIMAL(18,2) NULL,
        [CommunicationAllowance] DECIMAL(18,2) NULL,
        [OtherAllowances] DECIMAL(18,2) NULL,
        [TotalAllowances] DECIMAL(18,2) NULL,
        [HealthInsurance] DECIMAL(18,2) NULL,
        [LifeInsurance] DECIMAL(18,2) NULL,
        [RetirementContribution] DECIMAL(18,2) NULL,
        [ProvidentFund] DECIMAL(18,2) NULL,
        [Gratuity] DECIMAL(18,2) NULL,
        [Bonus] DECIMAL(18,2) NULL,
        [Commission] DECIMAL(18,2) NULL,
        [OvertimeRate] DECIMAL(18,2) NULL,
        [ShiftAllowance] DECIMAL(18,2) NULL,
        [NightShiftAllowance] DECIMAL(18,2) NULL,
        [WeekendAllowance] DECIMAL(18,2) NULL,
        [HolidayAllowance] DECIMAL(18,2) NULL,
        [IncomeTax] DECIMAL(18,2) NULL,
        [SocialSecurity] DECIMAL(18,2) NULL,
        [HealthInsuranceDeduction] DECIMAL(18,2) NULL,
        [LoanDeduction] DECIMAL(18,2) NULL,
        [AdvanceDeduction] DECIMAL(18,2) NULL,
        [OtherDeductions] DECIMAL(18,2) NULL,
        [TotalDeductions] DECIMAL(18,2) NULL,
        [GrossSalary] DECIMAL(18,2) NOT NULL,
        [NetSalary] DECIMAL(18,2) NOT NULL,
        [TakeHomePay] DECIMAL(18,2) NOT NULL,
        [PayrollGroup] NVARCHAR(50) NULL,
        [PayrollCategory] NVARCHAR(50) NULL,
        [CostCenter] NVARCHAR(20) NULL,
        [BusinessUnit] NVARCHAR(50) NOT NULL,
        [Region] NVARCHAR(50) NOT NULL,
        [Country] NVARCHAR(50) NOT NULL,
        [IsActive] BIT NOT NULL DEFAULT 1,
        [CreatedDate] DATETIME2 NOT NULL DEFAULT GETDATE(),
        [CreatedBy] NVARCHAR(100) NOT NULL DEFAULT 'SYSTEM',
        [ModifiedDate] DATETIME2 NULL,
        [ModifiedBy] NVARCHAR(100) NULL,
        CONSTRAINT [PK_EmployeeCompensation] PRIMARY KEY CLUSTERED ([CompensationID] ASC),
        CONSTRAINT [FK_EmployeeCompensation_Employees] FOREIGN KEY ([EmployeeID]) REFERENCES [dbo].[Employees] ([EmployeeID])
    );
END
GO

-- Create EmployeeAbsences table
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[EmployeeAbsences]') AND type in (N'U'))
BEGIN
    CREATE TABLE [dbo].[EmployeeAbsences] (
        [AbsenceID] INT IDENTITY(1,1) NOT NULL,
        [EmployeeID] INT NOT NULL,
        [AbsenceType] NVARCHAR(50) NOT NULL,
        [AbsenceCategory] NVARCHAR(50) NOT NULL,
        [AbsenceReason] NVARCHAR(200) NULL,
        [StartDate] DATE NOT NULL,
        [EndDate] DATE NOT NULL,
        [StartTime] TIME NULL,
        [EndTime] TIME NULL,
        [TotalDays] DECIMAL(5,2) NOT NULL,
        [TotalHours] DECIMAL(8,2) NULL,
        [IsFullDay] BIT NOT NULL DEFAULT 1,
        [IsHalfDay] BIT NOT NULL DEFAULT 0,
        [IsApproved] BIT NOT NULL DEFAULT 0,
        [ApprovedBy] INT NULL,
        [ApprovedDate] DATE NULL,
        [ApprovalNotes] NVARCHAR(500) NULL,
        [IsPaid] BIT NOT NULL DEFAULT 0,
        [PayRate] DECIMAL(18,2) NULL,
        [DeductionAmount] DECIMAL(18,2) NULL,
        [CompensationAmount] DECIMAL(18,2) NULL,
        [LeaveBalanceBefore] DECIMAL(5,2) NULL,
        [LeaveBalanceAfter] DECIMAL(5,2) NULL,
        [LeaveBalanceUsed] DECIMAL(5,2) NULL,
        [DocumentationRequired] BIT NOT NULL DEFAULT 0,
        [DocumentationProvided] BIT NOT NULL DEFAULT 0,
        [DocumentationNotes] NVARCHAR(500) NULL,
        [MedicalCertificate] BIT NOT NULL DEFAULT 0,
        [EmergencyContact] NVARCHAR(100) NULL,
        [EmergencyPhone] NVARCHAR(20) NULL,
        [ReturnToWorkDate] DATE NULL,
        [ReturnToWorkNotes] NVARCHAR(500) NULL,
        [IsActive] BIT NOT NULL DEFAULT 1,
        [CreatedDate] DATETIME2 NOT NULL DEFAULT GETDATE(),
        [CreatedBy] NVARCHAR(100) NOT NULL DEFAULT 'SYSTEM',
        [ModifiedDate] DATETIME2 NULL,
        [ModifiedBy] NVARCHAR(100) NULL,
        CONSTRAINT [PK_EmployeeAbsences] PRIMARY KEY CLUSTERED ([AbsenceID] ASC),
        CONSTRAINT [FK_EmployeeAbsences_Employees] FOREIGN KEY ([EmployeeID]) REFERENCES [dbo].[Employees] ([EmployeeID]),
        CONSTRAINT [FK_EmployeeAbsences_ApprovedBy] FOREIGN KEY ([ApprovedBy]) REFERENCES [dbo].[Employees] ([EmployeeID])
    );
END
GO

-- Create SecurityRoles table
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[SecurityRoles]') AND type in (N'U'))
BEGIN
    CREATE TABLE [dbo].[SecurityRoles] (
        [RoleID] INT IDENTITY(1,1) NOT NULL,
        [RoleName] NVARCHAR(50) NOT NULL,
        [RoleDescription] NVARCHAR(200) NULL,
        [IsActive] BIT NOT NULL DEFAULT 1,
        [CreatedDate] DATETIME2 NOT NULL DEFAULT GETDATE(),
        [CreatedBy] NVARCHAR(100) NOT NULL DEFAULT 'SYSTEM',
        [ModifiedDate] DATETIME2 NULL,
        [ModifiedBy] NVARCHAR(100) NULL,
        CONSTRAINT [PK_SecurityRoles] PRIMARY KEY CLUSTERED ([RoleID] ASC)
    );
END
GO

-- Create SecurityGroups table
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[SecurityGroups]') AND type in (N'U'))
BEGIN
    CREATE TABLE [dbo].[SecurityGroups] (
        [SecurityGroupID] INT IDENTITY(1,1) NOT NULL,
        [SecurityGroupName] NVARCHAR(50) NOT NULL,
        [SecurityGroupDescription] NVARCHAR(200) NULL,
        [IsActive] BIT NOT NULL DEFAULT 1,
        [CreatedDate] DATETIME2 NOT NULL DEFAULT GETDATE(),
        [CreatedBy] NVARCHAR(100) NOT NULL DEFAULT 'SYSTEM',
        [ModifiedDate] DATETIME2 NULL,
        [ModifiedBy] NVARCHAR(100) NULL,
        CONSTRAINT [PK_SecurityGroups] PRIMARY KEY CLUSTERED ([SecurityGroupID] ASC)
    );
END
GO

-- Create EmployeeSecurity table
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[EmployeeSecurity]') AND type in (N'U'))
BEGIN
    CREATE TABLE [dbo].[EmployeeSecurity] (
        [SecurityID] INT IDENTITY(1,1) NOT NULL,
        [EmployeeID] INT NOT NULL,
        [SecurityLevel] NVARCHAR(20) NOT NULL,
        [AccessLevel] NVARCHAR(20) NOT NULL,
        [PermissionSet] NVARCHAR(100) NULL,
        [RoleID] INT NULL,
        [SecurityGroupID] INT NULL,
        [CanViewSalary] BIT NOT NULL DEFAULT 0,
        [CanViewPII] BIT NOT NULL DEFAULT 0,
        [CanViewFinancial] BIT NOT NULL DEFAULT 0,
        [CanViewHR] BIT NOT NULL DEFAULT 0,
        [CanViewPayroll] BIT NOT NULL DEFAULT 0,
        [CanViewReports] BIT NOT NULL DEFAULT 0,
        [CanExportData] BIT NOT NULL DEFAULT 0,
        [CanModifyData] BIT NOT NULL DEFAULT 0,
        [CanDeleteData] BIT NOT NULL DEFAULT 0,
        [CanApprove] BIT NOT NULL DEFAULT 0,
        [CanReject] BIT NOT NULL DEFAULT 0,
        [DepartmentAccess] NVARCHAR(500) NULL,
        [EmployeeAccess] NVARCHAR(500) NULL,
        [DataAccessScope] NVARCHAR(100) NULL,
        [GeographicAccess] NVARCHAR(500) NULL,
        [TimeBasedAccess] NVARCHAR(200) NULL,
        [IPRestrictions] NVARCHAR(500) NULL,
        [DeviceRestrictions] NVARCHAR(500) NULL,
        [IsActive] BIT NOT NULL DEFAULT 1,
        [IsSuspended] BIT NOT NULL DEFAULT 0,
        [SuspensionReason] NVARCHAR(200) NULL,
        [SuspensionDate] DATE NULL,
        [SuspensionEndDate] DATE NULL,
        [LastAccessDate] DATETIME2 NULL,
        [LastAccessIP] NVARCHAR(50) NULL,
        [LastAccessDevice] NVARCHAR(100) NULL,
        [FailedLoginAttempts] INT NOT NULL DEFAULT 0,
        [LastFailedLogin] DATETIME2 NULL,
        [PasswordExpiryDate] DATE NULL,
        [TwoFactorEnabled] BIT NOT NULL DEFAULT 0,
        [SSOEnabled] BIT NOT NULL DEFAULT 0,
        [CreatedDate] DATETIME2 NOT NULL DEFAULT GETDATE(),
        [CreatedBy] NVARCHAR(100) NOT NULL DEFAULT 'SYSTEM',
        [ModifiedDate] DATETIME2 NULL,
        [ModifiedBy] NVARCHAR(100) NULL,
        [ApprovedBy] INT NULL,
        [ApprovedDate] DATE NULL,
        [ReviewDate] DATE NULL,
        [NextReviewDate] DATE NULL,
        CONSTRAINT [PK_EmployeeSecurity] PRIMARY KEY CLUSTERED ([SecurityID] ASC),
        CONSTRAINT [FK_EmployeeSecurity_Employees] FOREIGN KEY ([EmployeeID]) REFERENCES [dbo].[Employees] ([EmployeeID]),
        CONSTRAINT [FK_EmployeeSecurity_Roles] FOREIGN KEY ([RoleID]) REFERENCES [dbo].[SecurityRoles] ([RoleID]),
        CONSTRAINT [FK_EmployeeSecurity_Groups] FOREIGN KEY ([SecurityGroupID]) REFERENCES [dbo].[SecurityGroups] ([SecurityGroupID])
    );
END
GO

-- Create indexes for better performance
CREATE NONCLUSTERED INDEX [IX_Employees_EmployeeNumber] ON [dbo].[Employees] ([EmployeeNumber]);
CREATE NONCLUSTERED INDEX [IX_Employees_Email] ON [dbo].[Employees] ([Email]);
CREATE NONCLUSTERED INDEX [IX_Employees_DepartmentID] ON [dbo].[Employees] ([DepartmentID]);
CREATE NONCLUSTERED INDEX [IX_Employees_PositionID] ON [dbo].[Employees] ([PositionID]);
CREATE NONCLUSTERED INDEX [IX_Employees_ManagerID] ON [dbo].[Employees] ([ManagerID]);
CREATE NONCLUSTERED INDEX [IX_Employees_StartDate] ON [dbo].[Employees] ([StartDate]);
CREATE NONCLUSTERED INDEX [IX_Employees_EndDate] ON [dbo].[Employees] ([EndDate]);
CREATE NONCLUSTERED INDEX [IX_Employees_IsActive] ON [dbo].[Employees] ([IsActive]);

CREATE NONCLUSTERED INDEX [IX_EmployeeCompensation_EmployeeID] ON [dbo].[EmployeeCompensation] ([EmployeeID]);
CREATE NONCLUSTERED INDEX [IX_EmployeeCompensation_EffectiveDate] ON [dbo].[EmployeeCompensation] ([EffectiveDate]);
CREATE NONCLUSTERED INDEX [IX_EmployeeCompensation_EndDate] ON [dbo].[EmployeeCompensation] ([EndDate]);

CREATE NONCLUSTERED INDEX [IX_EmployeeAbsences_EmployeeID] ON [dbo].[EmployeeAbsences] ([EmployeeID]);
CREATE NONCLUSTERED INDEX [IX_EmployeeAbsences_StartDate] ON [dbo].[EmployeeAbsences] ([StartDate]);
CREATE NONCLUSTERED INDEX [IX_EmployeeAbsences_EndDate] ON [dbo].[EmployeeAbsences] ([EndDate]);
CREATE NONCLUSTERED INDEX [IX_EmployeeAbsences_AbsenceType] ON [dbo].[EmployeeAbsences] ([AbsenceType]);

CREATE NONCLUSTERED INDEX [IX_EmployeeSecurity_EmployeeID] ON [dbo].[EmployeeSecurity] ([EmployeeID]);
CREATE NONCLUSTERED INDEX [IX_EmployeeSecurity_RoleID] ON [dbo].[EmployeeSecurity] ([RoleID]);
CREATE NONCLUSTERED INDEX [IX_EmployeeSecurity_SecurityGroupID] ON [dbo].[EmployeeSecurity] ([SecurityGroupID]);

PRINT 'PayrollTestDB database and tables created successfully!';
