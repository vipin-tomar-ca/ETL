--@name: EmployeePIIData
--@description: Extract employee PII (Personally Identifiable Information) data for payroll analytics - multi-tenant support
--@type: SELECT
--@timeout: 60
--@database: SQL Server
--@author: Payroll ETL Team
--@version: 1.0
--@tags: payroll,employee,pii,personal,multitenant,extraction
--@tenant: CLIENT_DATABASE
--@security: ENCRYPTED

SELECT 
    e.EmployeeID,
    e.EmployeeNumber,
    e.FirstName,
    e.LastName,
    e.MiddleName,
    e.Email,
    e.Phone,
    e.MobilePhone,
    e.DateOfBirth,
    e.Gender,
    e.MaritalStatus,
    e.Nationality,
    e.PassportNumber,
    e.SocialSecurityNumber,
    e.TaxID,
    e.DrivingLicenseNumber,
    e.EmergencyContactName,
    e.EmergencyContactPhone,
    e.EmergencyContactRelationship,
    -- Address Information
    a.AddressLine1,
    a.AddressLine2,
    a.City,
    a.State,
    a.PostalCode,
    a.Country,
    a.AddressType,
    -- Bank Information
    b.BankName,
    b.BankAccountNumber,
    b.BankRoutingNumber,
    b.IBAN,
    b.SWIFTCode,
    b.AccountHolderName,
    b.AccountType,
    -- Additional PII
    e.BloodGroup,
    e.MedicalConditions,
    e.Allergies,
    e.EmergencyMedicalContact,
    e.EmergencyMedicalPhone,
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
LEFT JOIN EmployeeAddresses a ON e.EmployeeID = a.EmployeeID AND a.IsPrimary = 1
LEFT JOIN EmployeeBankAccounts b ON e.EmployeeID = b.EmployeeID AND b.IsPrimary = 1
WHERE e.IsActive = 1
    AND (@DepartmentID IS NULL OR e.DepartmentID = @DepartmentID)
    AND (@EmployeeID IS NULL OR e.EmployeeID = @EmployeeID)
    AND (@EmployeeNumber IS NULL OR e.EmployeeNumber = @EmployeeNumber)
ORDER BY e.EmployeeID;
