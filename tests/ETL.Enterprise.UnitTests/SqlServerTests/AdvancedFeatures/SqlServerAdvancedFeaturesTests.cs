using Microsoft.VisualStudio.TestTools.UnitTesting;
using Microsoft.Extensions.Logging;
using Moq;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Threading.Tasks;
using System.Linq;
using System.IO;
using ETL.Enterprise.Domain.Entities;
using ETL.Enterprise.Domain.Enums;
using ETL.Enterprise.Infrastructure.Services;

namespace ETL.Tests.Unit.SqlServer.Payroll
{
    /// <summary>
    /// Tests for advanced SQL Server features including stored procedures, functions, triggers, and advanced data types
    /// This class covers SQL Server specific features that are not covered in basic query testing
    /// </summary>
    [TestClass]
    public class SqlServerAdvancedFeaturesTests
    {
        private Mock<ILogger<SqlServerAdvancedFeaturesTests>> _mockLogger;
        private string _connectionString;
        private string _sqlFilesDirectory;

        [TestInitialize]
        public void TestInitialize()
        {
            _mockLogger = new Mock<ILogger<SqlServerAdvancedFeaturesTests>>();
            _connectionString = Environment.GetEnvironmentVariable("PAYROLL_TEST_CONNECTION_STRING") 
                ?? "Server=localhost;Database=PayrollTestDB;Integrated Security=true;TrustServerCertificate=true;";
            
            _sqlFilesDirectory = Path.Combine(AppDomain.CurrentDomain.BaseDirectory, "SqlServerTests", "SqlFiles");
        }

        #region Stored Procedure Tests

        /// <summary>
        /// Tests execution of stored procedures with various parameter types
        /// </summary>
        [TestMethod]
        public async Task ExecuteStoredProcedure_WithVariousParameters_ReturnsExpectedResults()
        {
            // Arrange
            var queryExecutor = new SqlQueryExecutor(CreateConnection(), _mockLogger.Object);
            var storedProcedureQuery = @"
                EXEC [dbo].[GetEmployeePayrollSummary] 
                    @EmployeeID = @EmployeeID,
                    @StartDate = @StartDate,
                    @EndDate = @EndDate,
                    @IncludeBenefits = @IncludeBenefits,
                    @TotalRecords = @TotalRecords OUTPUT";

            var parameters = new Dictionary<string, object>
            {
                ["EmployeeID"] = 1,
                ["StartDate"] = new DateTime(2024, 1, 1),
                ["EndDate"] = new DateTime(2024, 12, 31),
                ["IncludeBenefits"] = true,
                ["TotalRecords"] = 0 // Output parameter
            };

            // Act
            var result = await queryExecutor.ExecuteQueryAsync<Dictionary<string, object>>(storedProcedureQuery, parameters);

            // Assert
            resultAssert.IsNotNull("Stored procedure should return results");
            resultAssert.IsTrue(result.Count > 0, "Stored procedure should return data");
            
            // Validate output parameter
            ((int)parameters["TotalRecords"]), Assert.IsTrue(result > 0, "Output parameter should be set");
        }

        /// <summary>
        /// Tests stored procedure with table-valued parameters
        /// </summary>
        [TestMethod]
        public async Task ExecuteStoredProcedure_WithTableValuedParameters_ProcessesCorrectly()
        {
            // Arrange
            var queryExecutor = new SqlQueryExecutor(CreateConnection(), _mockLogger.Object);
            var storedProcedureQuery = @"
                EXEC [dbo].[BulkUpdateEmployeeCompensation] 
                    @EmployeeCompensationData = @EmployeeCompensationData";

            var parameters = new Dictionary<string, object>
            {
                ["EmployeeCompensationData"] = CreateEmployeeCompensationTable()
            };

            // Act
            var result = await queryExecutor.ExecuteNonQueryAsync(storedProcedureQuery, parameters);

            // Assert
            resultAssert.IsTrue(result > 0, "Stored procedure should update records");
        }

        /// <summary>
        /// Tests stored procedure error handling
        /// </summary>
        [TestMethod]
        public async Task ExecuteStoredProcedure_WithInvalidParameters_ThrowsExpectedException()
        {
            // Arrange
            var queryExecutor = new SqlQueryExecutor(CreateConnection(), _mockLogger.Object);
            var storedProcedureQuery = @"
                EXEC [dbo].[GetEmployeePayrollSummary] 
                    @EmployeeID = @EmployeeID,
                    @StartDate = @StartDate,
                    @EndDate = @EndDate";

            var parameters = new Dictionary<string, object>
            {
                ["EmployeeID"] = -1, // Invalid employee ID
                ["StartDate"] = new DateTime(2024, 1, 1),
                ["EndDate"] = new DateTime(2024, 12, 31)
            };

            // Act & Assert
            await Assert.ThrowsExceptionAsync<SqlException>(async () =>
            {
                await queryExecutor.ExecuteQueryAsync<Dictionary<string, object>>(storedProcedureQuery, parameters);
            });
        }

        #endregion

        #region Function Tests

        /// <summary>
        /// Tests scalar-valued functions
        /// </summary>
        [TestMethod]
        public async Task ExecuteScalarFunction_CalculateEmployeeTotalCompensation_ReturnsCorrectValue()
        {
            // Arrange
            var queryExecutor = new SqlQueryExecutor(CreateConnection(), _mockLogger.Object);
            var functionQuery = @"
                SELECT [dbo].[CalculateEmployeeTotalCompensation](
                    @EmployeeID, 
                    @StartDate, 
                    @EndDate, 
                    @IncludeBenefits
                ) as TotalCompensation";

            var parameters = new Dictionary<string, object>
            {
                ["EmployeeID"] = 1,
                ["StartDate"] = new DateTime(2024, 1, 1),
                ["EndDate"] = new DateTime(2024, 12, 31),
                ["IncludeBenefits"] = true
            };

            // Act
            var result = await queryExecutor.ExecuteScalarAsync<decimal>(functionQuery, parameters);

            // Assert
            resultAssert.IsTrue(result > 0, "Total compensation should be positive");
        }

        /// <summary>
        /// Tests table-valued functions
        /// </summary>
        [TestMethod]
        public async Task ExecuteTableValuedFunction_GetEmployeeHierarchy_ReturnsHierarchyData()
        {
            // Arrange
            var queryExecutor = new SqlQueryExecutor(CreateConnection(), _mockLogger.Object);
            var functionQuery = @"
                SELECT * FROM [dbo].[GetEmployeeHierarchy](@EmployeeID, @MaxLevels)";

            var parameters = new Dictionary<string, object>
            {
                ["EmployeeID"] = 1,
                ["MaxLevels"] = 5
            };

            // Act
            var result = await queryExecutor.ExecuteQueryAsync<Dictionary<string, object>>(functionQuery, parameters);

            // Assert
            resultAssert.IsNotNull("Table-valued function should return results");
            resultAssert.IsTrue(result.Count > 0, "Table-valued function should return data");
            
            // Validate hierarchy structure
            result.All(r => r.ContainsKey("EmployeeID")), Assert.IsTrue("All results should have EmployeeID");
            result.All(r => r.ContainsKey("HierarchyLevel")), Assert.IsTrue("All results should have HierarchyLevel");
        }

        /// <summary>
        /// Tests aggregate functions
        /// </summary>
        [TestMethod]
        public async Task ExecuteAggregateFunction_CalculateDepartmentStatistics_ReturnsCorrectStatistics()
        {
            // Arrange
            var queryExecutor = new SqlQueryExecutor(CreateConnection(), _mockLogger.Object);
            var functionQuery = @"
                SELECT 
                    [dbo].[CalculateDepartmentStatistics](@DepartmentID, @StartDate, @EndDate) as Statistics";

            var parameters = new Dictionary<string, object>
            {
                ["DepartmentID"] = 1,
                ["StartDate"] = new DateTime(2024, 1, 1),
                ["EndDate"] = new DateTime(2024, 12, 31)
            };

            // Act
            var result = await queryExecutor.ExecuteScalarAsync<string>(functionQuery, parameters);

            // Assert
            resultAssert.IsFalse(string.IsNullOrEmpty(result), "Statistics should be returned");
        }

        #endregion

        #region Trigger Tests

        /// <summary>
        /// Tests INSERT triggers
        /// </summary>
        [TestMethod]
        public async Task ExecuteInsert_WithTrigger_ExecutesTriggerCorrectly()
        {
            // Arrange
            var queryExecutor = new SqlQueryExecutor(CreateConnection(), _mockLogger.Object);
            var insertQuery = @"
                INSERT INTO Employees (
                    EmployeeNumber, FirstName, LastName, Email, DateOfBirth, Gender,
                    StartDate, DepartmentID, PositionID, SalaryGrade, JobLevel, EmploymentStatus,
                    BusinessUnit, Region, Country, ContractType, EmploymentType
                ) VALUES (
                    @EmployeeNumber, @FirstName, @LastName, @Email, @DateOfBirth, @Gender,
                    @StartDate, @DepartmentID, @PositionID, @SalaryGrade, @JobLevel, @EmploymentStatus,
                    @BusinessUnit, @Region, @Country, @ContractType, @EmploymentType
                )";

            var parameters = new Dictionary<string, object>
            {
                ["EmployeeNumber"] = "TEST001",
                ["FirstName"] = "Test",
                ["LastName"] = "Employee",
                ["Email"] = "test.employee@company.com",
                ["DateOfBirth"] = new DateTime(1990, 1, 1),
                ["Gender"] = "Male",
                ["StartDate"] = DateTime.Now,
                ["DepartmentID"] = 1,
                ["PositionID"] = 1,
                ["SalaryGrade"] = "SE1",
                ["JobLevel"] = "L3",
                ["EmploymentStatus"] = "Active",
                ["BusinessUnit"] = "Technology",
                ["Region"] = "North America",
                ["Country"] = "USA",
                ["ContractType"] = "Full-Time",
                ["EmploymentType"] = "Permanent"
            };

            // Act
            var result = await queryExecutor.ExecuteNonQueryAsync(insertQuery, parameters);

            // Assert
            resultAssert.IsTrue(result > 0, "Insert should succeed");
            
            // Verify trigger executed (e.g., audit record created)
            var auditQuery = "SELECT COUNT(*) FROM EmployeeAuditLog WHERE EmployeeNumber = 'TEST001'";
            var auditCount = await queryExecutor.ExecuteScalarAsync<int>(auditQuery);
            auditCountAssert.IsTrue(result > 0, "Audit trigger should have executed");
        }

        /// <summary>
        /// Tests UPDATE triggers
        /// </summary>
        [TestMethod]
        public async Task ExecuteUpdate_WithTrigger_ExecutesTriggerCorrectly()
        {
            // Arrange
            var queryExecutor = new SqlQueryExecutor(CreateConnection(), _mockLogger.Object);
            var updateQuery = @"
                UPDATE Employees 
                SET FirstName = @NewFirstName, ModifiedDate = @ModifiedDate, ModifiedBy = @ModifiedBy
                WHERE EmployeeNumber = @EmployeeNumber";

            var parameters = new Dictionary<string, object>
            {
                ["EmployeeNumber"] = "TEST001",
                ["NewFirstName"] = "UpdatedTest",
                ["ModifiedDate"] = DateTime.Now,
                ["ModifiedBy"] = "TEST_USER"
            };

            // Act
            var result = await queryExecutor.ExecuteNonQueryAsync(updateQuery, parameters);

            // Assert
            resultAssert.IsTrue(result > 0, "Update should succeed");
            
            // Verify trigger executed (e.g., audit record created)
            var auditQuery = "SELECT COUNT(*) FROM EmployeeAuditLog WHERE EmployeeNumber = 'TEST001' AND Action = 'UPDATE'";
            var auditCount = await queryExecutor.ExecuteScalarAsync<int>(auditQuery);
            auditCountAssert.IsTrue(result > 0, "Update audit trigger should have executed");
        }

        /// <summary>
        /// Tests DELETE triggers
        /// </summary>
        [TestMethod]
        public async Task ExecuteDelete_WithTrigger_ExecutesTriggerCorrectly()
        {
            // Arrange
            var queryExecutor = new SqlQueryExecutor(CreateConnection(), _mockLogger.Object);
            var deleteQuery = @"
                DELETE FROM Employees 
                WHERE EmployeeNumber = @EmployeeNumber";

            var parameters = new Dictionary<string, object>
            {
                ["EmployeeNumber"] = "TEST001"
            };

            // Act
            var result = await queryExecutor.ExecuteNonQueryAsync(deleteQuery, parameters);

            // Assert
            resultAssert.IsTrue(result > 0, "Delete should succeed");
            
            // Verify trigger executed (e.g., audit record created)
            var auditQuery = "SELECT COUNT(*) FROM EmployeeAuditLog WHERE EmployeeNumber = 'TEST001' AND Action = 'DELETE'";
            var auditCount = await queryExecutor.ExecuteScalarAsync<int>(auditQuery);
            auditCountAssert.IsTrue(result > 0, "Delete audit trigger should have executed");
        }

        #endregion

        #region Advanced Data Type Tests

        /// <summary>
        /// Tests XML data type operations
        /// </summary>
        [TestMethod]
        public async Task ExecuteQuery_WithXMLDataType_ProcessesCorrectly()
        {
            // Arrange
            var queryExecutor = new SqlQueryExecutor(CreateConnection(), _mockLogger.Object);
            var xmlQuery = @"
                SELECT 
                    EmployeeID,
                    EmployeeNumber,
                    EmployeeData.value('(/Employee/FirstName)[1]', 'NVARCHAR(50)') as FirstName,
                    EmployeeData.value('(/Employee/LastName)[1]', 'NVARCHAR(50)') as LastName,
                    EmployeeData.value('(/Employee/Department)[1]', 'NVARCHAR(100)') as Department
                FROM EmployeeXMLData 
                WHERE EmployeeData.exist('/Employee[@Active=""true""]') = 1";

            // Act
            var result = await queryExecutor.ExecuteQueryAsync<Dictionary<string, object>>(xmlQuery);

            // Assert
            resultAssert.IsNotNull("XML query should return results");
        }

        /// <summary>
        /// Tests JSON data type operations
        /// </summary>
        [TestMethod]
        public async Task ExecuteQuery_WithJSONDataType_ProcessesCorrectly()
        {
            // Arrange
            var queryExecutor = new SqlQueryExecutor(CreateConnection(), _mockLogger.Object);
            var jsonQuery = @"
                SELECT 
                    EmployeeID,
                    EmployeeNumber,
                    JSON_VALUE(EmployeeData, '$.FirstName') as FirstName,
                    JSON_VALUE(EmployeeData, '$.LastName') as LastName,
                    JSON_VALUE(EmployeeData, '$.Department') as Department,
                    JSON_QUERY(EmployeeData, '$.Skills') as Skills
                FROM EmployeeJSONData 
                WHERE JSON_VALUE(EmployeeData, '$.Active') = 'true'";

            // Act
            var result = await queryExecutor.ExecuteQueryAsync<Dictionary<string, object>>(jsonQuery);

            // Assert
            resultAssert.IsNotNull("JSON query should return results");
        }

        /// <summary>
        /// Tests Geography data type operations
        /// </summary>
        [TestMethod]
        public async Task ExecuteQuery_WithGeographyDataType_ProcessesCorrectly()
        {
            // Arrange
            var queryExecutor = new SqlQueryExecutor(CreateConnection(), _mockLogger.Object);
            var geographyQuery = @"
                SELECT 
                    EmployeeID,
                    EmployeeNumber,
                    Location.STAsText() as LocationText,
                    Location.STDistance(@OfficeLocation) as DistanceFromOffice
                FROM EmployeeLocations 
                WHERE Location.STDistance(@OfficeLocation) <= @MaxDistance";

            var parameters = new Dictionary<string, object>
            {
                ["OfficeLocation"] = "POINT(-122.4194 37.7749)", // San Francisco
                ["MaxDistance"] = 10000 // 10km in meters
            };

            // Act
            var result = await queryExecutor.ExecuteQueryAsync<Dictionary<string, object>>(geographyQuery, parameters);

            // Assert
            resultAssert.IsNotNull("Geography query should return results");
        }

        #endregion

        #region Advanced Query Features Tests

        /// <summary>
        /// Tests MERGE operations
        /// </summary>
        [TestMethod]
        public async Task ExecuteMerge_WithUpsertLogic_ProcessesCorrectly()
        {
            // Arrange
            var queryExecutor = new SqlQueryExecutor(CreateConnection(), _mockLogger.Object);
            var mergeQuery = @"
                MERGE EmployeeCompensation AS target
                USING (SELECT @EmployeeID as EmployeeID, @BaseSalary as BaseSalary, @EffectiveDate as EffectiveDate) AS source
                ON (target.EmployeeID = source.EmployeeID AND target.EffectiveDate = source.EffectiveDate)
                WHEN MATCHED THEN
                    UPDATE SET 
                        BaseSalary = source.BaseSalary,
                        ModifiedDate = GETDATE(),
                        ModifiedBy = @ModifiedBy
                WHEN NOT MATCHED THEN
                    INSERT (EmployeeID, BaseSalary, EffectiveDate, CreatedDate, CreatedBy)
                    VALUES (source.EmployeeID, source.BaseSalary, source.EffectiveDate, GETDATE(), @CreatedBy);";

            var parameters = new Dictionary<string, object>
            {
                ["EmployeeID"] = 1,
                ["BaseSalary"] = 75000,
                ["EffectiveDate"] = new DateTime(2024, 1, 1),
                ["ModifiedBy"] = "TEST_USER",
                ["CreatedBy"] = "TEST_USER"
            };

            // Act
            var result = await queryExecutor.ExecuteNonQueryAsync(mergeQuery, parameters);

            // Assert
            resultAssert.IsTrue(result > 0, "MERGE operation should succeed");
        }

        /// <summary>
        /// Tests PIVOT operations
        /// </summary>
        [TestMethod]
        public async Task ExecutePivot_WithEmployeeData_ReturnsPivotedResults()
        {
            // Arrange
            var queryExecutor = new SqlQueryExecutor(CreateConnection(), _mockLogger.Object);
            var pivotQuery = @"
                SELECT 
                    DepartmentID,
                    [2024-01] as January2024,
                    [2024-02] as February2024,
                    [2024-03] as March2024
                FROM (
                    SELECT 
                        DepartmentID,
                        FORMAT(StartDate, 'yyyy-MM') as StartMonth,
                        COUNT(*) as EmployeeCount
                    FROM Employees
                    WHERE StartDate >= '2024-01-01' AND StartDate < '2024-04-01'
                    GROUP BY DepartmentID, FORMAT(StartDate, 'yyyy-MM')
                ) AS SourceData
                PIVOT (
                    SUM(EmployeeCount)
                    FOR StartMonth IN ([2024-01], [2024-02], [2024-03])
                ) AS PivotTable";

            // Act
            var result = await queryExecutor.ExecuteQueryAsync<Dictionary<string, object>>(pivotQuery);

            // Assert
            resultAssert.IsNotNull("PIVOT query should return results");
        }

        /// <summary>
        /// Tests UNPIVOT operations
        /// </summary>
        [TestMethod]
        public async Task ExecuteUnpivot_WithEmployeeData_ReturnsUnpivotedResults()
        {
            // Arrange
            var queryExecutor = new SqlQueryExecutor(CreateConnection(), _mockLogger.Object);
            var unpivotQuery = @"
                SELECT 
                    EmployeeID,
                    AllowanceType,
                    AllowanceAmount
                FROM (
                    SELECT 
                        EmployeeID,
                        HousingAllowance,
                        TransportAllowance,
                        MealAllowance,
                        MedicalAllowance
                    FROM EmployeeCompensation
                    WHERE EmployeeID IN (1, 2, 3)
                ) AS SourceData
                UNPIVOT (
                    AllowanceAmount
                    FOR AllowanceType IN (HousingAllowance, TransportAllowance, MealAllowance, MedicalAllowance)
                ) AS UnpivotTable";

            // Act
            var result = await queryExecutor.ExecuteQueryAsync<Dictionary<string, object>>(unpivotQuery);

            // Assert
            resultAssert.IsNotNull("UNPIVOT query should return results");
        }

        #endregion

        #region Helper Methods

        private IDbConnection CreateConnection()
        {
            var connection = new SqlConnection(_connectionString);
            connection.Open();
            return connection;
        }

        private DataTable CreateEmployeeCompensationTable()
        {
            var table = new DataTable();
            table.Columns.Add("EmployeeID", typeof(int));
            table.Columns.Add("BaseSalary", typeof(decimal));
            table.Columns.Add("EffectiveDate", typeof(DateTime));
            
            // Add sample data
            var row1 = table.NewRow();
            row1["EmployeeID"] = 1;
            row1["BaseSalary"] = 75000;
            row1["EffectiveDate"] = new DateTime(2024, 1, 1);
            table.Rows.Add(row1);
            
            var row2 = table.NewRow();
            row2["EmployeeID"] = 2;
            row2["BaseSalary"] = 80000;
            row2["EffectiveDate"] = new DateTime(2024, 1, 1);
            table.Rows.Add(row2);
            
            return table;
        }

        #endregion
    }
}
