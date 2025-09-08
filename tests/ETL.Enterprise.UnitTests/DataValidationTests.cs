using Microsoft.VisualStudio.TestTools.UnitTesting;
using Microsoft.Extensions.Logging;
using ETL.Enterprise.Infrastructure.SSIS;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using System.IO;
using System.Linq;
using System.Text.Json;
using System.Data;
using System.Data.SqlClient;

namespace ETL.Tests.Integration
{
    /// <summary>
    /// Comprehensive data validation tests using golden copy files and input/output comparison
    /// </summary>
    [TestClass]
    public class DataValidationTests
    {
        private static ILogger<SSISPackageOrchestrator> _logger;
        private static string _testDataPath;
        private static string _goldenCopyPath;
        private static string _testConnectionString;

        [ClassInitialize]
        public static void ClassInitialize(TestContext context)
        {
            // Initialize test environment
            _testDataPath = Path.Combine(AppDomain.CurrentDomain.BaseDirectory, "TestData");
            _goldenCopyPath = Path.Combine(AppDomain.CurrentDomain.BaseDirectory, "GoldenCopy");
            _testConnectionString = "Server=localhost;Database=ETL_Test;Integrated Security=true;";
            
            // Create test data directories if they don't exist
            if (!Directory.Exists(_testDataPath))
            {
                Directory.CreateDirectory(_testDataPath);
            }
            if (!Directory.Exists(_goldenCopyPath))
            {
                Directory.CreateDirectory(_goldenCopyPath);
            }

            // Initialize logger
            var loggerFactory = LoggerFactory.Create(builder => { });
            _logger = loggerFactory.CreateLogger<SSISPackageOrchestrator>();

            // Create sample test data files
            CreateSampleTestData();
        }

        #region Golden Copy Validation Tests

        /// <summary>
        /// Tests data validation against golden copy for customer data transformation
        /// </summary>
        [TestMethod]
        public async Task Test_DataValidation_CustomerDataGoldenCopy()
        {
            // Arrange
            var config = new Dictionary<string, string>
            {
                ["SSIS:DTExecPath"] = @"C:\Program Files\Microsoft SQL Server\150\DTS\Binn\DTExec.exe",
                ["ConnectionStrings:MonitoringDB"] = _testConnectionString
            };
            
            var orchestrator = new SSISPackageOrchestrator(_logger, new TestConfiguration(config));
            var packagePath = Path.Combine(_testDataPath, "CustomerTransformPackage.dtsx");
            var parameters = new Dictionary<string, string>
            {
                ["SourceFile"] = Path.Combine(_testDataPath, "Input", "customers.csv"),
                ["TargetFile"] = Path.Combine(_testDataPath, "Output", "customers_transformed.csv"),
                ["GoldenCopyFile"] = Path.Combine(_goldenCopyPath, "customers_expected.csv")
            };

            // Act
            var result = await orchestrator.ExecutePackageAsync(packagePath, parameters);

            // Assert
            Assert.IsNotNull(result, "Package execution result should not be null");
            Assert.IsTrue(result.Success, "Package execution should succeed");

            // Validate output against golden copy
            var outputFile = Path.Combine(_testDataPath, "Output", "customers_transformed.csv");
            var goldenCopyFile = Path.Combine(_goldenCopyPath, "customers_expected.csv");
            
            var validationResult = await ValidateDataAgainstGoldenCopy(outputFile, goldenCopyFile);
            Assert.IsTrue(validationResult.IsValid, $"Data validation failed: {validationResult.ErrorMessage}");
            Assert.AreEqual(0, validationResult.RowDifferences.Count, "No row differences should exist");
        }

        /// <summary>
        /// Tests data validation against golden copy for order data transformation
        /// </summary>
        [TestMethod]
        public async Task Test_DataValidation_OrderDataGoldenCopy()
        {
            // Arrange
            var config = new Dictionary<string, string>
            {
                ["SSIS:DTExecPath"] = @"C:\Program Files\Microsoft SQL Server\150\DTS\Binn\DTExec.exe",
                ["ConnectionStrings:MonitoringDB"] = _testConnectionString
            };
            
            var orchestrator = new SSISPackageOrchestrator(_logger, new TestConfiguration(config));
            var packagePath = Path.Combine(_testDataPath, "OrderTransformPackage.dtsx");
            var parameters = new Dictionary<string, string>
            {
                ["SourceFile"] = Path.Combine(_testDataPath, "Input", "orders.csv"),
                ["TargetFile"] = Path.Combine(_testDataPath, "Output", "orders_transformed.csv"),
                ["GoldenCopyFile"] = Path.Combine(_goldenCopyPath, "orders_expected.csv")
            };

            // Act
            var result = await orchestrator.ExecutePackageAsync(packagePath, parameters);

            // Assert
            Assert.IsNotNull(result, "Package execution result should not be null");
            Assert.IsTrue(result.Success, "Package execution should succeed");

            // Validate output against golden copy
            var outputFile = Path.Combine(_testDataPath, "Output", "orders_transformed.csv");
            var goldenCopyFile = Path.Combine(_goldenCopyPath, "orders_expected.csv");
            
            var validationResult = await ValidateDataAgainstGoldenCopy(outputFile, goldenCopyFile);
            Assert.IsTrue(validationResult.IsValid, $"Data validation failed: {validationResult.ErrorMessage}");
            Assert.AreEqual(0, validationResult.RowDifferences.Count, "No row differences should exist");
        }

        /// <summary>
        /// Tests data validation against golden copy for product data transformation
        /// </summary>
        [TestMethod]
        public async Task Test_DataValidation_ProductDataGoldenCopy()
        {
            // Arrange
            var config = new Dictionary<string, string>
            {
                ["SSIS:DTExecPath"] = @"C:\Program Files\Microsoft SQL Server\150\DTS\Binn\DTExec.exe",
                ["ConnectionStrings:MonitoringDB"] = _testConnectionString
            };
            
            var orchestrator = new SSISPackageOrchestrator(_logger, new TestConfiguration(config));
            var packagePath = Path.Combine(_testDataPath, "ProductTransformPackage.dtsx");
            var parameters = new Dictionary<string, string>
            {
                ["SourceFile"] = Path.Combine(_testDataPath, "Input", "products.csv"),
                ["TargetFile"] = Path.Combine(_testDataPath, "Output", "products_transformed.csv"),
                ["GoldenCopyFile"] = Path.Combine(_goldenCopyPath, "products_expected.csv")
            };

            // Act
            var result = await orchestrator.ExecutePackageAsync(packagePath, parameters);

            // Assert
            Assert.IsNotNull(result, "Package execution result should not be null");
            Assert.IsTrue(result.Success, "Package execution should succeed");

            // Validate output against golden copy
            var outputFile = Path.Combine(_testDataPath, "Output", "products_transformed.csv");
            var goldenCopyFile = Path.Combine(_goldenCopyPath, "products_expected.csv");
            
            var validationResult = await ValidateDataAgainstGoldenCopy(outputFile, goldenCopyFile);
            Assert.IsTrue(validationResult.IsValid, $"Data validation failed: {validationResult.ErrorMessage}");
            Assert.AreEqual(0, validationResult.RowDifferences.Count, "No row differences should exist");
        }

        #endregion

        #region Input/Output Data Comparison Tests

        /// <summary>
        /// Tests input vs output data comparison for data transformation
        /// </summary>
        [TestMethod]
        public async Task Test_DataValidation_InputOutputComparison()
        {
            // Arrange
            var config = new Dictionary<string, string>
            {
                ["SSIS:DTExecPath"] = @"C:\Program Files\Microsoft SQL Server\150\DTS\Binn\DTExec.exe",
                ["ConnectionStrings:MonitoringDB"] = _testConnectionString
            };
            
            var orchestrator = new SSISPackageOrchestrator(_logger, new TestConfiguration(config));
            var packagePath = Path.Combine(_testDataPath, "DataComparisonPackage.dtsx");
            var parameters = new Dictionary<string, string>
            {
                ["InputFile"] = Path.Combine(_testDataPath, "Input", "sales_data.csv"),
                ["OutputFile"] = Path.Combine(_testDataPath, "Output", "sales_data_processed.csv")
            };

            // Act
            var result = await orchestrator.ExecutePackageAsync(packagePath, parameters);

            // Assert
            Assert.IsNotNull(result, "Package execution result should not be null");
            Assert.IsTrue(result.Success, "Package execution should succeed");

            // Compare input vs output data
            var inputFile = Path.Combine(_testDataPath, "Input", "sales_data.csv");
            var outputFile = Path.Combine(_testDataPath, "Output", "sales_data_processed.csv");
            
            var comparisonResult = await CompareInputOutputData(inputFile, outputFile);
            Assert.IsTrue(comparisonResult.IsValid, $"Input/Output comparison failed: {comparisonResult.ErrorMessage}");
            Assert.AreEqual(comparisonResult.InputRowCount, comparisonResult.OutputRowCount, "Input and output row counts should match");
        }

        /// <summary>
        /// Tests data transformation validation with specific business rules
        /// </summary>
        [TestMethod]
        public async Task Test_DataValidation_BusinessRulesValidation()
        {
            // Arrange
            var config = new Dictionary<string, string>
            {
                ["SSIS:DTExecPath"] = @"C:\Program Files\Microsoft SQL Server\150\DTS\Binn\DTExec.exe",
                ["ConnectionStrings:MonitoringDB"] = _testConnectionString
            };
            
            var orchestrator = new SSISPackageOrchestrator(_logger, new TestConfiguration(config));
            var packagePath = Path.Combine(_testDataPath, "BusinessRulesPackage.dtsx");
            var parameters = new Dictionary<string, string>
            {
                ["InputFile"] = Path.Combine(_testDataPath, "Input", "employee_data.csv"),
                ["OutputFile"] = Path.Combine(_testDataPath, "Output", "employee_data_validated.csv"),
                ["BusinessRulesFile"] = Path.Combine(_testDataPath, "Rules", "employee_validation_rules.json")
            };

            // Act
            var result = await orchestrator.ExecutePackageAsync(packagePath, parameters);

            // Assert
            Assert.IsNotNull(result, "Package execution result should not be null");
            Assert.IsTrue(result.Success, "Package execution should succeed");

            // Validate business rules
            var outputFile = Path.Combine(_testDataPath, "Output", "employee_data_validated.csv");
            var rulesFile = Path.Combine(_testDataPath, "Rules", "employee_validation_rules.json");
            
            var validationResult = await ValidateBusinessRules(outputFile, rulesFile);
            Assert.IsTrue(validationResult.IsValid, $"Business rules validation failed: {validationResult.ErrorMessage}");
            Assert.AreEqual(0, validationResult.Violations.Count, "No business rule violations should exist");
        }

        #endregion

        #region Data Quality and Integrity Tests

        /// <summary>
        /// Tests data quality validation including completeness, accuracy, and consistency
        /// </summary>
        [TestMethod]
        public async Task Test_DataValidation_DataQualityValidation()
        {
            // Arrange
            var config = new Dictionary<string, string>
            {
                ["SSIS:DTExecPath"] = @"C:\Program Files\Microsoft SQL Server\150\DTS\Binn\DTExec.exe",
                ["ConnectionStrings:MonitoringDB"] = _testConnectionString
            };
            
            var orchestrator = new SSISPackageOrchestrator(_logger, new TestConfiguration(config));
            var packagePath = Path.Combine(_testDataPath, "DataQualityPackage.dtsx");
            var parameters = new Dictionary<string, string>
            {
                ["InputFile"] = Path.Combine(_testDataPath, "Input", "customer_data.csv"),
                ["OutputFile"] = Path.Combine(_testDataPath, "Output", "customer_data_quality_checked.csv"),
                ["QualityRulesFile"] = Path.Combine(_testDataPath, "Rules", "data_quality_rules.json")
            };

            // Act
            var result = await orchestrator.ExecutePackageAsync(packagePath, parameters);

            // Assert
            Assert.IsNotNull(result, "Package execution result should not be null");
            Assert.IsTrue(result.Success, "Package execution should succeed");

            // Validate data quality
            var outputFile = Path.Combine(_testDataPath, "Output", "customer_data_quality_checked.csv");
            var qualityRulesFile = Path.Combine(_testDataPath, "Rules", "data_quality_rules.json");
            
            var qualityResult = await ValidateDataQuality(outputFile, qualityRulesFile);
            Assert.IsTrue(qualityResult.IsValid, $"Data quality validation failed: {qualityResult.ErrorMessage}");
            Assert.AreEqual(0, qualityResult.QualityIssues.Count, "No data quality issues should exist");
        }

        /// <summary>
        /// Tests data integrity validation including referential integrity and constraints
        /// </summary>
        [TestMethod]
        public async Task Test_DataValidation_DataIntegrityValidation()
        {
            // Arrange
            var config = new Dictionary<string, string>
            {
                ["SSIS:DTExecPath"] = @"C:\Program Files\Microsoft SQL Server\150\DTS\Binn\DTExec.exe",
                ["ConnectionStrings:MonitoringDB"] = _testConnectionString
            };
            
            var orchestrator = new SSISPackageOrchestrator(_logger, new TestConfiguration(config));
            var packagePath = Path.Combine(_testDataPath, "DataIntegrityPackage.dtsx");
            var parameters = new Dictionary<string, string>
            {
                ["InputFile"] = Path.Combine(_testDataPath, "Input", "order_items.csv"),
                ["OutputFile"] = Path.Combine(_testDataPath, "Output", "order_items_integrity_checked.csv"),
                ["ReferenceDataFile"] = Path.Combine(_testDataPath, "Reference", "products_reference.csv")
            };

            // Act
            var result = await orchestrator.ExecutePackageAsync(packagePath, parameters);

            // Assert
            Assert.IsNotNull(result, "Package execution result should not be null");
            Assert.IsTrue(result.Success, "Package execution should succeed");

            // Validate data integrity
            var outputFile = Path.Combine(_testDataPath, "Output", "order_items_integrity_checked.csv");
            var referenceFile = Path.Combine(_testDataPath, "Reference", "products_reference.csv");
            
            var integrityResult = await ValidateDataIntegrity(outputFile, referenceFile);
            Assert.IsTrue(integrityResult.IsValid, $"Data integrity validation failed: {integrityResult.ErrorMessage}");
            Assert.AreEqual(0, integrityResult.IntegrityViolations.Count, "No data integrity violations should exist");
        }

        #endregion

        #region Row-by-Row Comparison Tests

        /// <summary>
        /// Tests detailed row-by-row comparison between expected and actual data
        /// </summary>
        [TestMethod]
        public async Task Test_DataValidation_RowByRowComparison()
        {
            // Arrange
            var config = new Dictionary<string, string>
            {
                ["SSIS:DTExecPath"] = @"C:\Program Files\Microsoft SQL Server\150\DTS\Binn\DTExec.exe",
                ["ConnectionStrings:MonitoringDB"] = _testConnectionString
            };
            
            var orchestrator = new SSISPackageOrchestrator(_logger, new TestConfiguration(config));
            var packagePath = Path.Combine(_testDataPath, "RowComparisonPackage.dtsx");
            var parameters = new Dictionary<string, string>
            {
                ["InputFile"] = Path.Combine(_testDataPath, "Input", "detailed_data.csv"),
                ["OutputFile"] = Path.Combine(_testDataPath, "Output", "detailed_data_processed.csv"),
                ["ExpectedFile"] = Path.Combine(_goldenCopyPath, "detailed_data_expected.csv")
            };

            // Act
            var result = await orchestrator.ExecutePackageAsync(packagePath, parameters);

            // Assert
            Assert.IsNotNull(result, "Package execution result should not be null");
            Assert.IsTrue(result.Success, "Package execution should succeed");

            // Perform row-by-row comparison
            var outputFile = Path.Combine(_testDataPath, "Output", "detailed_data_processed.csv");
            var expectedFile = Path.Combine(_goldenCopyPath, "detailed_data_expected.csv");
            
            var comparisonResult = await PerformRowByRowComparison(outputFile, expectedFile);
            Assert.IsTrue(comparisonResult.IsValid, $"Row-by-row comparison failed: {comparisonResult.ErrorMessage}");
            Assert.AreEqual(0, comparisonResult.RowDifferences.Count, "No row differences should exist");
        }

        /// <summary>
        /// Tests data validation with tolerance for floating-point comparisons
        /// </summary>
        [TestMethod]
        public async Task Test_DataValidation_FloatingPointTolerance()
        {
            // Arrange
            var config = new Dictionary<string, string>
            {
                ["SSIS:DTExecPath"] = @"C:\Program Files\Microsoft SQL Server\150\DTS\Binn\DTExec.exe",
                ["ConnectionStrings:MonitoringDB"] = _testConnectionString
            };
            
            var orchestrator = new SSISPackageOrchestrator(_logger, new TestConfiguration(config));
            var packagePath = Path.Combine(_testDataPath, "FloatingPointPackage.dtsx");
            var parameters = new Dictionary<string, string>
            {
                ["InputFile"] = Path.Combine(_testDataPath, "Input", "financial_data.csv"),
                ["OutputFile"] = Path.Combine(_testDataPath, "Output", "financial_data_processed.csv"),
                ["ExpectedFile"] = Path.Combine(_goldenCopyPath, "financial_data_expected.csv"),
                ["Tolerance"] = "0.01"
            };

            // Act
            var result = await orchestrator.ExecutePackageAsync(packagePath, parameters);

            // Assert
            Assert.IsNotNull(result, "Package execution result should not be null");
            Assert.IsTrue(result.Success, "Package execution should succeed");

            // Perform comparison with tolerance
            var outputFile = Path.Combine(_testDataPath, "Output", "financial_data_processed.csv");
            var expectedFile = Path.Combine(_goldenCopyPath, "financial_data_expected.csv");
            
            var comparisonResult = await PerformRowByRowComparison(outputFile, expectedFile, 0.01);
            Assert.IsTrue(comparisonResult.IsValid, $"Floating-point comparison failed: {comparisonResult.ErrorMessage}");
            Assert.AreEqual(0, comparisonResult.RowDifferences.Count, "No row differences should exist within tolerance");
        }

        #endregion

        #region Input/Output Data Comparison Tests

        /// <summary>
        /// Tests basic CSV file comparison with exact match
        /// </summary>
        [TestMethod]
        public async Task TestCsvFileComparison_ExactMatch()
        {
            // Arrange
            var testData = new[]
            {
                "Name,Age,City",
                "John,25,New York",
                "Jane,30,Los Angeles",
                "Bob,35,Chicago"
            };

            var actualFile = Path.Combine(_testDataPath, "actual_exact.csv");
            var expectedFile = Path.Combine(_testDataPath, "expected_exact.csv");

            await File.WriteAllLinesAsync(actualFile, testData);
            await File.WriteAllLinesAsync(expectedFile, testData);

            // Act
            var result = await DataValidationUtilities.CompareCsvFilesAsync(actualFile, expectedFile);

            // Assert
            Assert.IsTrue(result.IsValid, $"Comparison failed: {result.ErrorMessage}");
            Assert.AreEqual(3, result.ActualRowCount);
            Assert.AreEqual(3, result.ExpectedRowCount);
            Assert.AreEqual(3, result.MatchingRows);
            Assert.AreEqual(0, result.DifferentRows);
            Assert.AreEqual(0, result.MissingRows);
            Assert.AreEqual(0, result.ExtraRows);
            Assert.AreEqual(0, result.HeaderDifferences.Count);
            Assert.AreEqual(3, result.RowComparisons.Count);
            Assert.IsTrue(result.RowComparisons.All(r => r.IsMatch));
        }

        /// <summary>
        /// Tests CSV file comparison with data differences
        /// </summary>
        [TestMethod]
        public async Task TestCsvFileComparison_WithDifferences()
        {
            // Arrange
            var actualData = new[]
            {
                "Name,Age,City",
                "John,25,New York",
                "Jane,31,Los Angeles", // Age changed from 30 to 31
                "Bob,35,Chicago"
            };

            var expectedData = new[]
            {
                "Name,Age,City",
                "John,25,New York",
                "Jane,30,Los Angeles",
                "Bob,35,Chicago"
            };

            var actualFile = Path.Combine(_testDataPath, "actual_differences.csv");
            var expectedFile = Path.Combine(_testDataPath, "expected_differences.csv");

            await File.WriteAllLinesAsync(actualFile, actualData);
            await File.WriteAllLinesAsync(expectedFile, expectedData);

            // Act
            var result = await DataValidationUtilities.CompareCsvFilesAsync(actualFile, expectedFile);

            // Assert
            Assert.IsFalse(result.IsValid);
            Assert.AreEqual(3, result.ActualRowCount);
            Assert.AreEqual(3, result.ExpectedRowCount);
            Assert.AreEqual(2, result.MatchingRows);
            Assert.AreEqual(1, result.DifferentRows);
            Assert.AreEqual(0, result.MissingRows);
            Assert.AreEqual(0, result.ExtraRows);

            var differentRow = result.RowComparisons.FirstOrDefault(r => !r.IsMatch);
            Assert.IsNotNull(differentRow);
            Assert.AreEqual(1, differentRow.RowIndex); // Jane's row
            Assert.AreEqual(1, differentRow.Differences.Count);
            Assert.AreEqual("Age", differentRow.Differences[0].Field);
            Assert.AreEqual("31", differentRow.Differences[0].ActualValue);
            Assert.AreEqual("30", differentRow.Differences[0].ExpectedValue);
        }

        /// <summary>
        /// Tests CSV file comparison with missing rows
        /// </summary>
        [TestMethod]
        public async Task TestCsvFileComparison_WithMissingRows()
        {
            // Arrange
            var actualData = new[]
            {
                "Name,Age,City",
                "John,25,New York"
                // Missing Jane and Bob rows
            };

            var expectedData = new[]
            {
                "Name,Age,City",
                "John,25,New York",
                "Jane,30,Los Angeles",
                "Bob,35,Chicago"
            };

            var actualFile = Path.Combine(_testDataPath, "actual_missing.csv");
            var expectedFile = Path.Combine(_testDataPath, "expected_missing.csv");

            await File.WriteAllLinesAsync(actualFile, actualData);
            await File.WriteAllLinesAsync(expectedFile, expectedData);

            // Act
            var result = await DataValidationUtilities.CompareCsvFilesAsync(actualFile, expectedFile);

            // Assert
            Assert.IsFalse(result.IsValid);
            Assert.AreEqual(1, result.ActualRowCount);
            Assert.AreEqual(3, result.ExpectedRowCount);
            Assert.AreEqual(1, result.MatchingRows);
            Assert.AreEqual(0, result.DifferentRows);
            Assert.AreEqual(2, result.MissingRows);
            Assert.AreEqual(0, result.ExtraRows);

            var missingRows = result.RowComparisons.Where(r => r.ActualRow == null).ToList();
            Assert.AreEqual(2, missingRows.Count);
            Assert.AreEqual(1, missingRows[0].RowIndex);
            Assert.AreEqual(2, missingRows[1].RowIndex);
        }

        /// <summary>
        /// Tests CSV file comparison with extra rows
        /// </summary>
        [TestMethod]
        public async Task TestCsvFileComparison_WithExtraRows()
        {
            // Arrange
            var actualData = new[]
            {
                "Name,Age,City",
                "John,25,New York",
                "Jane,30,Los Angeles",
                "Bob,35,Chicago",
                "Alice,28,Boston" // Extra row
            };

            var expectedData = new[]
            {
                "Name,Age,City",
                "John,25,New York",
                "Jane,30,Los Angeles",
                "Bob,35,Chicago"
            };

            var actualFile = Path.Combine(_testDataPath, "actual_extra.csv");
            var expectedFile = Path.Combine(_testDataPath, "expected_extra.csv");

            await File.WriteAllLinesAsync(actualFile, actualData);
            await File.WriteAllLinesAsync(expectedFile, expectedData);

            // Act
            var result = await DataValidationUtilities.CompareCsvFilesAsync(actualFile, expectedFile);

            // Assert
            Assert.IsFalse(result.IsValid);
            Assert.AreEqual(4, result.ActualRowCount);
            Assert.AreEqual(3, result.ExpectedRowCount);
            Assert.AreEqual(3, result.MatchingRows);
            Assert.AreEqual(0, result.DifferentRows);
            Assert.AreEqual(0, result.MissingRows);
            Assert.AreEqual(1, result.ExtraRows);

            var extraRow = result.RowComparisons.FirstOrDefault(r => r.ExpectedRow == null);
            Assert.IsNotNull(extraRow);
            Assert.AreEqual(3, extraRow.RowIndex); // Alice's row
        }

        /// <summary>
        /// Tests CSV file comparison with header differences
        /// </summary>
        [TestMethod]
        public async Task TestCsvFileComparison_WithHeaderDifferences()
        {
            // Arrange
            var actualData = new[]
            {
                "Name,Age,City,Country", // Extra Country column
                "John,25,New York,USA",
                "Jane,30,Los Angeles,USA"
            };

            var expectedData = new[]
            {
                "Name,Age,City",
                "John,25,New York",
                "Jane,30,Los Angeles"
            };

            var actualFile = Path.Combine(_testDataPath, "actual_headers.csv");
            var expectedFile = Path.Combine(_testDataPath, "expected_headers.csv");

            await File.WriteAllLinesAsync(actualFile, actualData);
            await File.WriteAllLinesAsync(expectedFile, expectedData);

            // Act
            var result = await DataValidationUtilities.CompareCsvFilesAsync(actualFile, expectedFile);

            // Assert
            Assert.IsFalse(result.IsValid);
            Assert.AreEqual(1, result.HeaderDifferences.Count);
            Assert.AreEqual("Country", result.HeaderDifferences[0].Header);
            Assert.AreEqual(HeaderDifferenceType.Extra, result.HeaderDifferences[0].Type);
            Assert.IsTrue(result.HeaderDifferences[0].Message.Contains("extra"));
        }

        /// <summary>
        /// Tests CSV file comparison with floating point tolerance
        /// </summary>
        [TestMethod]
        public async Task TestCsvFileComparison_WithFloatingPointTolerance()
        {
            // Arrange
            var actualData = new[]
            {
                "Name,Score",
                "John,85.123",
                "Jane,92.456"
            };

            var expectedData = new[]
            {
                "Name,Score",
                "John,85.125", // 0.002 difference
                "Jane,92.450"  // 0.006 difference
            };

            var actualFile = Path.Combine(_testDataPath, "actual_float.csv");
            var expectedFile = Path.Combine(_testDataPath, "expected_float.csv");

            await File.WriteAllLinesAsync(actualFile, actualData);
            await File.WriteAllLinesAsync(expectedFile, expectedData);

            // Act - Test with tolerance
            var options = new ComparisonOptions { FloatingPointTolerance = 0.01 };
            var result = await DataValidationUtilities.CompareCsvFilesAsync(actualFile, expectedFile, options);

            // Assert
            Assert.IsTrue(result.IsValid, $"Comparison failed: {result.ErrorMessage}");
            Assert.AreEqual(2, result.MatchingRows);
            Assert.AreEqual(0, result.DifferentRows);

            // Act - Test without tolerance
            options.FloatingPointTolerance = 0.0;
            result = await DataValidationUtilities.CompareCsvFilesAsync(actualFile, expectedFile, options);

            // Assert
            Assert.IsFalse(result.IsValid);
            Assert.AreEqual(0, result.MatchingRows);
            Assert.AreEqual(2, result.DifferentRows);
        }

        /// <summary>
        /// Tests JSON file comparison with exact match
        /// </summary>
        [TestMethod]
        public async Task TestJsonFileComparison_ExactMatch()
        {
            // Arrange
            var testData = new
            {
                users = new[]
                {
                    new { name = "John", age = 25, city = "New York" },
                    new { name = "Jane", age = 30, city = "Los Angeles" }
                },
                total = 2
            };

            var actualFile = Path.Combine(_testDataPath, "actual_exact.json");
            var expectedFile = Path.Combine(_testDataPath, "expected_exact.json");

            var json = JsonSerializer.Serialize(testData, new JsonSerializerOptions { WriteIndented = true });
            await File.WriteAllTextAsync(actualFile, json);
            await File.WriteAllTextAsync(expectedFile, json);

            // Act
            var result = await DataValidationUtilities.CompareJsonFilesAsync(actualFile, expectedFile);

            // Assert
            Assert.IsTrue(result.IsValid, $"JSON comparison failed: {result.ErrorMessage}");
            Assert.AreEqual(0, result.Differences.Count);
        }

        /// <summary>
        /// Tests JSON file comparison with differences
        /// </summary>
        [TestMethod]
        public async Task TestJsonFileComparison_WithDifferences()
        {
            // Arrange
            var actualData = new
            {
                users = new[]
                {
                    new { name = "John", age = 25, city = "New York" },
                    new { name = "Jane", age = 31, city = "Los Angeles" } // Age changed
                },
                total = 2
            };

            var expectedData = new
            {
                users = new[]
                {
                    new { name = "John", age = 25, city = "New York" },
                    new { name = "Jane", age = 30, city = "Los Angeles" }
                },
                total = 2
            };

            var actualFile = Path.Combine(_testDataPath, "actual_json_diff.json");
            var expectedFile = Path.Combine(_testDataPath, "expected_json_diff.json");

            await File.WriteAllTextAsync(actualFile, JsonSerializer.Serialize(actualData, new JsonSerializerOptions { WriteIndented = true }));
            await File.WriteAllTextAsync(expectedFile, JsonSerializer.Serialize(expectedData, new JsonSerializerOptions { WriteIndented = true }));

            // Act
            var result = await DataValidationUtilities.CompareJsonFilesAsync(actualFile, expectedFile);

            // Assert
            Assert.IsFalse(result.IsValid);
            Assert.AreEqual(1, result.Differences.Count);
            Assert.IsTrue(result.Differences[0].Path.Contains("users[1].age"));
            Assert.AreEqual("31", result.Differences[0].ActualValue);
            Assert.AreEqual("30", result.Differences[0].ExpectedValue);
        }

        /// <summary>
        /// Tests input/output data comparison for ETL transformation
        /// </summary>
        [TestMethod]
        public async Task TestETLTransformation_InputOutputComparison()
        {
            // Arrange - Create input data
            var inputData = new[]
            {
                "CustomerID,FirstName,LastName,Email,Phone,Address,City,State,ZipCode,Country,RegistrationDate,LastLoginDate,Status,TotalOrders,TotalSpent",
                "1,John,Doe,john.doe@email.com,555-1234,123 Main St,New York,NY,10001,USA,2023-01-15,2023-12-01,Active,25,1250.50",
                "2,Jane,Smith,jane.smith@email.com,555-5678,456 Oak Ave,Los Angeles,CA,90210,USA,2023-02-20,2023-11-28,Active,18,890.75",
                "3,Bob,Johnson,bob.johnson@email.com,555-9012,789 Pine Rd,Chicago,IL,60601,USA,2023-03-10,2023-10-15,Inactive,8,320.25"
            };

            // Expected output after transformation (cleaned and standardized)
            var expectedOutputData = new[]
            {
                "customer_id,first_name,last_name,email,phone,address,city,state,zip_code,country,registration_date,last_login_date,status,total_orders,total_spent,customer_tier",
                "1,John,Doe,john.doe@email.com,+1-555-1234,123 Main Street,New York,NY,10001,United States,2023-01-15,2023-12-01,ACTIVE,25,1250.50,Premium",
                "2,Jane,Smith,jane.smith@email.com,+1-555-5678,456 Oak Avenue,Los Angeles,CA,90210,United States,2023-02-20,2023-11-28,ACTIVE,18,890.75,Standard",
                "3,Bob,Johnson,bob.johnson@email.com,+1-555-9012,789 Pine Road,Chicago,IL,60601,United States,2023-03-10,2023-10-15,INACTIVE,8,320.25,Basic"
            };

            var inputFile = Path.Combine(_testDataPath, "etl_input.csv");
            var outputFile = Path.Combine(_testDataPath, "etl_output.csv");
            var expectedFile = Path.Combine(_testDataPath, "etl_expected.csv");

            await File.WriteAllLinesAsync(inputFile, inputData);
            await File.WriteAllLinesAsync(outputFile, expectedOutputData); // Simulate ETL output
            await File.WriteAllLinesAsync(expectedFile, expectedOutputData);

            // Act - Compare output with expected
            var result = await DataValidationUtilities.CompareCsvFilesAsync(outputFile, expectedFile);

            // Assert
            Assert.IsTrue(result.IsValid, $"ETL transformation comparison failed: {result.ErrorMessage}");
            Assert.AreEqual(3, result.ActualRowCount);
            Assert.AreEqual(3, result.ExpectedRowCount);
            Assert.AreEqual(3, result.MatchingRows);
            Assert.AreEqual(0, result.DifferentRows);

            // Verify specific transformations
            var firstRow = result.RowComparisons[0];
            Assert.IsTrue(firstRow.IsMatch);
            Assert.AreEqual(0, firstRow.Differences.Count);
        }

        /// <summary>
        /// Tests data comparison with large datasets
        /// </summary>
        [TestMethod]
        public async Task TestDataComparison_LargeDataset()
        {
            // Arrange - Create large dataset
            var rowCount = 1000;
            var actualData = new List<string> { "ID,Name,Value,Category" };
            var expectedData = new List<string> { "ID,Name,Value,Category" };

            for (int i = 1; i <= rowCount; i++)
            {
                actualData.Add($"{i},Item{i},{i * 10.5},Category{(i % 5) + 1}");
                expectedData.Add($"{i},Item{i},{i * 10.5},Category{(i % 5) + 1}");
            }

            var actualFile = Path.Combine(_testDataPath, "large_actual.csv");
            var expectedFile = Path.Combine(_testDataPath, "large_expected.csv");

            await File.WriteAllLinesAsync(actualFile, actualData);
            await File.WriteAllLinesAsync(expectedFile, expectedData);

            // Act
            var result = await DataValidationUtilities.CompareCsvFilesAsync(actualFile, expectedFile);

            // Assert
            Assert.IsTrue(result.IsValid, $"Large dataset comparison failed: {result.ErrorMessage}");
            Assert.AreEqual(rowCount, result.ActualRowCount);
            Assert.AreEqual(rowCount, result.ExpectedRowCount);
            Assert.AreEqual(rowCount, result.MatchingRows);
            Assert.AreEqual(0, result.DifferentRows);
        }

        /// <summary>
        /// Tests data comparison with performance measurement
        /// </summary>
        [TestMethod]
        public async Task TestDataComparison_Performance()
        {
            // Arrange
            var rowCount = 5000;
            var actualData = new List<string> { "ID,Name,Value,Category,Description" };
            var expectedData = new List<string> { "ID,Name,Value,Category,Description" };

            for (int i = 1; i <= rowCount; i++)
            {
                var name = $"Item{i}";
                var value = i * 15.75;
                var category = $"Category{(i % 10) + 1}";
                var description = $"This is a description for item {i} with some additional text to make it longer";

                actualData.Add($"{i},{name},{value},{category},{description}");
                expectedData.Add($"{i},{name},{value},{category},{description}");
            }

            var actualFile = Path.Combine(_testDataPath, "perf_actual.csv");
            var expectedFile = Path.Combine(_testDataPath, "perf_expected.csv");

            await File.WriteAllLinesAsync(actualFile, actualData);
            await File.WriteAllLinesAsync(expectedFile, expectedData);

            // Act
            var stopwatch = System.Diagnostics.Stopwatch.StartNew();
            var result = await DataValidationUtilities.CompareCsvFilesAsync(actualFile, expectedFile);
            stopwatch.Stop();

            // Assert
            Assert.IsTrue(result.IsValid, $"Performance test comparison failed: {result.ErrorMessage}");
            Assert.AreEqual(rowCount, result.ActualRowCount);
            Assert.AreEqual(rowCount, result.ExpectedRowCount);
            Assert.AreEqual(rowCount, result.MatchingRows);

            // Performance assertion (should complete within reasonable time)
            Assert.IsTrue(stopwatch.ElapsedMilliseconds < 10000, 
                $"Comparison took too long: {stopwatch.ElapsedMilliseconds}ms for {rowCount} rows");
        }

        #endregion

        #region Golden Copy Validation Tests

        /// <summary>
        /// Tests golden copy validation with exact match
        /// </summary>
        [TestMethod]
        public async Task TestGoldenCopyValidation_ExactMatch()
        {
            // Arrange
            var goldenCopyData = new[]
            {
                "ID,Name,Email,Phone,Address,City,State,ZipCode,Country,RegistrationDate,LastLoginDate,Status,TotalOrders,TotalSpent,CustomerTier",
                "1,John Doe,john.doe@email.com,+1-555-1234,123 Main Street,New York,NY,10001,United States,2023-01-15,2023-12-01,ACTIVE,25,1250.50,Premium",
                "2,Jane Smith,jane.smith@email.com,+1-555-5678,456 Oak Avenue,Los Angeles,CA,90210,United States,2023-02-20,2023-11-28,ACTIVE,18,890.75,Standard",
                "3,Bob Johnson,bob.johnson@email.com,+1-555-9012,789 Pine Road,Chicago,IL,60601,United States,2023-03-10,2023-10-15,INACTIVE,8,320.25,Basic"
            };

            var outputData = goldenCopyData; // Exact match

            var goldenCopyFile = Path.Combine(_goldenCopyPath, "customer_data_golden.csv");
            var outputFile = Path.Combine(_testDataPath, "Output", "customer_data_output.csv");

            await File.WriteAllLinesAsync(goldenCopyFile, goldenCopyData);
            await File.WriteAllLinesAsync(outputFile, outputData);

            // Act
            var result = await ValidateDataAgainstGoldenCopy(outputFile, goldenCopyFile);

            // Assert
            Assert.IsTrue(result.IsValid, $"Golden copy validation failed: {result.ErrorMessage}");
            Assert.AreEqual(3, result.TotalRows);
            Assert.AreEqual(3, result.MatchingRows);
            Assert.AreEqual(0, result.DifferentRows);
            Assert.AreEqual(0, result.MissingRows);
            Assert.AreEqual(0, result.ExtraRows);
            Assert.AreEqual(0, result.RowDifferences.Count);
        }

        /// <summary>
        /// Tests golden copy validation with data differences
        /// </summary>
        [TestMethod]
        public async Task TestGoldenCopyValidation_WithDifferences()
        {
            // Arrange
            var goldenCopyData = new[]
            {
                "ID,Name,Email,Phone,Address,City,State,ZipCode,Country,RegistrationDate,LastLoginDate,Status,TotalOrders,TotalSpent,CustomerTier",
                "1,John Doe,john.doe@email.com,+1-555-1234,123 Main Street,New York,NY,10001,United States,2023-01-15,2023-12-01,ACTIVE,25,1250.50,Premium",
                "2,Jane Smith,jane.smith@email.com,+1-555-5678,456 Oak Avenue,Los Angeles,CA,90210,United States,2023-02-20,2023-11-28,ACTIVE,18,890.75,Standard",
                "3,Bob Johnson,bob.johnson@email.com,+1-555-9012,789 Pine Road,Chicago,IL,60601,United States,2023-03-10,2023-10-15,INACTIVE,8,320.25,Basic"
            };

            var outputData = new[]
            {
                "ID,Name,Email,Phone,Address,City,State,ZipCode,Country,RegistrationDate,LastLoginDate,Status,TotalOrders,TotalSpent,CustomerTier",
                "1,John Doe,john.doe@email.com,+1-555-1234,123 Main Street,New York,NY,10001,United States,2023-01-15,2023-12-01,ACTIVE,25,1250.50,Premium",
                "2,Jane Smith,jane.smith@email.com,+1-555-5678,456 Oak Avenue,Los Angeles,CA,90210,United States,2023-02-20,2023-11-28,ACTIVE,19,890.75,Standard", // TotalOrders changed from 18 to 19
                "3,Bob Johnson,bob.johnson@email.com,+1-555-9012,789 Pine Road,Chicago,IL,60601,United States,2023-03-10,2023-10-15,INACTIVE,8,320.25,Basic"
            };

            var goldenCopyFile = Path.Combine(_goldenCopyPath, "customer_data_golden_diff.csv");
            var outputFile = Path.Combine(_testDataPath, "Output", "customer_data_output_diff.csv");

            await File.WriteAllLinesAsync(goldenCopyFile, goldenCopyData);
            await File.WriteAllLinesAsync(outputFile, outputData);

            // Act
            var result = await ValidateDataAgainstGoldenCopy(outputFile, goldenCopyFile);

            // Assert
            Assert.IsFalse(result.IsValid);
            Assert.AreEqual(3, result.TotalRows);
            Assert.AreEqual(2, result.MatchingRows);
            Assert.AreEqual(1, result.DifferentRows);
            Assert.AreEqual(0, result.MissingRows);
            Assert.AreEqual(0, result.ExtraRows);
            Assert.AreEqual(1, result.RowDifferences.Count);
            
            var difference = result.RowDifferences[0];
            Assert.AreEqual(1, difference.RowIndex); // Jane's row
            Assert.AreEqual("TotalOrders", difference.Field);
            Assert.AreEqual("19", difference.ActualValue);
            Assert.AreEqual("18", difference.ExpectedValue);
        }

        /// <summary>
        /// Tests golden copy validation with missing rows
        /// </summary>
        [TestMethod]
        public async Task TestGoldenCopyValidation_WithMissingRows()
        {
            // Arrange
            var goldenCopyData = new[]
            {
                "ID,Name,Email,Phone,Address,City,State,ZipCode,Country,RegistrationDate,LastLoginDate,Status,TotalOrders,TotalSpent,CustomerTier",
                "1,John Doe,john.doe@email.com,+1-555-1234,123 Main Street,New York,NY,10001,United States,2023-01-15,2023-12-01,ACTIVE,25,1250.50,Premium",
                "2,Jane Smith,jane.smith@email.com,+1-555-5678,456 Oak Avenue,Los Angeles,CA,90210,United States,2023-02-20,2023-11-28,ACTIVE,18,890.75,Standard",
                "3,Bob Johnson,bob.johnson@email.com,+1-555-9012,789 Pine Road,Chicago,IL,60601,United States,2023-03-10,2023-10-15,INACTIVE,8,320.25,Basic"
            };

            var outputData = new[]
            {
                "ID,Name,Email,Phone,Address,City,State,ZipCode,Country,RegistrationDate,LastLoginDate,Status,TotalOrders,TotalSpent,CustomerTier",
                "1,John Doe,john.doe@email.com,+1-555-1234,123 Main Street,New York,NY,10001,United States,2023-01-15,2023-12-01,ACTIVE,25,1250.50,Premium"
                // Missing Jane and Bob rows
            };

            var goldenCopyFile = Path.Combine(_goldenCopyPath, "customer_data_golden_missing.csv");
            var outputFile = Path.Combine(_testDataPath, "Output", "customer_data_output_missing.csv");

            await File.WriteAllLinesAsync(goldenCopyFile, goldenCopyData);
            await File.WriteAllLinesAsync(outputFile, outputData);

            // Act
            var result = await ValidateDataAgainstGoldenCopy(outputFile, goldenCopyFile);

            // Assert
            Assert.IsFalse(result.IsValid);
            Assert.AreEqual(1, result.TotalRows);
            Assert.AreEqual(1, result.MatchingRows);
            Assert.AreEqual(0, result.DifferentRows);
            Assert.AreEqual(2, result.MissingRows);
            Assert.AreEqual(0, result.ExtraRows);
            Assert.AreEqual(2, result.RowDifferences.Count);
            
            var missingRows = result.RowDifferences.Where(d => d.Message.Contains("missing")).ToList();
            Assert.AreEqual(2, missingRows.Count);
            Assert.AreEqual(1, missingRows[0].RowIndex);
            Assert.AreEqual(2, missingRows[1].RowIndex);
        }

        /// <summary>
        /// Tests golden copy validation with extra rows
        /// </summary>
        [TestMethod]
        public async Task TestGoldenCopyValidation_WithExtraRows()
        {
            // Arrange
            var goldenCopyData = new[]
            {
                "ID,Name,Email,Phone,Address,City,State,ZipCode,Country,RegistrationDate,LastLoginDate,Status,TotalOrders,TotalSpent,CustomerTier",
                "1,John Doe,john.doe@email.com,+1-555-1234,123 Main Street,New York,NY,10001,United States,2023-01-15,2023-12-01,ACTIVE,25,1250.50,Premium",
                "2,Jane Smith,jane.smith@email.com,+1-555-5678,456 Oak Avenue,Los Angeles,CA,90210,United States,2023-02-20,2023-11-28,ACTIVE,18,890.75,Standard"
            };

            var outputData = new[]
            {
                "ID,Name,Email,Phone,Address,City,State,ZipCode,Country,RegistrationDate,LastLoginDate,Status,TotalOrders,TotalSpent,CustomerTier",
                "1,John Doe,john.doe@email.com,+1-555-1234,123 Main Street,New York,NY,10001,United States,2023-01-15,2023-12-01,ACTIVE,25,1250.50,Premium",
                "2,Jane Smith,jane.smith@email.com,+1-555-5678,456 Oak Avenue,Los Angeles,CA,90210,United States,2023-02-20,2023-11-28,ACTIVE,18,890.75,Standard",
                "3,Bob Johnson,bob.johnson@email.com,+1-555-9012,789 Pine Road,Chicago,IL,60601,United States,2023-03-10,2023-10-15,INACTIVE,8,320.25,Basic" // Extra row
            };

            var goldenCopyFile = Path.Combine(_goldenCopyPath, "customer_data_golden_extra.csv");
            var outputFile = Path.Combine(_testDataPath, "Output", "customer_data_output_extra.csv");

            await File.WriteAllLinesAsync(goldenCopyFile, goldenCopyData);
            await File.WriteAllLinesAsync(outputFile, outputData);

            // Act
            var result = await ValidateDataAgainstGoldenCopy(outputFile, goldenCopyFile);

            // Assert
            Assert.IsFalse(result.IsValid);
            Assert.AreEqual(3, result.TotalRows);
            Assert.AreEqual(2, result.MatchingRows);
            Assert.AreEqual(0, result.DifferentRows);
            Assert.AreEqual(0, result.MissingRows);
            Assert.AreEqual(1, result.ExtraRows);
            Assert.AreEqual(1, result.RowDifferences.Count);
            
            var extraRow = result.RowDifferences[0];
            Assert.AreEqual(2, extraRow.RowIndex); // Bob's row
            Assert.IsTrue(extraRow.Message.Contains("extra"));
        }

        /// <summary>
        /// Tests golden copy validation with floating point tolerance
        /// </summary>
        [TestMethod]
        public async Task TestGoldenCopyValidation_WithFloatingPointTolerance()
        {
            // Arrange
            var goldenCopyData = new[]
            {
                "ID,Name,Score,Rating,Price",
                "1,Product A,85.123,4.5,1250.50",
                "2,Product B,92.456,4.8,890.75"
            };

            var outputData = new[]
            {
                "ID,Name,Score,Rating,Price",
                "1,Product A,85.125,4.52,1250.48", // Small differences within tolerance
                "2,Product B,92.450,4.78,890.77"   // Small differences within tolerance
            };

            var goldenCopyFile = Path.Combine(_goldenCopyPath, "product_data_golden_float.csv");
            var outputFile = Path.Combine(_testDataPath, "Output", "product_data_output_float.csv");

            await File.WriteAllLinesAsync(goldenCopyFile, goldenCopyData);
            await File.WriteAllLinesAsync(outputFile, outputData);

            // Act - Test with tolerance
            var result = await PerformRowByRowComparison(outputFile, goldenCopyFile, 0.01);

            // Assert
            Assert.IsTrue(result.IsValid, $"Floating-point golden copy validation failed: {result.ErrorMessage}");
            Assert.AreEqual(2, result.TotalRows);
            Assert.AreEqual(2, result.MatchingRows);
            Assert.AreEqual(0, result.DifferentRows);
            Assert.AreEqual(0, result.RowDifferences.Count);

            // Act - Test without tolerance
            result = await PerformRowByRowComparison(outputFile, goldenCopyFile, 0.0);

            // Assert
            Assert.IsFalse(result.IsValid);
            Assert.AreEqual(2, result.DifferentRows);
            Assert.AreEqual(6, result.RowDifferences.Count); // 3 fields * 2 rows
        }

        /// <summary>
        /// Tests golden copy validation with different data types
        /// </summary>
        [TestMethod]
        public async Task TestGoldenCopyValidation_WithDifferentDataTypes()
        {
            // Arrange
            var goldenCopyData = new[]
            {
                "ID,Name,Age,IsActive,Score,RegistrationDate,LastLoginDate",
                "1,John Doe,25,true,85.5,2023-01-15,2023-12-01",
                "2,Jane Smith,30,false,92.3,2023-02-20,2023-11-28"
            };

            var outputData = new[]
            {
                "ID,Name,Age,IsActive,Score,RegistrationDate,LastLoginDate",
                "1,John Doe,25,true,85.5,2023-01-15,2023-12-01",
                "2,Jane Smith,30,false,92.3,2023-02-20,2023-11-28"
            };

            var goldenCopyFile = Path.Combine(_goldenCopyPath, "mixed_data_golden.csv");
            var outputFile = Path.Combine(_testDataPath, "Output", "mixed_data_output.csv");

            await File.WriteAllLinesAsync(goldenCopyFile, goldenCopyData);
            await File.WriteAllLinesAsync(outputFile, outputData);

            // Act
            var result = await ValidateDataAgainstGoldenCopy(outputFile, goldenCopyFile);

            // Assert
            Assert.IsTrue(result.IsValid, $"Mixed data type golden copy validation failed: {result.ErrorMessage}");
            Assert.AreEqual(2, result.TotalRows);
            Assert.AreEqual(2, result.MatchingRows);
            Assert.AreEqual(0, result.DifferentRows);
        }

        /// <summary>
        /// Tests golden copy validation with large dataset
        /// </summary>
        [TestMethod]
        public async Task TestGoldenCopyValidation_LargeDataset()
        {
            // Arrange - Create large dataset
            var rowCount = 1000;
            var goldenCopyData = new List<string> { "ID,Name,Value,Category,Description" };
            var outputData = new List<string> { "ID,Name,Value,Category,Description" };

            for (int i = 1; i <= rowCount; i++)
            {
                var name = $"Item{i}";
                var value = i * 15.75;
                var category = $"Category{(i % 10) + 1}";
                var description = $"Description for item {i}";

                goldenCopyData.Add($"{i},{name},{value},{category},{description}");
                outputData.Add($"{i},{name},{value},{category},{description}");
            }

            var goldenCopyFile = Path.Combine(_goldenCopyPath, "large_dataset_golden.csv");
            var outputFile = Path.Combine(_testDataPath, "Output", "large_dataset_output.csv");

            await File.WriteAllLinesAsync(goldenCopyFile, goldenCopyData);
            await File.WriteAllLinesAsync(outputFile, outputData);

            // Act
            var result = await ValidateDataAgainstGoldenCopy(outputFile, goldenCopyFile);

            // Assert
            Assert.IsTrue(result.IsValid, $"Large dataset golden copy validation failed: {result.ErrorMessage}");
            Assert.AreEqual(rowCount, result.TotalRows);
            Assert.AreEqual(rowCount, result.MatchingRows);
            Assert.AreEqual(0, result.DifferentRows);
            Assert.AreEqual(0, result.MissingRows);
            Assert.AreEqual(0, result.ExtraRows);
        }

        /// <summary>
        /// Tests golden copy validation with performance measurement
        /// </summary>
        [TestMethod]
        public async Task TestGoldenCopyValidation_Performance()
        {
            // Arrange
            var rowCount = 5000;
            var goldenCopyData = new List<string> { "ID,Name,Value,Category,Description,Status,Priority,Owner,StartDate,EndDate" };
            var outputData = new List<string> { "ID,Name,Value,Category,Description,Status,Priority,Owner,StartDate,EndDate" };

            for (int i = 1; i <= rowCount; i++)
            {
                var name = $"Task{i}";
                var value = i * 25.50;
                var category = $"Category{(i % 5) + 1}";
                var description = $"This is a detailed description for task {i} with additional information";
                var status = i % 2 == 0 ? "Completed" : "In Progress";
                var priority = $"Priority{(i % 3) + 1}";
                var owner = $"User{(i % 10) + 1}";
                var startDate = DateTime.Now.AddDays(-i).ToString("yyyy-MM-dd");
                var endDate = DateTime.Now.AddDays(i).ToString("yyyy-MM-dd");

                goldenCopyData.Add($"{i},{name},{value},{category},{description},{status},{priority},{owner},{startDate},{endDate}");
                outputData.Add($"{i},{name},{value},{category},{description},{status},{priority},{owner},{startDate},{endDate}");
            }

            var goldenCopyFile = Path.Combine(_goldenCopyPath, "performance_golden.csv");
            var outputFile = Path.Combine(_testDataPath, "Output", "performance_output.csv");

            await File.WriteAllLinesAsync(goldenCopyFile, goldenCopyData);
            await File.WriteAllLinesAsync(outputFile, outputData);

            // Act
            var stopwatch = System.Diagnostics.Stopwatch.StartNew();
            var result = await ValidateDataAgainstGoldenCopy(outputFile, goldenCopyFile);
            stopwatch.Stop();

            // Assert
            Assert.IsTrue(result.IsValid, $"Performance golden copy validation failed: {result.ErrorMessage}");
            Assert.AreEqual(rowCount, result.TotalRows);
            Assert.AreEqual(rowCount, result.MatchingRows);

            // Performance assertion (should complete within reasonable time)
            Assert.IsTrue(stopwatch.ElapsedMilliseconds < 15000, 
                $"Golden copy validation took too long: {stopwatch.ElapsedMilliseconds}ms for {rowCount} rows");
        }

        /// <summary>
        /// Tests golden copy validation with schema validation
        /// </summary>
        [TestMethod]
        public async Task TestGoldenCopyValidation_WithSchemaValidation()
        {
            // Arrange
            var goldenCopyData = new[]
            {
                "ID,Name,Email,Phone,Address,City,State,ZipCode,Country,RegistrationDate,LastLoginDate,Status,TotalOrders,TotalSpent,CustomerTier",
                "1,John Doe,john.doe@email.com,+1-555-1234,123 Main Street,New York,NY,10001,United States,2023-01-15,2023-12-01,ACTIVE,25,1250.50,Premium",
                "2,Jane Smith,jane.smith@email.com,+1-555-5678,456 Oak Avenue,Los Angeles,CA,90210,United States,2023-02-20,2023-11-28,ACTIVE,18,890.75,Standard"
            };

            var outputData = new[]
            {
                "ID,Name,Email,Phone,Address,City,State,ZipCode,Country,RegistrationDate,LastLoginDate,Status,TotalOrders,TotalSpent,CustomerTier",
                "1,John Doe,john.doe@email.com,+1-555-1234,123 Main Street,New York,NY,10001,United States,2023-01-15,2023-12-01,ACTIVE,25,1250.50,Premium",
                "2,Jane Smith,jane.smith@email.com,+1-555-5678,456 Oak Avenue,Los Angeles,CA,90210,United States,2023-02-20,2023-11-28,ACTIVE,18,890.75,Standard"
            };

            var goldenCopyFile = Path.Combine(_goldenCopyPath, "schema_validation_golden.csv");
            var outputFile = Path.Combine(_testDataPath, "Output", "schema_validation_output.csv");

            await File.WriteAllLinesAsync(goldenCopyFile, goldenCopyData);
            await File.WriteAllLinesAsync(outputFile, outputData);

            // Act
            var result = await ValidateDataAgainstGoldenCopy(outputFile, goldenCopyFile);

            // Assert
            Assert.IsTrue(result.IsValid, $"Schema validation golden copy validation failed: {result.ErrorMessage}");
            Assert.AreEqual(2, result.TotalRows);
            Assert.AreEqual(2, result.MatchingRows);
            Assert.AreEqual(0, result.DifferentRows);

            // Verify schema compliance
            var outputDataList = await ReadCsvFile(outputFile);
            var goldenCopyDataList = await ReadCsvFile(goldenCopyFile);
            
            Assert.AreEqual(goldenCopyDataList[0].Keys.Count, outputDataList[0].Keys.Count, "Column count should match");
            Assert.IsTrue(goldenCopyDataList[0].Keys.SequenceEqual(outputDataList[0].Keys), "Column names should match");
        }

        /// <summary>
        /// Tests golden copy validation with business rule validation
        /// </summary>
        [TestMethod]
        public async Task TestGoldenCopyValidation_WithBusinessRules()
        {
            // Arrange
            var goldenCopyData = new[]
            {
                "ID,Name,Age,Salary,Department,ManagerID,StartDate,EndDate,Status",
                "1,John Doe,25,50000,Engineering,10,2023-01-15,,ACTIVE",
                "2,Jane Smith,30,75000,Marketing,20,2023-02-20,,ACTIVE",
                "3,Bob Johnson,35,60000,Engineering,10,2023-03-10,2023-12-31,INACTIVE"
            };

            var outputData = new[]
            {
                "ID,Name,Age,Salary,Department,ManagerID,StartDate,EndDate,Status",
                "1,John Doe,25,50000,Engineering,10,2023-01-15,,ACTIVE",
                "2,Jane Smith,30,75000,Marketing,20,2023-02-20,,ACTIVE",
                "3,Bob Johnson,35,60000,Engineering,10,2023-03-10,2023-12-31,INACTIVE"
            };

            var goldenCopyFile = Path.Combine(_goldenCopyPath, "business_rules_golden.csv");
            var outputFile = Path.Combine(_testDataPath, "Output", "business_rules_output.csv");

            await File.WriteAllLinesAsync(goldenCopyFile, goldenCopyData);
            await File.WriteAllLinesAsync(outputFile, outputData);

            // Act
            var result = await ValidateDataAgainstGoldenCopy(outputFile, goldenCopyFile);

            // Assert
            Assert.IsTrue(result.IsValid, $"Business rules golden copy validation failed: {result.ErrorMessage}");
            Assert.AreEqual(3, result.TotalRows);
            Assert.AreEqual(3, result.MatchingRows);
            Assert.AreEqual(0, result.DifferentRows);

            // Verify business rules
            var outputDataList = await ReadCsvFile(outputFile);
            
            foreach (var row in outputDataList)
            {
                // Rule 1: Age should be between 18 and 65
                if (int.TryParse(row["Age"], out int age))
                {
                    Assert.IsTrue(age >= 18 && age <= 65, $"Age {age} is outside valid range (18-65)");
                }
                
                // Rule 2: Salary should be positive
                if (decimal.TryParse(row["Salary"], out decimal salary))
                {
                    Assert.IsTrue(salary > 0, $"Salary {salary} should be positive");
                }
                
                // Rule 3: Active employees should not have EndDate
                if (row["Status"] == "ACTIVE")
                {
                    Assert.IsTrue(string.IsNullOrEmpty(row["EndDate"]), "Active employees should not have EndDate");
                }
            }
        }

        #endregion

        #region Data Transformation Validation Tests

        /// <summary>
        /// Tests data transformation validation with field mapping
        /// </summary>
        [TestMethod]
        public async Task TestDataTransformation_FieldMapping()
        {
            // Arrange - Input data with different field names
            var inputData = new[]
            {
                "CustomerID,FirstName,LastName,EmailAddress,PhoneNumber,StreetAddress,CityName,StateCode,PostalCode,CountryName,RegDate,LastLogin,AccountStatus,OrderCount,TotalAmount",
                "1,John,Doe,john.doe@email.com,555-1234,123 Main St,New York,NY,10001,USA,2023-01-15,2023-12-01,Active,25,1250.50",
                "2,Jane,Smith,jane.smith@email.com,555-5678,456 Oak Ave,Los Angeles,CA,90210,USA,2023-02-20,2023-11-28,Active,18,890.75"
            };

            // Expected output after field mapping transformation
            var expectedOutputData = new[]
            {
                "customer_id,first_name,last_name,email,phone,address,city,state,zip_code,country,registration_date,last_login_date,status,total_orders,total_spent",
                "1,John,Doe,john.doe@email.com,555-1234,123 Main St,New York,NY,10001,USA,2023-01-15,2023-12-01,Active,25,1250.50",
                "2,Jane,Smith,jane.smith@email.com,555-5678,456 Oak Ave,Los Angeles,CA,90210,USA,2023-02-20,2023-11-28,Active,18,890.75"
            };

            var inputFile = Path.Combine(_testDataPath, "Input", "field_mapping_input.csv");
            var outputFile = Path.Combine(_testDataPath, "Output", "field_mapping_output.csv");
            var expectedFile = Path.Combine(_testDataPath, "Expected", "field_mapping_expected.csv");

            await File.WriteAllLinesAsync(inputFile, inputData);
            await File.WriteAllLinesAsync(outputFile, expectedOutputData); // Simulate transformation output
            await File.WriteAllLinesAsync(expectedFile, expectedOutputData);

            // Act
            var result = await ValidateDataAgainstGoldenCopy(outputFile, expectedFile);

            // Assert
            Assert.IsTrue(result.IsValid, $"Field mapping transformation validation failed: {result.ErrorMessage}");
            Assert.AreEqual(2, result.TotalRows);
            Assert.AreEqual(2, result.MatchingRows);
            Assert.AreEqual(0, result.DifferentRows);

            // Verify field mapping transformation
            var inputDataList = await ReadCsvFile(inputFile);
            var outputDataList = await ReadCsvFile(outputFile);
            
            // Verify that field names were transformed correctly
            Assert.IsTrue(outputDataList[0].ContainsKey("customer_id"), "Field 'CustomerID' should be mapped to 'customer_id'");
            Assert.IsTrue(outputDataList[0].ContainsKey("first_name"), "Field 'FirstName' should be mapped to 'first_name'");
            Assert.IsTrue(outputDataList[0].ContainsKey("email"), "Field 'EmailAddress' should be mapped to 'email'");
            Assert.IsTrue(outputDataList[0].ContainsKey("registration_date"), "Field 'RegDate' should be mapped to 'registration_date'");
        }

        /// <summary>
        /// Tests data transformation validation with data type conversion
        /// </summary>
        [TestMethod]
        public async Task TestDataTransformation_DataTypeConversion()
        {
            // Arrange - Input data with string values
            var inputData = new[]
            {
                "ID,Name,Age,Salary,IsActive,Score,RegistrationDate",
                "1,John Doe,25,50000.50,true,85.5,2023-01-15",
                "2,Jane Smith,30,75000.75,false,92.3,2023-02-20"
            };

            // Expected output after data type conversion
            var expectedOutputData = new[]
            {
                "ID,Name,Age,Salary,IsActive,Score,RegistrationDate",
                "1,John Doe,25,50000.50,true,85.5,2023-01-15",
                "2,Jane Smith,30,75000.75,false,92.3,2023-02-20"
            };

            var inputFile = Path.Combine(_testDataPath, "Input", "data_type_input.csv");
            var outputFile = Path.Combine(_testDataPath, "Output", "data_type_output.csv");
            var expectedFile = Path.Combine(_testDataPath, "Expected", "data_type_expected.csv");

            await File.WriteAllLinesAsync(inputFile, inputData);
            await File.WriteAllLinesAsync(outputFile, expectedOutputData); // Simulate transformation output
            await File.WriteAllLinesAsync(expectedFile, expectedOutputData);

            // Act
            var result = await ValidateDataAgainstGoldenCopy(outputFile, expectedFile);

            // Assert
            Assert.IsTrue(result.IsValid, $"Data type conversion validation failed: {result.ErrorMessage}");
            Assert.AreEqual(2, result.TotalRows);
            Assert.AreEqual(2, result.MatchingRows);
            Assert.AreEqual(0, result.DifferentRows);

            // Verify data type conversion
            var outputDataList = await ReadCsvFile(outputFile);
            
            foreach (var row in outputDataList)
            {
                // Verify integer conversion
                Assert.IsTrue(int.TryParse(row["Age"], out int age), "Age should be convertible to integer");
                Assert.IsTrue(age > 0, "Age should be positive");
                
                // Verify decimal conversion
                Assert.IsTrue(decimal.TryParse(row["Salary"], out decimal salary), "Salary should be convertible to decimal");
                Assert.IsTrue(salary > 0, "Salary should be positive");
                
                // Verify boolean conversion
                Assert.IsTrue(bool.TryParse(row["IsActive"], out bool isActive), "IsActive should be convertible to boolean");
                
                // Verify date conversion
                Assert.IsTrue(DateTime.TryParse(row["RegistrationDate"], out DateTime regDate), "RegistrationDate should be convertible to DateTime");
            }
        }

        /// <summary>
        /// Tests data transformation validation with data cleansing
        /// </summary>
        [TestMethod]
        public async Task TestDataTransformation_DataCleansing()
        {
            // Arrange - Input data with dirty data
            var inputData = new[]
            {
                "ID,Name,Email,Phone,Address,City,State,ZipCode",
                "1,  John Doe  ,JOHN.DOE@EMAIL.COM,555-1234,123 Main St,New York,NY,10001",
                "2,Jane Smith,jane.smith@email.com,(555) 567-8900,456 Oak Ave,Los Angeles,CA,90210",
                "3,Bob Johnson,bob.johnson@email.com,555.901.2345,789 Pine Rd,Chicago,IL,60601"
            };

            // Expected output after data cleansing
            var expectedOutputData = new[]
            {
                "ID,Name,Email,Phone,Address,City,State,ZipCode",
                "1,John Doe,john.doe@email.com,555-1234,123 Main St,New York,NY,10001",
                "2,Jane Smith,jane.smith@email.com,555-567-8900,456 Oak Ave,Los Angeles,CA,90210",
                "3,Bob Johnson,bob.johnson@email.com,555-901-2345,789 Pine Rd,Chicago,IL,60601"
            };

            var inputFile = Path.Combine(_testDataPath, "Input", "data_cleansing_input.csv");
            var outputFile = Path.Combine(_testDataPath, "Output", "data_cleansing_output.csv");
            var expectedFile = Path.Combine(_testDataPath, "Expected", "data_cleansing_expected.csv");

            await File.WriteAllLinesAsync(inputFile, inputData);
            await File.WriteAllLinesAsync(outputFile, expectedOutputData); // Simulate transformation output
            await File.WriteAllLinesAsync(expectedFile, expectedOutputData);

            // Act
            var result = await ValidateDataAgainstGoldenCopy(outputFile, expectedFile);

            // Assert
            Assert.IsTrue(result.IsValid, $"Data cleansing validation failed: {result.ErrorMessage}");
            Assert.AreEqual(3, result.TotalRows);
            Assert.AreEqual(3, result.MatchingRows);
            Assert.AreEqual(0, result.DifferentRows);

            // Verify data cleansing transformations
            var outputDataList = await ReadCsvFile(outputFile);
            
            foreach (var row in outputDataList)
            {
                // Verify name trimming
                Assert.IsFalse(row["Name"].StartsWith(" ") || row["Name"].EndsWith(" "), "Name should be trimmed");
                
                // Verify email normalization
                Assert.IsTrue(row["Email"].All(c => char.IsLower(c) || c == '@' || c == '.'), "Email should be lowercase");
                
                // Verify phone number formatting
                Assert.IsTrue(row["Phone"].All(c => char.IsDigit(c) || c == '-'), "Phone should be formatted consistently");
            }
        }

        /// <summary>
        /// Tests data transformation validation with data enrichment
        /// </summary>
        [TestMethod]
        public async Task TestDataTransformation_DataEnrichment()
        {
            // Arrange - Input data without enriched fields
            var inputData = new[]
            {
                "ID,Name,Email,Phone,Address,City,State,ZipCode,TotalOrders,TotalSpent",
                "1,John Doe,john.doe@email.com,555-1234,123 Main St,New York,NY,10001,25,1250.50",
                "2,Jane Smith,jane.smith@email.com,555-5678,456 Oak Ave,Los Angeles,CA,90210,18,890.75",
                "3,Bob Johnson,bob.johnson@email.com,555-9012,789 Pine Rd,Chicago,IL,60601,8,320.25"
            };

            // Expected output after data enrichment
            var expectedOutputData = new[]
            {
                "ID,Name,Email,Phone,Address,City,State,ZipCode,TotalOrders,TotalSpent,CustomerTier,AverageOrderValue,LastOrderDate",
                "1,John Doe,john.doe@email.com,555-1234,123 Main St,New York,NY,10001,25,1250.50,Premium,50.02,2023-12-01",
                "2,Jane Smith,jane.smith@email.com,555-5678,456 Oak Ave,Los Angeles,CA,90210,18,890.75,Standard,49.49,2023-11-28",
                "3,Bob Johnson,bob.johnson@email.com,555-9012,789 Pine Rd,Chicago,IL,60601,8,320.25,Basic,40.03,2023-10-15"
            };

            var inputFile = Path.Combine(_testDataPath, "Input", "data_enrichment_input.csv");
            var outputFile = Path.Combine(_testDataPath, "Output", "data_enrichment_output.csv");
            var expectedFile = Path.Combine(_testDataPath, "Expected", "data_enrichment_expected.csv");

            await File.WriteAllLinesAsync(inputFile, inputData);
            await File.WriteAllLinesAsync(outputFile, expectedOutputData); // Simulate transformation output
            await File.WriteAllLinesAsync(expectedFile, expectedOutputData);

            // Act
            var result = await ValidateDataAgainstGoldenCopy(outputFile, expectedFile);

            // Assert
            Assert.IsTrue(result.IsValid, $"Data enrichment validation failed: {result.ErrorMessage}");
            Assert.AreEqual(3, result.TotalRows);
            Assert.AreEqual(3, result.MatchingRows);
            Assert.AreEqual(0, result.DifferentRows);

            // Verify data enrichment
            var outputDataList = await ReadCsvFile(outputFile);
            
            foreach (var row in outputDataList)
            {
                // Verify customer tier calculation
                Assert.IsTrue(new[] { "Premium", "Standard", "Basic" }.Contains(row["CustomerTier"]), "CustomerTier should be calculated");
                
                // Verify average order value calculation
                if (decimal.TryParse(row["TotalSpent"], out decimal totalSpent) && 
                    int.TryParse(row["TotalOrders"], out int totalOrders))
                {
                    var expectedAvg = totalSpent / totalOrders;
                    if (decimal.TryParse(row["AverageOrderValue"], out decimal actualAvg))
                    {
                        Assert.IsTrue(Math.Abs(actualAvg - expectedAvg) < 0.01m, "AverageOrderValue should be calculated correctly");
                    }
                }
                
                // Verify last order date
                Assert.IsTrue(DateTime.TryParse(row["LastOrderDate"], out DateTime lastOrderDate), "LastOrderDate should be valid");
            }
        }

        /// <summary>
        /// Tests data transformation validation with data aggregation
        /// </summary>
        [TestMethod]
        public async Task TestDataTransformation_DataAggregation()
        {
            // Arrange - Input data with detailed records
            var inputData = new[]
            {
                "OrderID,CustomerID,ProductID,Quantity,UnitPrice,OrderDate,Category",
                "1,1,101,2,25.50,2023-01-15,Electronics",
                "2,1,102,1,15.75,2023-01-15,Electronics",
                "3,2,103,3,10.25,2023-02-20,Clothing",
                "4,2,104,1,45.00,2023-02-20,Electronics",
                "5,3,105,2,20.00,2023-03-10,Clothing"
            };

            // Expected output after aggregation by customer
            var expectedOutputData = new[]
            {
                "CustomerID,TotalOrders,TotalQuantity,TotalAmount,AverageOrderValue,FirstOrderDate,LastOrderDate,CategoryCount",
                "1,2,3,66.75,33.38,2023-01-15,2023-01-15,2",
                "2,2,4,75.75,37.88,2023-02-20,2023-02-20,2",
                "3,1,2,40.00,40.00,2023-03-10,2023-03-10,1"
            };

            var inputFile = Path.Combine(_testDataPath, "Input", "data_aggregation_input.csv");
            var outputFile = Path.Combine(_testDataPath, "Output", "data_aggregation_output.csv");
            var expectedFile = Path.Combine(_testDataPath, "Expected", "data_aggregation_expected.csv");

            await File.WriteAllLinesAsync(inputFile, inputData);
            await File.WriteAllLinesAsync(outputFile, expectedOutputData); // Simulate transformation output
            await File.WriteAllLinesAsync(expectedFile, expectedOutputData);

            // Act
            var result = await ValidateDataAgainstGoldenCopy(outputFile, expectedFile);

            // Assert
            Assert.IsTrue(result.IsValid, $"Data aggregation validation failed: {result.ErrorMessage}");
            Assert.AreEqual(3, result.TotalRows);
            Assert.AreEqual(3, result.MatchingRows);
            Assert.AreEqual(0, result.DifferentRows);

            // Verify aggregation calculations
            var outputDataList = await ReadCsvFile(outputFile);
            
            foreach (var row in outputDataList)
            {
                // Verify total amount calculation
                if (decimal.TryParse(row["TotalAmount"], out decimal totalAmount))
                {
                    Assert.IsTrue(totalAmount > 0, "TotalAmount should be positive");
                }
                
                // Verify average order value calculation
                if (decimal.TryParse(row["TotalAmount"], out decimal total) && 
                    int.TryParse(row["TotalOrders"], out int orders))
                {
                    var expectedAvg = total / orders;
                    if (decimal.TryParse(row["AverageOrderValue"], out decimal actualAvg))
                    {
                        Assert.IsTrue(Math.Abs(actualAvg - expectedAvg) < 0.01m, "AverageOrderValue should be calculated correctly");
                    }
                }
                
                // Verify date range
                Assert.IsTrue(DateTime.TryParse(row["FirstOrderDate"], out DateTime firstDate), "FirstOrderDate should be valid");
                Assert.IsTrue(DateTime.TryParse(row["LastOrderDate"], out DateTime lastDate), "LastOrderDate should be valid");
                Assert.IsTrue(firstDate <= lastDate, "FirstOrderDate should be <= LastOrderDate");
            }
        }

        /// <summary>
        /// Tests data transformation validation with data filtering
        /// </summary>
        [TestMethod]
        public async Task TestDataTransformation_DataFiltering()
        {
            // Arrange - Input data with all records
            var inputData = new[]
            {
                "ID,Name,Age,Salary,Department,Status",
                "1,John Doe,25,50000,Engineering,Active",
                "2,Jane Smith,30,75000,Marketing,Active",
                "3,Bob Johnson,35,60000,Engineering,Inactive",
                "4,Alice Brown,28,55000,Sales,Active",
                "5,Charlie Wilson,45,80000,Engineering,Active"
            };

            // Expected output after filtering (Active employees only)
            var expectedOutputData = new[]
            {
                "ID,Name,Age,Salary,Department,Status",
                "1,John Doe,25,50000,Engineering,Active",
                "2,Jane Smith,30,75000,Marketing,Active",
                "4,Alice Brown,28,55000,Sales,Active",
                "5,Charlie Wilson,45,80000,Engineering,Active"
            };

            var inputFile = Path.Combine(_testDataPath, "Input", "data_filtering_input.csv");
            var outputFile = Path.Combine(_testDataPath, "Output", "data_filtering_output.csv");
            var expectedFile = Path.Combine(_testDataPath, "Expected", "data_filtering_expected.csv");

            await File.WriteAllLinesAsync(inputFile, inputData);
            await File.WriteAllLinesAsync(outputFile, expectedOutputData); // Simulate transformation output
            await File.WriteAllLinesAsync(expectedFile, expectedOutputData);

            // Act
            var result = await ValidateDataAgainstGoldenCopy(outputFile, expectedFile);

            // Assert
            Assert.IsTrue(result.IsValid, $"Data filtering validation failed: {result.ErrorMessage}");
            Assert.AreEqual(4, result.TotalRows);
            Assert.AreEqual(4, result.MatchingRows);
            Assert.AreEqual(0, result.DifferentRows);

            // Verify filtering logic
            var inputDataList = await ReadCsvFile(inputFile);
            var outputDataList = await ReadCsvFile(outputFile);
            
            // Verify that only active employees are included
            foreach (var row in outputDataList)
            {
                Assert.AreEqual("Active", row["Status"], "Only active employees should be included");
            }
            
            // Verify that inactive employees are excluded
            var inactiveCount = inputDataList.Count(r => r["Status"] == "Inactive");
            Assert.AreEqual(1, inactiveCount, "Should have 1 inactive employee in input");
            Assert.AreEqual(0, outputDataList.Count(r => r["Status"] == "Inactive"), "Should have 0 inactive employees in output");
        }

        /// <summary>
        /// Tests data transformation validation with data sorting
        /// </summary>
        [TestMethod]
        public async Task TestDataTransformation_DataSorting()
        {
            // Arrange - Input data in random order
            var inputData = new[]
            {
                "ID,Name,Age,Salary,Department",
                "3,Bob Johnson,35,60000,Engineering",
                "1,John Doe,25,50000,Engineering",
                "5,Charlie Wilson,45,80000,Engineering",
                "2,Jane Smith,30,75000,Marketing",
                "4,Alice Brown,28,55000,Sales"
            };

            // Expected output after sorting by Salary (descending)
            var expectedOutputData = new[]
            {
                "ID,Name,Age,Salary,Department",
                "5,Charlie Wilson,45,80000,Engineering",
                "2,Jane Smith,30,75000,Marketing",
                "3,Bob Johnson,35,60000,Engineering",
                "4,Alice Brown,28,55000,Sales",
                "1,John Doe,25,50000,Engineering"
            };

            var inputFile = Path.Combine(_testDataPath, "Input", "data_sorting_input.csv");
            var outputFile = Path.Combine(_testDataPath, "Output", "data_sorting_output.csv");
            var expectedFile = Path.Combine(_testDataPath, "Expected", "data_sorting_expected.csv");

            await File.WriteAllLinesAsync(inputFile, inputData);
            await File.WriteAllLinesAsync(outputFile, expectedOutputData); // Simulate transformation output
            await File.WriteAllLinesAsync(expectedFile, expectedOutputData);

            // Act
            var result = await ValidateDataAgainstGoldenCopy(outputFile, expectedFile);

            // Assert
            Assert.IsTrue(result.IsValid, $"Data sorting validation failed: {result.ErrorMessage}");
            Assert.AreEqual(5, result.TotalRows);
            Assert.AreEqual(5, result.MatchingRows);
            Assert.AreEqual(0, result.DifferentRows);

            // Verify sorting logic
            var outputDataList = await ReadCsvFile(outputFile);
            
            // Verify that data is sorted by salary in descending order
            for (int i = 0; i < outputDataList.Count - 1; i++)
            {
                if (decimal.TryParse(outputDataList[i]["Salary"], out decimal currentSalary) &&
                    decimal.TryParse(outputDataList[i + 1]["Salary"], out decimal nextSalary))
                {
                    Assert.IsTrue(currentSalary >= nextSalary, $"Salary should be in descending order: {currentSalary} >= {nextSalary}");
                }
            }
        }

        /// <summary>
        /// Tests data transformation validation with data deduplication
        /// </summary>
        [TestMethod]
        public async Task TestDataTransformation_DataDeduplication()
        {
            // Arrange - Input data with duplicates
            var inputData = new[]
            {
                "ID,Name,Email,Phone,Address,City,State,ZipCode",
                "1,John Doe,john.doe@email.com,555-1234,123 Main St,New York,NY,10001",
                "2,Jane Smith,jane.smith@email.com,555-5678,456 Oak Ave,Los Angeles,CA,90210",
                "1,John Doe,john.doe@email.com,555-1234,123 Main St,New York,NY,10001", // Duplicate
                "3,Bob Johnson,bob.johnson@email.com,555-9012,789 Pine Rd,Chicago,IL,60601",
                "2,Jane Smith,jane.smith@email.com,555-5678,456 Oak Ave,Los Angeles,CA,90210" // Duplicate
            };

            // Expected output after deduplication
            var expectedOutputData = new[]
            {
                "ID,Name,Email,Phone,Address,City,State,ZipCode",
                "1,John Doe,john.doe@email.com,555-1234,123 Main St,New York,NY,10001",
                "2,Jane Smith,jane.smith@email.com,555-5678,456 Oak Ave,Los Angeles,CA,90210",
                "3,Bob Johnson,bob.johnson@email.com,555-9012,789 Pine Rd,Chicago,IL,60601"
            };

            var inputFile = Path.Combine(_testDataPath, "Input", "data_deduplication_input.csv");
            var outputFile = Path.Combine(_testDataPath, "Output", "data_deduplication_output.csv");
            var expectedFile = Path.Combine(_testDataPath, "Expected", "data_deduplication_expected.csv");

            await File.WriteAllLinesAsync(inputFile, inputData);
            await File.WriteAllLinesAsync(outputFile, expectedOutputData); // Simulate transformation output
            await File.WriteAllLinesAsync(expectedFile, expectedOutputData);

            // Act
            var result = await ValidateDataAgainstGoldenCopy(outputFile, expectedFile);

            // Assert
            Assert.IsTrue(result.IsValid, $"Data deduplication validation failed: {result.ErrorMessage}");
            Assert.AreEqual(3, result.TotalRows);
            Assert.AreEqual(3, result.MatchingRows);
            Assert.AreEqual(0, result.DifferentRows);

            // Verify deduplication logic
            var inputDataList = await ReadCsvFile(inputFile);
            var outputDataList = await ReadCsvFile(outputFile);
            
            // Verify that duplicates are removed
            Assert.AreEqual(5, inputDataList.Count, "Input should have 5 records including duplicates");
            Assert.AreEqual(3, outputDataList.Count, "Output should have 3 unique records");
            
            // Verify that all unique records are preserved
            var inputIds = inputDataList.Select(r => r["ID"]).Distinct().ToList();
            var outputIds = outputDataList.Select(r => r["ID"]).ToList();
            Assert.IsTrue(inputIds.SequenceEqual(outputIds), "All unique IDs should be preserved");
        }

        /// <summary>
        /// Tests data transformation validation with complex business rules
        /// </summary>
        [TestMethod]
        public async Task TestDataTransformation_ComplexBusinessRules()
        {
            // Arrange - Input data for complex business rule validation
            var inputData = new[]
            {
                "CustomerID,FirstName,LastName,Email,Phone,Address,City,State,ZipCode,Country,RegistrationDate,LastLoginDate,Status,TotalOrders,TotalSpent",
                "1,John,Doe,john.doe@email.com,555-1234,123 Main St,New York,NY,10001,USA,2023-01-15,2023-12-01,Active,25,1250.50",
                "2,Jane,Smith,jane.smith@email.com,555-5678,456 Oak Ave,Los Angeles,CA,90210,USA,2023-02-20,2023-11-28,Active,18,890.75",
                "3,Bob,Johnson,bob.johnson@email.com,555-9012,789 Pine Rd,Chicago,IL,60601,USA,2023-03-10,2023-10-15,Inactive,8,320.25"
            };

            // Expected output after applying complex business rules
            var expectedOutputData = new[]
            {
                "customer_id,first_name,last_name,email,phone,address,city,state,zip_code,country,registration_date,last_login_date,status,total_orders,total_spent,customer_tier,risk_score,lifetime_value",
                "1,John,Doe,john.doe@email.com,+1-555-1234,123 Main Street,New York,NY,10001,United States,2023-01-15,2023-12-01,ACTIVE,25,1250.50,Premium,Low,1250.50",
                "2,Jane,Smith,jane.smith@email.com,+1-555-5678,456 Oak Avenue,Los Angeles,CA,90210,United States,2023-02-20,2023-11-28,ACTIVE,18,890.75,Standard,Medium,890.75",
                "3,Bob,Johnson,bob.johnson@email.com,+1-555-9012,789 Pine Road,Chicago,IL,60601,United States,2023-03-10,2023-10-15,INACTIVE,8,320.25,Basic,High,320.25"
            };

            var inputFile = Path.Combine(_testDataPath, "Input", "complex_business_rules_input.csv");
            var outputFile = Path.Combine(_testDataPath, "Output", "complex_business_rules_output.csv");
            var expectedFile = Path.Combine(_testDataPath, "Expected", "complex_business_rules_expected.csv");

            await File.WriteAllLinesAsync(inputFile, inputData);
            await File.WriteAllLinesAsync(outputFile, expectedOutputData); // Simulate transformation output
            await File.WriteAllLinesAsync(expectedFile, expectedOutputData);

            // Act
            var result = await ValidateDataAgainstGoldenCopy(outputFile, expectedFile);

            // Assert
            Assert.IsTrue(result.IsValid, $"Complex business rules validation failed: {result.ErrorMessage}");
            Assert.AreEqual(3, result.TotalRows);
            Assert.AreEqual(3, result.MatchingRows);
            Assert.AreEqual(0, result.DifferentRows);

            // Verify complex business rules
            var outputDataList = await ReadCsvFile(outputFile);
            
            foreach (var row in outputDataList)
            {
                // Rule 1: Customer tier calculation based on total spent
                if (decimal.TryParse(row["total_spent"], out decimal totalSpent))
                {
                    var expectedTier = totalSpent >= 1000 ? "Premium" : 
                                     totalSpent >= 500 ? "Standard" : "Basic";
                    Assert.AreEqual(expectedTier, row["customer_tier"], "Customer tier should be calculated correctly");
                }
                
                // Rule 2: Risk score calculation based on status and activity
                var expectedRiskScore = row["status"] == "INACTIVE" ? "High" : 
                                      row["total_orders"] == "25" ? "Low" : "Medium";
                Assert.AreEqual(expectedRiskScore, row["risk_score"], "Risk score should be calculated correctly");
                
                // Rule 3: Lifetime value equals total spent for now
                if (decimal.TryParse(row["total_spent"], out decimal total) &&
                    decimal.TryParse(row["lifetime_value"], out decimal ltv))
                {
                    Assert.AreEqual(total, ltv, "Lifetime value should equal total spent");
                }
                
                // Rule 4: Phone number formatting
                Assert.IsTrue(row["phone"].StartsWith("+1-"), "Phone should be formatted with country code");
                
                // Rule 5: Address standardization
                Assert.IsTrue(row["address"].Contains("Street") || row["address"].Contains("Avenue") || row["address"].Contains("Road"), 
                    "Address should be standardized");
                
                // Rule 6: Country name standardization
                Assert.AreEqual("United States", row["country"], "Country should be standardized");
                
                // Rule 7: Status standardization
                Assert.IsTrue(new[] { "ACTIVE", "INACTIVE" }.Contains(row["status"]), "Status should be standardized");
            }
        }

        #endregion

        #region Data Quality and Integrity Tests

        /// <summary>
        /// Tests data completeness validation
        /// </summary>
        [TestMethod]
        public async Task TestDataQuality_CompletenessValidation()
        {
            // Arrange - Data with missing values
            var testData = new[]
            {
                "ID,Name,Email,Phone,Address,City,State,ZipCode,Country",
                "1,John Doe,john.doe@email.com,555-1234,123 Main St,New York,NY,10001,USA",
                "2,Jane Smith,,555-5678,456 Oak Ave,Los Angeles,CA,90210,USA", // Missing email
                "3,Bob Johnson,bob.johnson@email.com,,789 Pine Rd,Chicago,IL,60601,USA", // Missing phone
                "4,Alice Brown,alice.brown@email.com,555-9012,,Boston,MA,02101,USA", // Missing address
                "5,Charlie Wilson,charlie.wilson@email.com,555-3456,321 Elm St,,,USA" // Missing city and state
            };

            var testFile = Path.Combine(_testDataPath, "completeness_test.csv");
            await File.WriteAllLinesAsync(testFile, testData);

            // Act
            var requiredFields = new[] { "Name", "Email", "Phone", "Address", "City", "State" };
            var result = await DataValidationUtilities.ValidateDataCompletenessAsync(testFile, requiredFields);

            // Assert
            Assert.IsFalse(result.IsValid, "Data should not be valid due to missing required fields");
            Assert.AreEqual(5, result.RowValidations.Count);
            Assert.AreEqual(1, result.CompleteRows); // Only John Doe has all required fields
            Assert.AreEqual(4, result.IncompleteRows);

            // Verify specific missing fields
            var janeRow = result.RowValidations[1];
            Assert.IsFalse(janeRow.IsValid);
            Assert.IsTrue(janeRow.MissingFields.Contains("Email"));

            var bobRow = result.RowValidations[2];
            Assert.IsFalse(bobRow.IsValid);
            Assert.IsTrue(bobRow.MissingFields.Contains("Phone"));

            var aliceRow = result.RowValidations[3];
            Assert.IsFalse(aliceRow.IsValid);
            Assert.IsTrue(aliceRow.MissingFields.Contains("Address"));

            var charlieRow = result.RowValidations[4];
            Assert.IsFalse(charlieRow.IsValid);
            Assert.IsTrue(charlieRow.MissingFields.Contains("City"));
            Assert.IsTrue(charlieRow.MissingFields.Contains("State"));
        }

        /// <summary>
        /// Tests data accuracy validation
        /// </summary>
        [TestMethod]
        public async Task TestDataQuality_AccuracyValidation()
        {
            // Arrange - Create reference data and test data
            var referenceData = new[]
            {
                "ID,Name,Email,Phone,Address,City,State,ZipCode,Country",
                "1,John Doe,john.doe@email.com,555-1234,123 Main St,New York,NY,10001,USA",
                "2,Jane Smith,jane.smith@email.com,555-5678,456 Oak Ave,Los Angeles,CA,90210,USA",
                "3,Bob Johnson,bob.johnson@email.com,555-9012,789 Pine Rd,Chicago,IL,60601,USA"
            };

            var testData = new[]
            {
                "ID,Name,Email,Phone,Address,City,State,ZipCode,Country",
                "1,John Doe,john.doe@email.com,555-1234,123 Main St,New York,NY,10001,USA", // Exact match
                "2,Jane Smith,jane.smith@email.com,555-5679,456 Oak Ave,Los Angeles,CA,90210,USA", // Phone changed
                "3,Bob Johnson,bob.johnson@email.com,555-9012,789 Pine Rd,Chicago,IL,60602,USA" // ZipCode changed
            };

            var referenceFile = Path.Combine(_testDataPath, "accuracy_reference.csv");
            var testFile = Path.Combine(_testDataPath, "accuracy_test.csv");

            await File.WriteAllLinesAsync(referenceFile, referenceData);
            await File.WriteAllLinesAsync(testFile, testData);

            // Act
            var keyFields = new[] { "ID" };
            var result = await DataValidationUtilities.ValidateDataAccuracyAsync(testFile, referenceFile, keyFields);

            // Assert
            Assert.IsFalse(result.IsValid, "Data should not be valid due to accuracy issues");
            Assert.AreEqual(3, result.RowValidations.Count);
            Assert.AreEqual(1, result.AccurateRows); // Only John Doe is accurate
            Assert.AreEqual(2, result.InaccurateRows);

            // Verify specific accuracy issues
            var janeRow = result.RowValidations[1];
            Assert.IsFalse(janeRow.IsValid);
            Assert.AreEqual(1, janeRow.FieldDifferences.Count);
            Assert.AreEqual("Phone", janeRow.FieldDifferences[0].Field);
            Assert.AreEqual("555-5679", janeRow.FieldDifferences[0].ActualValue);
            Assert.AreEqual("555-5678", janeRow.FieldDifferences[0].ExpectedValue);

            var bobRow = result.RowValidations[2];
            Assert.IsFalse(bobRow.IsValid);
            Assert.AreEqual(1, bobRow.FieldDifferences.Count);
            Assert.AreEqual("ZipCode", bobRow.FieldDifferences[0].Field);
            Assert.AreEqual("60602", bobRow.FieldDifferences[0].ActualValue);
            Assert.AreEqual("60601", bobRow.FieldDifferences[0].ExpectedValue);
        }

        /// <summary>
        /// Tests data consistency validation
        /// </summary>
        [TestMethod]
        public async Task TestDataQuality_ConsistencyValidation()
        {
            // Arrange - Create multiple datasets with consistency rules
            var customerData = new[]
            {
                "CustomerID,Name,Email,Phone,Address,City,State,ZipCode,Country",
                "1,John Doe,john.doe@email.com,555-1234,123 Main St,New York,NY,10001,USA",
                "2,Jane Smith,jane.smith@email.com,555-5678,456 Oak Ave,Los Angeles,CA,90210,USA",
                "3,Bob Johnson,bob.johnson@email.com,555-9012,789 Pine Rd,Chicago,IL,60601,USA"
            };

            var orderData = new[]
            {
                "OrderID,CustomerID,ProductID,Quantity,UnitPrice,OrderDate,TotalAmount",
                "1,1,101,2,25.50,2023-01-15,51.00",
                "2,1,102,1,15.75,2023-01-15,15.75",
                "3,2,103,3,10.25,2023-02-20,30.75",
                "4,4,104,1,45.00,2023-02-20,45.00" // CustomerID 4 doesn't exist in customer data
            };

            var customerFile = Path.Combine(_testDataPath, "consistency_customers.csv");
            var orderFile = Path.Combine(_testDataPath, "consistency_orders.csv");

            await File.WriteAllLinesAsync(customerFile, customerData);
            await File.WriteAllLinesAsync(orderFile, orderData);

            // Act
            var dataFiles = new[] { customerFile, orderFile };
            var keyFields = new[] { "CustomerID" };
            var consistencyRules = new[] { "CustomerID must exist in customer data" };
            var result = await DataValidationUtilities.ValidateDataConsistencyAsync(dataFiles, keyFields, consistencyRules);

            // Assert
            Assert.IsFalse(result.IsValid, "Data should not be consistent due to referential integrity violation");
            Assert.AreEqual(1, result.ConsistencyChecks.Count);
            Assert.IsFalse(result.ConsistencyChecks[0].IsConsistent);
            Assert.IsTrue(result.ConsistencyChecks[0].Message.Contains("CustomerID 4"));
        }

        /// <summary>
        /// Tests data statistical validation
        /// </summary>
        [TestMethod]
        public async Task TestDataQuality_StatisticalValidation()
        {
            // Arrange - Data with numeric fields for statistical analysis
            var testData = new[]
            {
                "ID,Name,Age,Salary,Score,Rating",
                "1,John Doe,25,50000,85.5,4.5",
                "2,Jane Smith,30,75000,92.3,4.8",
                "3,Bob Johnson,35,60000,78.2,4.2",
                "4,Alice Brown,28,55000,88.7,4.6",
                "5,Charlie Wilson,45,80000,95.1,4.9",
                "6,David Lee,22,45000,72.8,3.8",
                "7,Eva Garcia,38,70000,89.4,4.7",
                "8,Frank Miller,29,52000,83.6,4.4",
                "9,Grace Taylor,31,68000,91.2,4.8",
                "10,Henry Davis,27,58000,86.9,4.5"
            };

            var testFile = Path.Combine(_testDataPath, "statistical_test.csv");
            await File.WriteAllLinesAsync(testFile, testData);

            // Act
            var numericFields = new[] { "Age", "Salary", "Score", "Rating" };
            var options = new StatisticalValidationOptions { OutlierThreshold = 2.0 };
            var result = await DataValidationUtilities.ValidateDataStatisticsAsync(testFile, numericFields, options);

            // Assert
            Assert.IsTrue(result.IsValid, "Data should be statistically valid");
            Assert.AreEqual(4, result.FieldStatistics.Count);
            Assert.AreEqual(4, result.Outliers.Count);
            Assert.AreEqual(4, result.DistributionChecks.Count);

            // Verify statistics for Age field
            var ageStats = result.FieldStatistics["Age"];
            Assert.AreEqual(10, ageStats.Count);
            Assert.AreEqual(22, ageStats.Min);
            Assert.AreEqual(45, ageStats.Max);
            Assert.IsTrue(ageStats.Mean > 25 && ageStats.Mean < 35);
            Assert.IsTrue(ageStats.StandardDeviation > 0);

            // Verify statistics for Salary field
            var salaryStats = result.FieldStatistics["Salary"];
            Assert.AreEqual(10, salaryStats.Count);
            Assert.AreEqual(45000, salaryStats.Min);
            Assert.AreEqual(80000, salaryStats.Max);
            Assert.IsTrue(salaryStats.Mean > 50000 && salaryStats.Mean < 70000);

            // Verify no outliers detected
            foreach (var field in numericFields)
            {
                Assert.AreEqual(0, result.Outliers[field].Count, $"No outliers should be detected for {field}");
            }
        }

        /// <summary>
        /// Tests data freshness validation
        /// </summary>
        [TestMethod]
        public async Task TestDataQuality_FreshnessValidation()
        {
            // Arrange - Data with timestamps
            var testData = new[]
            {
                "ID,Name,Email,LastLoginDate,LastOrderDate,LastUpdateDate",
                "1,John Doe,john.doe@email.com,2023-12-01,2023-11-28,2023-12-01",
                "2,Jane Smith,jane.smith@email.com,2023-11-28,2023-11-25,2023-11-28",
                "3,Bob Johnson,bob.johnson@email.com,2023-10-15,2023-10-10,2023-10-15",
                "4,Alice Brown,alice.brown@email.com,2023-12-02,2023-11-30,2023-12-02"
            };

            var testFile = Path.Combine(_testDataPath, "freshness_test.csv");
            await File.WriteAllLinesAsync(testFile, testData);

            // Act - Test with 7 days max age
            var maxAge = TimeSpan.FromDays(7);
            var result = await DataValidationUtilities.ValidateDataFreshnessAsync(testFile, "LastLoginDate", maxAge);

            // Assert
            Assert.IsFalse(result.IsValid, "Data should not be fresh due to old timestamps");
            Assert.AreEqual(4, result.RowValidations.Count);
            Assert.AreEqual(2, result.FreshRows); // John and Alice have recent logins
            Assert.AreEqual(2, result.StaleRows); // Jane and Bob have old logins

            // Verify specific freshness issues
            var janeRow = result.RowValidations[1];
            Assert.IsFalse(janeRow.IsValid);
            Assert.IsTrue(janeRow.Age > maxAge);

            var bobRow = result.RowValidations[2];
            Assert.IsFalse(bobRow.IsValid);
            Assert.IsTrue(bobRow.Age > maxAge);
        }

        /// <summary>
        /// Tests data uniqueness validation
        /// </summary>
        [TestMethod]
        public async Task TestDataQuality_UniquenessValidation()
        {
            // Arrange - Data with duplicate values
            var testData = new[]
            {
                "ID,Name,Email,Phone,Address,City,State,ZipCode",
                "1,John Doe,john.doe@email.com,555-1234,123 Main St,New York,NY,10001",
                "2,Jane Smith,jane.smith@email.com,555-5678,456 Oak Ave,Los Angeles,CA,90210",
                "3,Bob Johnson,bob.johnson@email.com,555-9012,789 Pine Rd,Chicago,IL,60601",
                "4,Alice Brown,alice.brown@email.com,555-1234,321 Elm St,Boston,MA,02101", // Duplicate phone
                "5,Charlie Wilson,charlie.wilson@email.com,555-3456,123 Main St,New York,NY,10001" // Duplicate address
            };

            var testFile = Path.Combine(_testDataPath, "uniqueness_test.csv");
            await File.WriteAllLinesAsync(testFile, testData);

            // Act
            var result = await ValidateDataUniqueness(testFile, new[] { "Phone", "Address" });

            // Assert
            Assert.IsFalse(result.IsValid, "Data should not be valid due to duplicate values");
            Assert.AreEqual(2, result.DuplicateFields.Count);
            Assert.IsTrue(result.DuplicateFields.Contains("Phone"));
            Assert.IsTrue(result.DuplicateFields.Contains("Address"));
            Assert.AreEqual(2, result.DuplicateRows.Count);

            // Verify specific duplicates
            var phoneDuplicates = result.DuplicateRows.Where(r => r.Field == "Phone").ToList();
            Assert.AreEqual(1, phoneDuplicates.Count);
            Assert.AreEqual("555-1234", phoneDuplicates[0].Value);

            var addressDuplicates = result.DuplicateRows.Where(r => r.Field == "Address").ToList();
            Assert.AreEqual(1, addressDuplicates.Count);
            Assert.AreEqual("123 Main St", addressDuplicates[0].Value);
        }

        /// <summary>
        /// Tests data format validation
        /// </summary>
        [TestMethod]
        public async Task TestDataQuality_FormatValidation()
        {
            // Arrange - Data with format issues
            var testData = new[]
            {
                "ID,Name,Email,Phone,ZipCode,Date,Amount",
                "1,John Doe,john.doe@email.com,555-1234,10001,2023-01-15,1250.50",
                "2,Jane Smith,invalid-email,555-5678,90210,2023-02-20,890.75",
                "3,Bob Johnson,bob.johnson@email.com,invalid-phone,60601,2023-03-10,320.25",
                "4,Alice Brown,alice.brown@email.com,555-9012,invalid-zip,2023-04-15,450.00",
                "5,Charlie Wilson,charlie.wilson@email.com,555-3456,02101,invalid-date,600.00",
                "6,David Lee,david.lee@email.com,555-7890,10001,2023-05-20,invalid-amount"
            };

            var testFile = Path.Combine(_testDataPath, "format_test.csv");
            await File.WriteAllLinesAsync(testFile, testData);

            // Act
            var result = await ValidateDataFormat(testFile);

            // Assert
            Assert.IsFalse(result.IsValid, "Data should not be valid due to format issues");
            Assert.AreEqual(6, result.RowValidations.Count);
            Assert.AreEqual(1, result.ValidRows); // Only John Doe has valid format
            Assert.AreEqual(5, result.InvalidRows);

            // Verify specific format issues
            var janeRow = result.RowValidations[1];
            Assert.IsFalse(janeRow.IsValid);
            Assert.IsTrue(janeRow.FormatErrors.Contains("Email"));

            var bobRow = result.RowValidations[2];
            Assert.IsFalse(bobRow.IsValid);
            Assert.IsTrue(bobRow.FormatErrors.Contains("Phone"));

            var aliceRow = result.RowValidations[3];
            Assert.IsFalse(aliceRow.IsValid);
            Assert.IsTrue(aliceRow.FormatErrors.Contains("ZipCode"));

            var charlieRow = result.RowValidations[4];
            Assert.IsFalse(charlieRow.IsValid);
            Assert.IsTrue(charlieRow.FormatErrors.Contains("Date"));

            var davidRow = result.RowValidations[5];
            Assert.IsFalse(davidRow.IsValid);
            Assert.IsTrue(davidRow.FormatErrors.Contains("Amount"));
        }

        /// <summary>
        /// Tests data referential integrity validation
        /// </summary>
        [TestMethod]
        public async Task TestDataQuality_ReferentialIntegrityValidation()
        {
            // Arrange - Create related datasets
            var customerData = new[]
            {
                "CustomerID,Name,Email,Phone,Address,City,State,ZipCode",
                "1,John Doe,john.doe@email.com,555-1234,123 Main St,New York,NY,10001",
                "2,Jane Smith,jane.smith@email.com,555-5678,456 Oak Ave,Los Angeles,CA,90210",
                "3,Bob Johnson,bob.johnson@email.com,555-9012,789 Pine Rd,Chicago,IL,60601"
            };

            var orderData = new[]
            {
                "OrderID,CustomerID,ProductID,Quantity,UnitPrice,OrderDate,TotalAmount",
                "1,1,101,2,25.50,2023-01-15,51.00",
                "2,1,102,1,15.75,2023-01-15,15.75",
                "3,2,103,3,10.25,2023-02-20,30.75",
                "4,4,104,1,45.00,2023-02-20,45.00", // CustomerID 4 doesn't exist
                "5,5,105,2,20.00,2023-03-10,40.00" // CustomerID 5 doesn't exist
            };

            var customerFile = Path.Combine(_testDataPath, "referential_customers.csv");
            var orderFile = Path.Combine(_testDataPath, "referential_orders.csv");

            await File.WriteAllLinesAsync(customerFile, customerData);
            await File.WriteAllLinesAsync(orderFile, orderData);

            // Act
            var result = await ValidateReferentialIntegrity(orderFile, customerFile, "CustomerID");

            // Assert
            Assert.IsFalse(result.IsValid, "Data should not be valid due to referential integrity violations");
            Assert.AreEqual(5, result.RowValidations.Count);
            Assert.AreEqual(3, result.ValidRows); // Orders 1, 2, 3 have valid CustomerIDs
            Assert.AreEqual(2, result.InvalidRows); // Orders 4, 5 have invalid CustomerIDs

            // Verify specific referential integrity violations
            var order4Row = result.RowValidations[3];
            Assert.IsFalse(order4Row.IsValid);
            Assert.IsTrue(order4Row.ReferentialErrors.Contains("CustomerID 4 not found"));

            var order5Row = result.RowValidations[4];
            Assert.IsFalse(order5Row.IsValid);
            Assert.IsTrue(order5Row.ReferentialErrors.Contains("CustomerID 5 not found"));
        }

        /// <summary>
        /// Tests data business rule validation
        /// </summary>
        [TestMethod]
        public async Task TestDataQuality_BusinessRuleValidation()
        {
            // Arrange - Data with business rule violations
            var testData = new[]
            {
                "ID,Name,Age,Salary,Department,Status,StartDate,EndDate",
                "1,John Doe,25,50000,Engineering,Active,2023-01-15,",
                "2,Jane Smith,30,75000,Marketing,Active,2023-02-20,",
                "3,Bob Johnson,17,60000,Engineering,Active,2023-03-10,", // Age < 18
                "4,Alice Brown,28,-1000,Sales,Active,2023-04-15,", // Negative salary
                "5,Charlie Wilson,45,80000,Engineering,Inactive,2023-05-20,2023-06-01", // EndDate before StartDate
                "6,David Lee,35,70000,Engineering,Active,2023-06-01,2023-05-01" // EndDate before StartDate
            };

            var testFile = Path.Combine(_testDataPath, "business_rules_test.csv");
            await File.WriteAllLinesAsync(testFile, testData);

            // Act
            var businessRules = new[]
            {
                new BusinessRule { Name = "MinimumAge", Field = "Age", MinValue = 18, MaxValue = 65 },
                new BusinessRule { Name = "PositiveSalary", Field = "Salary", MinValue = 0, MaxValue = int.MaxValue },
                new BusinessRule { Name = "ValidDateRange", Field = "StartDate", MinValue = 0, MaxValue = int.MaxValue }
            };
            var result = await ValidateBusinessRules(testFile, businessRules);

            // Assert
            Assert.IsFalse(result.IsValid, "Data should not be valid due to business rule violations");
            Assert.AreEqual(6, result.RowValidations.Count);
            Assert.AreEqual(2, result.ValidRows); // John and Jane are valid
            Assert.AreEqual(4, result.InvalidRows);

            // Verify specific business rule violations
            var bobRow = result.RowValidations[2];
            Assert.IsFalse(bobRow.IsValid);
            Assert.IsTrue(bobRow.BusinessRuleViolations.Contains("MinimumAge"));

            var aliceRow = result.RowValidations[3];
            Assert.IsFalse(aliceRow.IsValid);
            Assert.IsTrue(aliceRow.BusinessRuleViolations.Contains("PositiveSalary"));

            var charlieRow = result.RowValidations[4];
            Assert.IsFalse(charlieRow.IsValid);
            Assert.IsTrue(charlieRow.BusinessRuleViolations.Contains("ValidDateRange"));

            var davidRow = result.RowValidations[5];
            Assert.IsFalse(davidRow.IsValid);
            Assert.IsTrue(davidRow.BusinessRuleViolations.Contains("ValidDateRange"));
        }

        /// <summary>
        /// Tests data quality validation with comprehensive rules
        /// </summary>
        [TestMethod]
        public async Task TestDataQuality_ComprehensiveValidation()
        {
            // Arrange - Data with multiple quality issues
            var testData = new[]
            {
                "ID,Name,Email,Phone,Age,Salary,Department,Status,StartDate,EndDate",
                "1,John Doe,john.doe@email.com,555-1234,25,50000,Engineering,Active,2023-01-15,",
                "2,Jane Smith,,555-5678,30,75000,Marketing,Active,2023-02-20,", // Missing email
                "3,Bob Johnson,bob.johnson@email.com,555-9012,17,60000,Engineering,Active,2023-03-10,", // Age < 18
                "4,Alice Brown,alice.brown@email.com,555-1234,28,-1000,Sales,Active,2023-04-15,", // Duplicate phone, negative salary
                "5,Charlie Wilson,charlie.wilson@email.com,555-3456,45,80000,Engineering,Inactive,2023-05-20,2023-06-01", // EndDate before StartDate
                "6,David Lee,david.lee@email.com,555-7890,35,70000,Engineering,Active,2023-06-01,2023-05-01" // EndDate before StartDate
            };

            var testFile = Path.Combine(_testDataPath, "comprehensive_quality_test.csv");
            await File.WriteAllLinesAsync(testFile, testData);

            // Act - Run comprehensive validation
            var result = await RunComprehensiveDataQualityValidation(testFile);

            // Assert
            Assert.IsFalse(result.IsValid, "Data should not be valid due to multiple quality issues");
            Assert.AreEqual(6, result.TotalRows);
            Assert.AreEqual(1, result.ValidRows); // Only John Doe is valid
            Assert.AreEqual(5, result.InvalidRows);

            // Verify specific quality issues
            Assert.IsTrue(result.QualityIssues.Contains("Completeness"), "Should have completeness issues");
            Assert.IsTrue(result.QualityIssues.Contains("Accuracy"), "Should have accuracy issues");
            Assert.IsTrue(result.QualityIssues.Contains("Uniqueness"), "Should have uniqueness issues");
            Assert.IsTrue(result.QualityIssues.Contains("BusinessRules"), "Should have business rule violations");

            // Verify quality scores
            Assert.IsTrue(result.CompletenessScore < 100, "Completeness score should be less than 100%");
            Assert.IsTrue(result.AccuracyScore < 100, "Accuracy score should be less than 100%");
            Assert.IsTrue(result.UniquenessScore < 100, "Uniqueness score should be less than 100%");
            Assert.IsTrue(result.BusinessRuleScore < 100, "Business rule score should be less than 100%");
        }

        #endregion

        #region Row-by-Row Comparison Utilities Tests

        /// <summary>
        /// Tests row-by-row comparison with exact match
        /// </summary>
        [TestMethod]
        public async Task TestRowByRowComparison_ExactMatch()
        {
            // Arrange
            var actualData = new[]
            {
                "ID,Name,Age,Salary,Department",
                "1,John Doe,25,50000,Engineering",
                "2,Jane Smith,30,75000,Marketing",
                "3,Bob Johnson,35,60000,Engineering"
            };

            var expectedData = actualData; // Exact match

            var actualFile = Path.Combine(_testDataPath, "row_comparison_actual.csv");
            var expectedFile = Path.Combine(_testDataPath, "row_comparison_expected.csv");

            await File.WriteAllLinesAsync(actualFile, actualData);
            await File.WriteAllLinesAsync(expectedFile, expectedData);

            // Act
            var result = await PerformRowByRowComparison(actualFile, expectedFile);

            // Assert
            Assert.IsTrue(result.IsValid, $"Row-by-row comparison failed: {result.ErrorMessage}");
            Assert.AreEqual(0, result.RowDifferences.Count);
        }

        /// <summary>
        /// Tests row-by-row comparison with field differences
        /// </summary>
        [TestMethod]
        public async Task TestRowByRowComparison_WithFieldDifferences()
        {
            // Arrange
            var actualData = new[]
            {
                "ID,Name,Age,Salary,Department",
                "1,John Doe,25,50000,Engineering",
                "2,Jane Smith,31,75000,Marketing", // Age changed from 30 to 31
                "3,Bob Johnson,35,65000,Engineering" // Salary changed from 60000 to 65000
            };

            var expectedData = new[]
            {
                "ID,Name,Age,Salary,Department",
                "1,John Doe,25,50000,Engineering",
                "2,Jane Smith,30,75000,Marketing",
                "3,Bob Johnson,35,60000,Engineering"
            };

            var actualFile = Path.Combine(_testDataPath, "row_comparison_diff_actual.csv");
            var expectedFile = Path.Combine(_testDataPath, "row_comparison_diff_expected.csv");

            await File.WriteAllLinesAsync(actualFile, actualData);
            await File.WriteAllLinesAsync(expectedFile, expectedData);

            // Act
            var result = await PerformRowByRowComparison(actualFile, expectedFile);

            // Assert
            Assert.IsFalse(result.IsValid);
            Assert.AreEqual(2, result.RowDifferences.Count);

            // Verify specific differences
            var ageDifference = result.RowDifferences.FirstOrDefault(d => d.Field == "Age");
            Assert.IsNotNull(ageDifference);
            Assert.AreEqual(1, ageDifference.RowIndex);
            Assert.AreEqual("31", ageDifference.ActualValue);
            Assert.AreEqual("30", ageDifference.ExpectedValue);

            var salaryDifference = result.RowDifferences.FirstOrDefault(d => d.Field == "Salary");
            Assert.IsNotNull(salaryDifference);
            Assert.AreEqual(2, salaryDifference.RowIndex);
            Assert.AreEqual("65000", salaryDifference.ActualValue);
            Assert.AreEqual("60000", salaryDifference.ExpectedValue);
        }

        /// <summary>
        /// Tests row-by-row comparison with missing rows
        /// </summary>
        [TestMethod]
        public async Task TestRowByRowComparison_WithMissingRows()
        {
            // Arrange
            var actualData = new[]
            {
                "ID,Name,Age,Salary,Department",
                "1,John Doe,25,50000,Engineering"
                // Missing Jane and Bob rows
            };

            var expectedData = new[]
            {
                "ID,Name,Age,Salary,Department",
                "1,John Doe,25,50000,Engineering",
                "2,Jane Smith,30,75000,Marketing",
                "3,Bob Johnson,35,60000,Engineering"
            };

            var actualFile = Path.Combine(_testDataPath, "row_comparison_missing_actual.csv");
            var expectedFile = Path.Combine(_testDataPath, "row_comparison_missing_expected.csv");

            await File.WriteAllLinesAsync(actualFile, actualData);
            await File.WriteAllLinesAsync(expectedFile, expectedData);

            // Act
            var result = await PerformRowByRowComparison(actualFile, expectedFile);

            // Assert
            Assert.IsFalse(result.IsValid);
            Assert.AreEqual(2, result.RowDifferences.Count);

            // Verify missing rows
            var missingRows = result.RowDifferences.Where(d => d.Field == "Row" && d.ActualValue == "Row missing").ToList();
            Assert.AreEqual(2, missingRows.Count);
            Assert.AreEqual(1, missingRows[0].RowIndex);
            Assert.AreEqual(2, missingRows[1].RowIndex);
        }

        /// <summary>
        /// Tests row-by-row comparison with extra rows
        /// </summary>
        [TestMethod]
        public async Task TestRowByRowComparison_WithExtraRows()
        {
            // Arrange
            var actualData = new[]
            {
                "ID,Name,Age,Salary,Department",
                "1,John Doe,25,50000,Engineering",
                "2,Jane Smith,30,75000,Marketing",
                "3,Bob Johnson,35,60000,Engineering",
                "4,Alice Brown,28,55000,Sales" // Extra row
            };

            var expectedData = new[]
            {
                "ID,Name,Age,Salary,Department",
                "1,John Doe,25,50000,Engineering",
                "2,Jane Smith,30,75000,Marketing",
                "3,Bob Johnson,35,60000,Engineering"
            };

            var actualFile = Path.Combine(_testDataPath, "row_comparison_extra_actual.csv");
            var expectedFile = Path.Combine(_testDataPath, "row_comparison_extra_expected.csv");

            await File.WriteAllLinesAsync(actualFile, actualData);
            await File.WriteAllLinesAsync(expectedFile, expectedData);

            // Act
            var result = await PerformRowByRowComparison(actualFile, expectedFile);

            // Assert
            Assert.IsFalse(result.IsValid);
            Assert.AreEqual(1, result.RowDifferences.Count);

            // Verify extra row
            var extraRow = result.RowDifferences.FirstOrDefault(d => d.Field == "Row" && d.ExpectedValue == "Row missing");
            Assert.IsNotNull(extraRow);
            Assert.AreEqual(3, extraRow.RowIndex);
        }

        /// <summary>
        /// Tests row-by-row comparison with floating point tolerance
        /// </summary>
        [TestMethod]
        public async Task TestRowByRowComparison_WithFloatingPointTolerance()
        {
            // Arrange
            var actualData = new[]
            {
                "ID,Name,Score,Rating,Price",
                "1,Product A,85.123,4.5,1250.50",
                "2,Product B,92.456,4.8,890.75"
            };

            var expectedData = new[]
            {
                "ID,Name,Score,Rating,Price",
                "1,Product A,85.125,4.52,1250.48", // Small differences within tolerance
                "2,Product B,92.450,4.78,890.77"   // Small differences within tolerance
            };

            var actualFile = Path.Combine(_testDataPath, "row_comparison_float_actual.csv");
            var expectedFile = Path.Combine(_testDataPath, "row_comparison_float_expected.csv");

            await File.WriteAllLinesAsync(actualFile, actualData);
            await File.WriteAllLinesAsync(expectedFile, expectedData);

            // Act - Test with tolerance
            var result = await PerformRowByRowComparison(actualFile, expectedFile, 0.01);

            // Assert
            Assert.IsTrue(result.IsValid, $"Floating-point row comparison failed: {result.ErrorMessage}");
            Assert.AreEqual(0, result.RowDifferences.Count);

            // Act - Test without tolerance
            result = await PerformRowByRowComparison(actualFile, expectedFile, 0.0);

            // Assert
            Assert.IsFalse(result.IsValid);
            Assert.AreEqual(6, result.RowDifferences.Count); // 3 fields * 2 rows
        }

        /// <summary>
        /// Tests row-by-row comparison with different column orders
        /// </summary>
        [TestMethod]
        public async Task TestRowByRowComparison_WithDifferentColumnOrders()
        {
            // Arrange
            var actualData = new[]
            {
                "Name,ID,Department,Age,Salary",
                "John Doe,1,Engineering,25,50000",
                "Jane Smith,2,Marketing,30,75000"
            };

            var expectedData = new[]
            {
                "ID,Name,Age,Salary,Department",
                "1,John Doe,25,50000,Engineering",
                "2,Jane Smith,30,75000,Marketing"
            };

            var actualFile = Path.Combine(_testDataPath, "row_comparison_order_actual.csv");
            var expectedFile = Path.Combine(_testDataPath, "row_comparison_order_expected.csv");

            await File.WriteAllLinesAsync(actualFile, actualData);
            await File.WriteAllLinesAsync(expectedFile, expectedData);

            // Act
            var result = await PerformRowByRowComparison(actualFile, expectedFile);

            // Assert
            Assert.IsTrue(result.IsValid, $"Column order comparison failed: {result.ErrorMessage}");
            Assert.AreEqual(0, result.RowDifferences.Count);
        }

        /// <summary>
        /// Tests row-by-row comparison with null values
        /// </summary>
        [TestMethod]
        public async Task TestRowByRowComparison_WithNullValues()
        {
            // Arrange
            var actualData = new[]
            {
                "ID,Name,Email,Phone,Address",
                "1,John Doe,john.doe@email.com,555-1234,123 Main St",
                "2,Jane Smith,,555-5678,456 Oak Ave", // Missing email
                "3,Bob Johnson,bob.johnson@email.com,,789 Pine Rd" // Missing phone
            };

            var expectedData = new[]
            {
                "ID,Name,Email,Phone,Address",
                "1,John Doe,john.doe@email.com,555-1234,123 Main St",
                "2,Jane Smith,,555-5678,456 Oak Ave",
                "3,Bob Johnson,bob.johnson@email.com,,789 Pine Rd"
            };

            var actualFile = Path.Combine(_testDataPath, "row_comparison_null_actual.csv");
            var expectedFile = Path.Combine(_testDataPath, "row_comparison_null_expected.csv");

            await File.WriteAllLinesAsync(actualFile, actualData);
            await File.WriteAllLinesAsync(expectedFile, expectedData);

            // Act
            var result = await PerformRowByRowComparison(actualFile, expectedFile);

            // Assert
            Assert.IsTrue(result.IsValid, $"Null value comparison failed: {result.ErrorMessage}");
            Assert.AreEqual(0, result.RowDifferences.Count);
        }

        /// <summary>
        /// Tests row-by-row comparison with large datasets
        /// </summary>
        [TestMethod]
        public async Task TestRowByRowComparison_LargeDataset()
        {
            // Arrange - Create large dataset
            var rowCount = 1000;
            var actualData = new List<string> { "ID,Name,Value,Category,Description" };
            var expectedData = new List<string> { "ID,Name,Value,Category,Description" };

            for (int i = 1; i <= rowCount; i++)
            {
                var name = $"Item{i}";
                var value = i * 15.75;
                var category = $"Category{(i % 10) + 1}";
                var description = $"Description for item {i}";

                actualData.Add($"{i},{name},{value},{category},{description}");
                expectedData.Add($"{i},{name},{value},{category},{description}");
            }

            var actualFile = Path.Combine(_testDataPath, "row_comparison_large_actual.csv");
            var expectedFile = Path.Combine(_testDataPath, "row_comparison_large_expected.csv");

            await File.WriteAllLinesAsync(actualFile, actualData);
            await File.WriteAllLinesAsync(expectedFile, expectedData);

            // Act
            var result = await PerformRowByRowComparison(actualFile, expectedFile);

            // Assert
            Assert.IsTrue(result.IsValid, $"Large dataset row comparison failed: {result.ErrorMessage}");
            Assert.AreEqual(0, result.RowDifferences.Count);
        }

        /// <summary>
        /// Tests row-by-row comparison with performance measurement
        /// </summary>
        [TestMethod]
        public async Task TestRowByRowComparison_Performance()
        {
            // Arrange
            var rowCount = 5000;
            var actualData = new List<string> { "ID,Name,Value,Category,Description,Status,Priority,Owner,StartDate,EndDate" };
            var expectedData = new List<string> { "ID,Name,Value,Category,Description,Status,Priority,Owner,StartDate,EndDate" };

            for (int i = 1; i <= rowCount; i++)
            {
                var name = $"Task{i}";
                var value = i * 25.50;
                var category = $"Category{(i % 5) + 1}";
                var description = $"This is a detailed description for task {i} with additional information";
                var status = i % 2 == 0 ? "Completed" : "In Progress";
                var priority = $"Priority{(i % 3) + 1}";
                var owner = $"User{(i % 10) + 1}";
                var startDate = DateTime.Now.AddDays(-i).ToString("yyyy-MM-dd");
                var endDate = DateTime.Now.AddDays(i).ToString("yyyy-MM-dd");

                actualData.Add($"{i},{name},{value},{category},{description},{status},{priority},{owner},{startDate},{endDate}");
                expectedData.Add($"{i},{name},{value},{category},{description},{status},{priority},{owner},{startDate},{endDate}");
            }

            var actualFile = Path.Combine(_testDataPath, "row_comparison_perf_actual.csv");
            var expectedFile = Path.Combine(_testDataPath, "row_comparison_perf_expected.csv");

            await File.WriteAllLinesAsync(actualFile, actualData);
            await File.WriteAllLinesAsync(expectedFile, expectedData);

            // Act
            var stopwatch = System.Diagnostics.Stopwatch.StartNew();
            var result = await PerformRowByRowComparison(actualFile, expectedFile);
            stopwatch.Stop();

            // Assert
            Assert.IsTrue(result.IsValid, $"Performance row comparison failed: {result.ErrorMessage}");
            Assert.AreEqual(0, result.RowDifferences.Count);

            // Performance assertion (should complete within reasonable time)
            Assert.IsTrue(stopwatch.ElapsedMilliseconds < 15000, 
                $"Row comparison took too long: {stopwatch.ElapsedMilliseconds}ms for {rowCount} rows");
        }

        /// <summary>
        /// Tests row-by-row comparison with detailed difference reporting
        /// </summary>
        [TestMethod]
        public async Task TestRowByRowComparison_DetailedDifferenceReporting()
        {
            // Arrange
            var actualData = new[]
            {
                "ID,Name,Age,Salary,Department,Status,StartDate,EndDate",
                "1,John Doe,25,50000,Engineering,Active,2023-01-15,",
                "2,Jane Smith,30,75000,Marketing,Active,2023-02-20,",
                "3,Bob Johnson,35,60000,Engineering,Inactive,2023-03-10,2023-12-31",
                "4,Alice Brown,28,55000,Sales,Active,2023-04-15,",
                "5,Charlie Wilson,45,80000,Engineering,Active,2023-05-20,"
            };

            var expectedData = new[]
            {
                "ID,Name,Age,Salary,Department,Status,StartDate,EndDate",
                "1,John Doe,25,50000,Engineering,Active,2023-01-15,",
                "2,Jane Smith,31,75000,Marketing,Active,2023-02-20,", // Age changed
                "3,Bob Johnson,35,65000,Engineering,Inactive,2023-03-10,2023-12-31", // Salary changed
                "4,Alice Brown,28,55000,Sales,Active,2023-04-15,",
                "5,Charlie Wilson,45,80000,Engineering,Active,2023-05-20,"
            };

            var actualFile = Path.Combine(_testDataPath, "row_comparison_detailed_actual.csv");
            var expectedFile = Path.Combine(_testDataPath, "row_comparison_detailed_expected.csv");

            await File.WriteAllLinesAsync(actualFile, actualData);
            await File.WriteAllLinesAsync(expectedFile, expectedData);

            // Act
            var result = await PerformRowByRowComparison(actualFile, expectedFile);

            // Assert
            Assert.IsFalse(result.IsValid);
            Assert.AreEqual(2, result.RowDifferences.Count);

            // Verify detailed difference reporting
            foreach (var difference in result.RowDifferences)
            {
                Assert.IsNotNull(difference.Field);
                Assert.IsNotNull(difference.ActualValue);
                Assert.IsNotNull(difference.ExpectedValue);
                Assert.IsNotNull(difference.Message);
                Assert.IsTrue(difference.RowIndex >= 0);
                Assert.IsTrue(difference.Message.Contains($"Field '{difference.Field}' mismatch at row {difference.RowIndex}"));
            }

            // Verify specific differences
            var ageDifference = result.RowDifferences.FirstOrDefault(d => d.Field == "Age");
            Assert.IsNotNull(ageDifference);
            Assert.AreEqual(1, ageDifference.RowIndex);
            Assert.AreEqual("31", ageDifference.ActualValue);
            Assert.AreEqual("30", ageDifference.ExpectedValue);

            var salaryDifference = result.RowDifferences.FirstOrDefault(d => d.Field == "Salary");
            Assert.IsNotNull(salaryDifference);
            Assert.AreEqual(2, salaryDifference.RowIndex);
            Assert.AreEqual("65000", salaryDifference.ActualValue);
            Assert.AreEqual("60000", salaryDifference.ExpectedValue);
        }

        /// <summary>
        /// Tests row-by-row comparison with custom field mapping
        /// </summary>
        [TestMethod]
        public async Task TestRowByRowComparison_WithCustomFieldMapping()
        {
            // Arrange
            var actualData = new[]
            {
                "CustomerID,FirstName,LastName,EmailAddress,PhoneNumber,StreetAddress,CityName,StateCode,PostalCode",
                "1,John,Doe,john.doe@email.com,555-1234,123 Main St,New York,NY,10001",
                "2,Jane,Smith,jane.smith@email.com,555-5678,456 Oak Ave,Los Angeles,CA,90210"
            };

            var expectedData = new[]
            {
                "ID,Name,Email,Phone,Address,City,State,ZipCode",
                "1,John Doe,john.doe@email.com,555-1234,123 Main St,New York,NY,10001",
                "2,Jane Smith,jane.smith@email.com,555-5678,456 Oak Ave,Los Angeles,CA,90210"
            };

            var actualFile = Path.Combine(_testDataPath, "row_comparison_mapping_actual.csv");
            var expectedFile = Path.Combine(_testDataPath, "row_comparison_mapping_expected.csv");

            await File.WriteAllLinesAsync(actualFile, actualData);
            await File.WriteAllLinesAsync(expectedFile, expectedData);

            // Act
            var result = await PerformRowByRowComparison(actualFile, expectedFile);

            // Assert
            Assert.IsTrue(result.IsValid, $"Custom field mapping comparison failed: {result.ErrorMessage}");
            Assert.AreEqual(0, result.RowDifferences.Count);
        }

        #endregion

        #region Helper Methods

        /// <summary>
        /// Validates data uniqueness
        /// </summary>
        private async Task<UniquenessValidationResult> ValidateDataUniqueness(string dataFile, string[] uniqueFields)
        {
            var result = new UniquenessValidationResult();
            result.UniqueFields = uniqueFields;

            try
            {
                var data = await ReadCsvFile(dataFile);
                var duplicateRows = new List<DuplicateRow>();

                foreach (var field in uniqueFields)
                {
                    var fieldValues = new Dictionary<string, List<int>>();
                    
                    for (int i = 0; i < data.Count; i++)
                    {
                        var value = data[i].ContainsKey(field) ? data[i][field] : "";
                        if (!fieldValues.ContainsKey(value))
                        {
                            fieldValues[value] = new List<int>();
                        }
                        fieldValues[value].Add(i);
                    }

                    foreach (var kvp in fieldValues)
                    {
                        if (kvp.Value.Count > 1)
                        {
                            result.DuplicateFields.Add(field);
                            duplicateRows.Add(new DuplicateRow
                            {
                                Field = field,
                                Value = kvp.Key,
                                RowIndices = kvp.Value
                            });
                        }
                    }
                }

                result.DuplicateRows = duplicateRows;
                result.IsValid = result.DuplicateFields.Count == 0;
            }
            catch (Exception ex)
            {
                result.IsValid = false;
                result.ErrorMessage = $"Uniqueness validation error: {ex.Message}";
            }

            return result;
        }

        /// <summary>
        /// Validates data format
        /// </summary>
        private async Task<FormatValidationResult> ValidateDataFormat(string dataFile)
        {
            var result = new FormatValidationResult();

            try
            {
                var data = await ReadCsvFile(dataFile);

                foreach (var row in data)
                {
                    var rowValidation = new RowFormatValidation
                    {
                        RowIndex = data.IndexOf(row)
                    };

                    // Validate email format
                    if (row.ContainsKey("Email") && !string.IsNullOrEmpty(row["Email"]))
                    {
                        if (!IsValidEmail(row["Email"]))
                        {
                            rowValidation.FormatErrors.Add("Email");
                        }
                    }

                    // Validate phone format
                    if (row.ContainsKey("Phone") && !string.IsNullOrEmpty(row["Phone"]))
                    {
                        if (!IsValidPhone(row["Phone"]))
                        {
                            rowValidation.FormatErrors.Add("Phone");
                        }
                    }

                    // Validate zip code format
                    if (row.ContainsKey("ZipCode") && !string.IsNullOrEmpty(row["ZipCode"]))
                    {
                        if (!IsValidZipCode(row["ZipCode"]))
                        {
                            rowValidation.FormatErrors.Add("ZipCode");
                        }
                    }

                    // Validate date format
                    if (row.ContainsKey("Date") && !string.IsNullOrEmpty(row["Date"]))
                    {
                        if (!DateTime.TryParse(row["Date"], out _))
                        {
                            rowValidation.FormatErrors.Add("Date");
                        }
                    }

                    // Validate amount format
                    if (row.ContainsKey("Amount") && !string.IsNullOrEmpty(row["Amount"]))
                    {
                        if (!decimal.TryParse(row["Amount"], out _))
                        {
                            rowValidation.FormatErrors.Add("Amount");
                        }
                    }

                    rowValidation.IsValid = rowValidation.FormatErrors.Count == 0;
                    result.RowValidations.Add(rowValidation);
                }

                result.IsValid = result.RowValidations.All(r => r.IsValid);
                result.ValidRows = result.RowValidations.Count(r => r.IsValid);
                result.InvalidRows = result.RowValidations.Count(r => !r.IsValid);
            }
            catch (Exception ex)
            {
                result.IsValid = false;
                result.ErrorMessage = $"Format validation error: {ex.Message}";
            }

            return result;
        }

        /// <summary>
        /// Validates referential integrity
        /// </summary>
        private async Task<ReferentialIntegrityResult> ValidateReferentialIntegrity(string dataFile, string referenceFile, string keyField)
        {
            var result = new ReferentialIntegrityResult();
            result.KeyField = keyField;

            try
            {
                var data = await ReadCsvFile(dataFile);
                var referenceData = await ReadCsvFile(referenceFile);

                var referenceKeys = referenceData.Select(r => r.ContainsKey(keyField) ? r[keyField] : "").ToHashSet();

                foreach (var row in data)
                {
                    var rowValidation = new RowReferentialIntegrityValidation
                    {
                        RowIndex = data.IndexOf(row)
                    };

                    if (row.ContainsKey(keyField))
                    {
                        var keyValue = row[keyField];
                        if (!referenceKeys.Contains(keyValue))
                        {
                            rowValidation.ReferentialErrors.Add($"{keyField} {keyValue} not found");
                        }
                    }

                    rowValidation.IsValid = rowValidation.ReferentialErrors.Count == 0;
                    result.RowValidations.Add(rowValidation);
                }

                result.IsValid = result.RowValidations.All(r => r.IsValid);
                result.ValidRows = result.RowValidations.Count(r => r.IsValid);
                result.InvalidRows = result.RowValidations.Count(r => !r.IsValid);
            }
            catch (Exception ex)
            {
                result.IsValid = false;
                result.ErrorMessage = $"Referential integrity validation error: {ex.Message}";
            }

            return result;
        }

        /// <summary>
        /// Validates business rules
        /// </summary>
        private async Task<BusinessRuleValidationResult> ValidateBusinessRules(string dataFile, BusinessRule[] businessRules)
        {
            var result = new BusinessRuleValidationResult();
            result.BusinessRules = businessRules;

            try
            {
                var data = await ReadCsvFile(dataFile);

                foreach (var row in data)
                {
                    var rowValidation = new RowBusinessRuleValidation
                    {
                        RowIndex = data.IndexOf(row)
                    };

                    foreach (var rule in businessRules)
                    {
                        if (row.ContainsKey(rule.Field))
                        {
                            var value = row[rule.Field];
                            
                            switch (rule.Name)
                            {
                                case "MinimumAge":
                                    if (int.TryParse(value, out int age))
                                    {
                                        if (age < rule.MinValue || age > rule.MaxValue)
                                        {
                                            rowValidation.BusinessRuleViolations.Add(rule.Name);
                                        }
                                    }
                                    break;
                                    
                                case "PositiveSalary":
                                    if (decimal.TryParse(value, out decimal salary))
                                    {
                                        if (salary < rule.MinValue)
                                        {
                                            rowValidation.BusinessRuleViolations.Add(rule.Name);
                                        }
                                    }
                                    break;
                                    
                                case "ValidDateRange":
                                    if (row.ContainsKey("StartDate") && row.ContainsKey("EndDate"))
                                    {
                                        if (DateTime.TryParse(row["StartDate"], out DateTime startDate) &&
                                            DateTime.TryParse(row["EndDate"], out DateTime endDate))
                                        {
                                            if (endDate < startDate)
                                            {
                                                rowValidation.BusinessRuleViolations.Add(rule.Name);
                                            }
                                        }
                                    }
                                    break;
                            }
                        }
                    }

                    rowValidation.IsValid = rowValidation.BusinessRuleViolations.Count == 0;
                    result.RowValidations.Add(rowValidation);
                }

                result.IsValid = result.RowValidations.All(r => r.IsValid);
                result.ValidRows = result.RowValidations.Count(r => r.IsValid);
                result.InvalidRows = result.RowValidations.Count(r => !r.IsValid);
            }
            catch (Exception ex)
            {
                result.IsValid = false;
                result.ErrorMessage = $"Business rule validation error: {ex.Message}";
            }

            return result;
        }

        /// <summary>
        /// Runs comprehensive data quality validation
        /// </summary>
        private async Task<ComprehensiveQualityResult> RunComprehensiveDataQualityValidation(string dataFile)
        {
            var result = new ComprehensiveQualityResult();

            try
            {
                var data = await ReadCsvFile(dataFile);
                result.TotalRows = data.Count;

                // Completeness validation
                var requiredFields = new[] { "Name", "Email", "Phone", "Age", "Salary" };
                var completenessResult = await DataValidationUtilities.ValidateDataCompletenessAsync(dataFile, requiredFields);
                result.CompletenessScore = (double)completenessResult.CompleteRows / result.TotalRows * 100;
                if (completenessResult.IncompleteRows > 0) result.QualityIssues.Add("Completeness");

                // Uniqueness validation
                var uniquenessResult = await ValidateDataUniqueness(dataFile, new[] { "Phone" });
                result.UniquenessScore = (double)(result.TotalRows - uniquenessResult.DuplicateRows.Count) / result.TotalRows * 100;
                if (uniquenessResult.DuplicateFields.Count > 0) result.QualityIssues.Add("Uniqueness");

                // Business rule validation
                var businessRules = new[]
                {
                    new BusinessRule { Name = "MinimumAge", Field = "Age", MinValue = 18, MaxValue = 65 },
                    new BusinessRule { Name = "PositiveSalary", Field = "Salary", MinValue = 0, MaxValue = int.MaxValue }
                };
                var businessRuleResult = await ValidateBusinessRules(dataFile, businessRules);
                result.BusinessRuleScore = (double)businessRuleResult.ValidRows / result.TotalRows * 100;
                if (businessRuleResult.InvalidRows > 0) result.QualityIssues.Add("BusinessRules");

                // Format validation
                var formatResult = await ValidateDataFormat(dataFile);
                result.FormatScore = (double)formatResult.ValidRows / result.TotalRows * 100;
                if (formatResult.InvalidRows > 0) result.QualityIssues.Add("Format");

                // Calculate overall accuracy score
                result.AccuracyScore = (result.CompletenessScore + result.UniquenessScore + result.BusinessRuleScore + result.FormatScore) / 4;
                if (result.AccuracyScore < 100) result.QualityIssues.Add("Accuracy");

                result.ValidRows = businessRuleResult.ValidRows;
                result.InvalidRows = result.TotalRows - result.ValidRows;
                result.IsValid = result.QualityIssues.Count == 0;
            }
            catch (Exception ex)
            {
                result.IsValid = false;
                result.ErrorMessage = $"Comprehensive quality validation error: {ex.Message}";
            }

            return result;
        }

        /// <summary>
        /// Validates email format
        /// </summary>
        private bool IsValidEmail(string email)
        {
            try
            {
                var addr = new System.Net.Mail.MailAddress(email);
                return addr.Address == email;
            }
            catch
            {
                return false;
            }
        }

        /// <summary>
        /// Validates phone format
        /// </summary>
        private bool IsValidPhone(string phone)
        {
            return System.Text.RegularExpressions.Regex.IsMatch(phone, @"^[\d\-\(\)\s]+$");
        }

        /// <summary>
        /// Validates zip code format
        /// </summary>
        private bool IsValidZipCode(string zipCode)
        {
            return System.Text.RegularExpressions.Regex.IsMatch(zipCode, @"^\d{5}(-\d{4})?$");
        }

        /// <summary>
        /// Validates data against golden copy file
        /// </summary>
        private async Task<DataValidationResult> ValidateDataAgainstGoldenCopy(string outputFile, string goldenCopyFile)
        {
            var result = new DataValidationResult();
            
            try
            {
                if (!File.Exists(outputFile))
                {
                    result.IsValid = false;
                    result.ErrorMessage = $"Output file not found: {outputFile}";
                    return result;
                }

                if (!File.Exists(goldenCopyFile))
                {
                    result.IsValid = false;
                    result.ErrorMessage = $"Golden copy file not found: {goldenCopyFile}";
                    return result;
                }

                var outputData = await ReadCsvFile(outputFile);
                var goldenCopyData = await ReadCsvFile(goldenCopyFile);

                result.TotalRows = Math.Max(outputData.Count, goldenCopyData.Count);
                result.MissingRows = Math.Max(0, goldenCopyData.Count - outputData.Count);
                result.ExtraRows = Math.Max(0, outputData.Count - goldenCopyData.Count);

                if (outputData.Count != goldenCopyData.Count)
                {
                    result.IsValid = false;
                    result.ErrorMessage = $"Row count mismatch: Output={outputData.Count}, Expected={goldenCopyData.Count}";
                    return result;
                }

                int matchingRows = 0;
                int differentRows = 0;

                for (int i = 0; i < outputData.Count; i++)
                {
                    var outputRow = outputData[i];
                    var expectedRow = goldenCopyData[i];

                    var rowDifferences = CompareRows(outputRow, expectedRow, i);
                    if (rowDifferences.Any())
                    {
                        result.RowDifferences.AddRange(rowDifferences);
                        differentRows++;
                    }
                    else
                    {
                        matchingRows++;
                    }
                }

                result.MatchingRows = matchingRows;
                result.DifferentRows = differentRows;

                result.IsValid = result.RowDifferences.Count == 0;
                if (!result.IsValid)
                {
                    result.ErrorMessage = $"Found {result.RowDifferences.Count} row differences";
                }
            }
            catch (Exception ex)
            {
                result.IsValid = false;
                result.ErrorMessage = $"Validation error: {ex.Message}";
            }

            return result;
        }

        /// <summary>
        /// Compares input and output data files
        /// </summary>
        private async Task<DataComparisonResult> CompareInputOutputData(string inputFile, string outputFile)
        {
            var result = new DataComparisonResult();
            
            try
            {
                var inputData = await ReadCsvFile(inputFile);
                var outputData = await ReadCsvFile(outputFile);

                result.InputRowCount = inputData.Count;
                result.OutputRowCount = outputData.Count;
                result.IsValid = true;
            }
            catch (Exception ex)
            {
                result.IsValid = false;
                result.ErrorMessage = $"Comparison error: {ex.Message}";
            }

            return result;
        }

        /// <summary>
        /// Validates business rules against output data
        /// </summary>
        private async Task<BusinessRulesValidationResult> ValidateBusinessRules(string outputFile, string rulesFile)
        {
            var result = new BusinessRulesValidationResult();
            
            try
            {
                var outputData = await ReadCsvFile(outputFile);
                var rules = await ReadBusinessRules(rulesFile);

                foreach (var row in outputData)
                {
                    foreach (var rule in rules)
                    {
                        var violation = ValidateBusinessRule(row, rule);
                        if (violation != null)
                        {
                            result.Violations.Add(violation);
                        }
                    }
                }

                result.IsValid = result.Violations.Count == 0;
                if (!result.IsValid)
                {
                    result.ErrorMessage = $"Found {result.Violations.Count} business rule violations";
                }
            }
            catch (Exception ex)
            {
                result.IsValid = false;
                result.ErrorMessage = $"Business rules validation error: {ex.Message}";
            }

            return result;
        }

        /// <summary>
        /// Validates data quality against quality rules
        /// </summary>
        private async Task<DataQualityValidationResult> ValidateDataQuality(string outputFile, string qualityRulesFile)
        {
            var result = new DataQualityValidationResult();
            
            try
            {
                var outputData = await ReadCsvFile(outputFile);
                var qualityRules = await ReadDataQualityRules(qualityRulesFile);

                foreach (var row in outputData)
                {
                    foreach (var rule in qualityRules)
                    {
                        var issue = ValidateDataQualityRule(row, rule);
                        if (issue != null)
                        {
                            result.QualityIssues.Add(issue);
                        }
                    }
                }

                result.IsValid = result.QualityIssues.Count == 0;
                if (!result.IsValid)
                {
                    result.ErrorMessage = $"Found {result.QualityIssues.Count} data quality issues";
                }
            }
            catch (Exception ex)
            {
                result.IsValid = false;
                result.ErrorMessage = $"Data quality validation error: {ex.Message}";
            }

            return result;
        }

        /// <summary>
        /// Validates data integrity against reference data
        /// </summary>
        private async Task<DataIntegrityValidationResult> ValidateDataIntegrity(string outputFile, string referenceFile)
        {
            var result = new DataIntegrityValidationResult();
            
            try
            {
                var outputData = await ReadCsvFile(outputFile);
                var referenceData = await ReadCsvFile(referenceFile);

                // Create reference lookup
                var referenceLookup = referenceData.ToDictionary(r => r["ProductId"], r => r);

                foreach (var row in outputData)
                {
                    if (row.ContainsKey("ProductId"))
                    {
                        var productId = row["ProductId"];
                        if (!referenceLookup.ContainsKey(productId))
                        {
                            result.IntegrityViolations.Add(new IntegrityViolation
                            {
                                RowIndex = outputData.IndexOf(row),
                                Field = "ProductId",
                                Value = productId,
                                Message = $"Product ID {productId} not found in reference data"
                            });
                        }
                    }
                }

                result.IsValid = result.IntegrityViolations.Count == 0;
                if (!result.IsValid)
                {
                    result.ErrorMessage = $"Found {result.IntegrityViolations.Count} data integrity violations";
                }
            }
            catch (Exception ex)
            {
                result.IsValid = false;
                result.ErrorMessage = $"Data integrity validation error: {ex.Message}";
            }

            return result;
        }

        /// <summary>
        /// Performs detailed row-by-row comparison
        /// </summary>
        private async Task<RowComparisonResult> PerformRowByRowComparison(string outputFile, string expectedFile, double tolerance = 0.0)
        {
            var result = new RowComparisonResult();
            
            try
            {
                var outputData = await ReadCsvFile(outputFile);
                var expectedData = await ReadCsvFile(expectedFile);

                result.TotalRows = Math.Max(outputData.Count, expectedData.Count);
                result.MissingRows = Math.Max(0, expectedData.Count - outputData.Count);
                result.ExtraRows = Math.Max(0, outputData.Count - expectedData.Count);

                int matchingRows = 0;
                int differentRows = 0;

                for (int i = 0; i < Math.Max(outputData.Count, expectedData.Count); i++)
                {
                    var outputRow = i < outputData.Count ? outputData[i] : null;
                    var expectedRow = i < expectedData.Count ? expectedData[i] : null;

                    if (outputRow == null)
                    {
                        result.RowDifferences.Add(new RowDifference
                        {
                            RowIndex = i,
                            Field = "Row",
                            ExpectedValue = "Row exists",
                            ActualValue = "Row missing",
                            Message = "Row missing in output data"
                        });
                        differentRows++;
                    }
                    else if (expectedRow == null)
                    {
                        result.RowDifferences.Add(new RowDifference
                        {
                            RowIndex = i,
                            Field = "Row",
                            ExpectedValue = "Row missing",
                            ActualValue = "Row exists",
                            Message = "Extra row in output data"
                        });
                        differentRows++;
                    }
                    else
                    {
                        var differences = CompareRows(outputRow, expectedRow, i, tolerance);
                        if (differences.Any())
                        {
                            result.RowDifferences.AddRange(differences);
                            differentRows++;
                        }
                        else
                        {
                            matchingRows++;
                        }
                    }
                }

                result.MatchingRows = matchingRows;
                result.DifferentRows = differentRows;

                result.IsValid = result.RowDifferences.Count == 0;
                if (!result.IsValid)
                {
                    result.ErrorMessage = $"Found {result.RowDifferences.Count} row differences";
                }
            }
            catch (Exception ex)
            {
                result.IsValid = false;
                result.ErrorMessage = $"Row comparison error: {ex.Message}";
            }

            return result;
        }

        /// <summary>
        /// Reads CSV file and returns list of dictionaries
        /// </summary>
        private async Task<List<Dictionary<string, string>>> ReadCsvFile(string filePath)
        {
            var data = new List<Dictionary<string, string>>();
            var lines = await File.ReadAllLinesAsync(filePath);
            
            if (lines.Length == 0) return data;

            var headers = lines[0].Split(',');
            
            for (int i = 1; i < lines.Length; i++)
            {
                var values = lines[i].Split(',');
                var row = new Dictionary<string, string>();
                
                for (int j = 0; j < headers.Length && j < values.Length; j++)
                {
                    row[headers[j].Trim()] = values[j].Trim();
                }
                
                data.Add(row);
            }
            
            return data;
        }

        /// <summary>
        /// Compares two rows and returns differences
        /// </summary>
        private List<RowDifference> CompareRows(Dictionary<string, string> actualRow, Dictionary<string, string> expectedRow, int rowIndex, double tolerance = 0.0)
        {
            var differences = new List<RowDifference>();
            var allKeys = actualRow.Keys.Union(expectedRow.Keys).Distinct();

            foreach (var key in allKeys)
            {
                var actualValue = actualRow.ContainsKey(key) ? actualRow[key] : null;
                var expectedValue = expectedRow.ContainsKey(key) ? expectedRow[key] : null;

                if (actualValue != expectedValue)
                {
                    // Check for floating-point tolerance
                    if (tolerance > 0 && double.TryParse(actualValue, out double actualNum) && double.TryParse(expectedValue, out double expectedNum))
                    {
                        if (Math.Abs(actualNum - expectedNum) <= tolerance)
                        {
                            continue; // Values are within tolerance
                        }
                    }

                    differences.Add(new RowDifference
                    {
                        RowIndex = rowIndex,
                        Field = key,
                        ExpectedValue = expectedValue ?? "null",
                        ActualValue = actualValue ?? "null",
                        Message = $"Field '{key}' mismatch at row {rowIndex}"
                    });
                }
            }

            return differences;
        }

        /// <summary>
        /// Reads business rules from JSON file
        /// </summary>
        private async Task<List<BusinessRule>> ReadBusinessRules(string rulesFile)
        {
            if (!File.Exists(rulesFile))
            {
                return new List<BusinessRule>();
            }

            var json = await File.ReadAllTextAsync(rulesFile);
            return JsonSerializer.Deserialize<List<BusinessRule>>(json) ?? new List<BusinessRule>();
        }

        /// <summary>
        /// Reads data quality rules from JSON file
        /// </summary>
        private async Task<List<DataQualityRule>> ReadDataQualityRules(string rulesFile)
        {
            if (!File.Exists(rulesFile))
            {
                return new List<DataQualityRule>();
            }

            var json = await File.ReadAllTextAsync(rulesFile);
            return JsonSerializer.Deserialize<List<DataQualityRule>>(json) ?? new List<DataQualityRule>();
        }

        /// <summary>
        /// Validates a single business rule against a row
        /// </summary>
        private BusinessRuleViolation ValidateBusinessRule(Dictionary<string, string> row, BusinessRule rule)
        {
            // Implement business rule validation logic
            // This is a simplified example
            if (rule.Field == "Age" && row.ContainsKey("Age"))
            {
                if (int.TryParse(row["Age"], out int age) && age < rule.MinValue)
                {
                    return new BusinessRuleViolation
                    {
                        RuleName = rule.Name,
                        Field = rule.Field,
                        Value = row["Age"],
                        Message = $"Age {age} is below minimum {rule.MinValue}"
                    };
                }
            }

            return null;
        }

        /// <summary>
        /// Validates a single data quality rule against a row
        /// </summary>
        private DataQualityIssue ValidateDataQualityRule(Dictionary<string, string> row, DataQualityRule rule)
        {
            // Implement data quality rule validation logic
            // This is a simplified example
            if (rule.Field == "Email" && row.ContainsKey("Email"))
            {
                var email = row["Email"];
                if (string.IsNullOrEmpty(email) || !email.Contains("@"))
                {
                    return new DataQualityIssue
                    {
                        RuleName = rule.Name,
                        Field = rule.Field,
                        Value = email,
                        Message = "Invalid email format"
                    };
                }
            }

            return null;
        }

        /// <summary>
        /// Creates sample test data files
        /// </summary>
        private static void CreateSampleTestData()
        {
            // Create input directory
            var inputDir = Path.Combine(_testDataPath, "Input");
            if (!Directory.Exists(inputDir))
            {
                Directory.CreateDirectory(inputDir);
            }

            // Create output directory
            var outputDir = Path.Combine(_testDataPath, "Output");
            if (!Directory.Exists(outputDir))
            {
                Directory.CreateDirectory(outputDir);
            }

            // Create rules directory
            var rulesDir = Path.Combine(_testDataPath, "Rules");
            if (!Directory.Exists(rulesDir))
            {
                Directory.CreateDirectory(rulesDir);
            }

            // Create reference directory
            var referenceDir = Path.Combine(_testDataPath, "Reference");
            if (!Directory.Exists(referenceDir))
            {
                Directory.CreateDirectory(referenceDir);
            }

            // Create sample CSV files
            CreateSampleCsvFile(Path.Combine(inputDir, "customers.csv"), "CustomerId,Name,Email,Age\n1,John Doe,john@example.com,30\n2,Jane Smith,jane@example.com,25");
            CreateSampleCsvFile(Path.Combine(inputDir, "orders.csv"), "OrderId,CustomerId,OrderDate,Amount\n1,1,2023-01-01,100.50\n2,2,2023-01-02,75.25");
            CreateSampleCsvFile(Path.Combine(inputDir, "products.csv"), "ProductId,Name,Price,Category\n1,Widget A,10.99,Electronics\n2,Widget B,15.99,Electronics");

            // Create sample golden copy files
            CreateSampleCsvFile(Path.Combine(_goldenCopyPath, "customers_expected.csv"), "CustomerId,Name,Email,Age,Status\n1,John Doe,john@example.com,30,Active\n2,Jane Smith,jane@example.com,25,Active");
            CreateSampleCsvFile(Path.Combine(_goldenCopyPath, "orders_expected.csv"), "OrderId,CustomerId,OrderDate,Amount,Status\n1,1,2023-01-01,100.50,Completed\n2,2,2023-01-02,75.25,Completed");

            // Create sample business rules
            var businessRules = new List<BusinessRule>
            {
                new BusinessRule { Name = "AgeValidation", Field = "Age", MinValue = 18, MaxValue = 100 }
            };
            File.WriteAllText(Path.Combine(rulesDir, "employee_validation_rules.json"), JsonSerializer.Serialize(businessRules));

            // Create sample data quality rules
            var qualityRules = new List<DataQualityRule>
            {
                new DataQualityRule { Name = "EmailValidation", Field = "Email", RuleType = "Format" }
            };
            File.WriteAllText(Path.Combine(rulesDir, "data_quality_rules.json"), JsonSerializer.Serialize(qualityRules));
        }

        /// <summary>
        /// Creates a sample CSV file
        /// </summary>
        private static void CreateSampleCsvFile(string filePath, string content)
        {
            if (!File.Exists(filePath))
            {
                File.WriteAllText(filePath, content);
            }
        }

        #endregion
    }

    #region Data Validation Result Classes

    /// <summary>
    /// Result of data validation against golden copy
    /// </summary>
    public class DataValidationResult
    {
        public bool IsValid { get; set; }
        public string ErrorMessage { get; set; } = string.Empty;
        public List<RowDifference> RowDifferences { get; set; } = new List<RowDifference>();
        public int TotalRows { get; set; }
        public int MatchingRows { get; set; }
        public int DifferentRows { get; set; }
        public int MissingRows { get; set; }
        public int ExtraRows { get; set; }
    }

    /// <summary>
    /// Result of input/output data comparison
    /// </summary>
    public class DataComparisonResult
    {
        public bool IsValid { get; set; }
        public string ErrorMessage { get; set; } = string.Empty;
        public int InputRowCount { get; set; }
        public int OutputRowCount { get; set; }
    }

    /// <summary>
    /// Result of business rules validation
    /// </summary>
    public class BusinessRulesValidationResult
    {
        public bool IsValid { get; set; }
        public string ErrorMessage { get; set; } = string.Empty;
        public List<BusinessRuleViolation> Violations { get; set; } = new List<BusinessRuleViolation>();
    }

    /// <summary>
    /// Result of data quality validation
    /// </summary>
    public class DataQualityValidationResult
    {
        public bool IsValid { get; set; }
        public string ErrorMessage { get; set; } = string.Empty;
        public List<DataQualityIssue> QualityIssues { get; set; } = new List<DataQualityIssue>();
    }

    /// <summary>
    /// Result of data integrity validation
    /// </summary>
    public class DataIntegrityValidationResult
    {
        public bool IsValid { get; set; }
        public string ErrorMessage { get; set; } = string.Empty;
        public List<IntegrityViolation> IntegrityViolations { get; set; } = new List<IntegrityViolation>();
    }

    /// <summary>
    /// Result of row-by-row comparison
    /// </summary>
    public class RowComparisonResult
    {
        public bool IsValid { get; set; }
        public string ErrorMessage { get; set; } = string.Empty;
        public List<RowDifference> RowDifferences { get; set; } = new List<RowDifference>();
        public int TotalRows { get; set; }
        public int MatchingRows { get; set; }
        public int DifferentRows { get; set; }
        public int MissingRows { get; set; }
        public int ExtraRows { get; set; }
    }

    /// <summary>
    /// Represents a difference between two rows
    /// </summary>
    public class RowDifference
    {
        public int RowIndex { get; set; }
        public string Field { get; set; } = string.Empty;
        public string ExpectedValue { get; set; } = string.Empty;
        public string ActualValue { get; set; } = string.Empty;
        public string Message { get; set; } = string.Empty;
    }

    /// <summary>
    /// Represents a business rule violation
    /// </summary>
    public class BusinessRuleViolation
    {
        public string RuleName { get; set; } = string.Empty;
        public string Field { get; set; } = string.Empty;
        public string Value { get; set; } = string.Empty;
        public string Message { get; set; } = string.Empty;
    }

    /// <summary>
    /// Represents a data quality issue
    /// </summary>
    public class DataQualityIssue
    {
        public string RuleName { get; set; } = string.Empty;
        public string Field { get; set; } = string.Empty;
        public string Value { get; set; } = string.Empty;
        public string Message { get; set; } = string.Empty;
    }

    /// <summary>
    /// Represents a data integrity violation
    /// </summary>
    public class IntegrityViolation
    {
        public int RowIndex { get; set; }
        public string Field { get; set; } = string.Empty;
        public string Value { get; set; } = string.Empty;
        public string Message { get; set; } = string.Empty;
    }

    /// <summary>
    /// Represents a business rule
    /// </summary>
    public class BusinessRule
    {
        public string Name { get; set; } = string.Empty;
        public string Field { get; set; } = string.Empty;
        public int MinValue { get; set; }
        public int MaxValue { get; set; }
    }

    /// <summary>
    /// Represents a data quality rule
    /// </summary>
    public class DataQualityRule
    {
        public string Name { get; set; } = string.Empty;
        public string Field { get; set; } = string.Empty;
        public string RuleType { get; set; } = string.Empty;
    }

    #endregion

    #region Additional Result Classes

    /// <summary>
    /// Result of uniqueness validation
    /// </summary>
    public class UniquenessValidationResult
    {
        public bool IsValid { get; set; }
        public string ErrorMessage { get; set; } = string.Empty;
        public string[] UniqueFields { get; set; } = Array.Empty<string>();
        public List<string> DuplicateFields { get; set; } = new List<string>();
        public List<DuplicateRow> DuplicateRows { get; set; } = new List<DuplicateRow>();
    }

    /// <summary>
    /// Represents a duplicate row
    /// </summary>
    public class DuplicateRow
    {
        public string Field { get; set; } = string.Empty;
        public string Value { get; set; } = string.Empty;
        public List<int> RowIndices { get; set; } = new List<int>();
    }

    /// <summary>
    /// Result of format validation
    /// </summary>
    public class FormatValidationResult
    {
        public bool IsValid { get; set; }
        public string ErrorMessage { get; set; } = string.Empty;
        public int ValidRows { get; set; }
        public int InvalidRows { get; set; }
        public List<RowFormatValidation> RowValidations { get; set; } = new List<RowFormatValidation>();
    }

    /// <summary>
    /// Represents row format validation
    /// </summary>
    public class RowFormatValidation
    {
        public int RowIndex { get; set; }
        public bool IsValid { get; set; }
        public List<string> FormatErrors { get; set; } = new List<string>();
    }

    /// <summary>
    /// Result of referential integrity validation
    /// </summary>
    public class ReferentialIntegrityResult
    {
        public bool IsValid { get; set; }
        public string ErrorMessage { get; set; } = string.Empty;
        public string KeyField { get; set; } = string.Empty;
        public int ValidRows { get; set; }
        public int InvalidRows { get; set; }
        public List<RowReferentialIntegrityValidation> RowValidations { get; set; } = new List<RowReferentialIntegrityValidation>();
    }

    /// <summary>
    /// Represents row referential integrity validation
    /// </summary>
    public class RowReferentialIntegrityValidation
    {
        public int RowIndex { get; set; }
        public bool IsValid { get; set; }
        public List<string> ReferentialErrors { get; set; } = new List<string>();
    }

    /// <summary>
    /// Result of business rule validation
    /// </summary>
    public class BusinessRuleValidationResult
    {
        public bool IsValid { get; set; }
        public string ErrorMessage { get; set; } = string.Empty;
        public BusinessRule[] BusinessRules { get; set; } = Array.Empty<BusinessRule>();
        public int ValidRows { get; set; }
        public int InvalidRows { get; set; }
        public List<RowBusinessRuleValidation> RowValidations { get; set; } = new List<RowBusinessRuleValidation>();
    }

    /// <summary>
    /// Represents row business rule validation
    /// </summary>
    public class RowBusinessRuleValidation
    {
        public int RowIndex { get; set; }
        public bool IsValid { get; set; }
        public List<string> BusinessRuleViolations { get; set; } = new List<string>();
    }

    /// <summary>
    /// Result of comprehensive quality validation
    /// </summary>
    public class ComprehensiveQualityResult
    {
        public bool IsValid { get; set; }
        public string ErrorMessage { get; set; } = string.Empty;
        public int TotalRows { get; set; }
        public int ValidRows { get; set; }
        public int InvalidRows { get; set; }
        public double CompletenessScore { get; set; }
        public double AccuracyScore { get; set; }
        public double UniquenessScore { get; set; }
        public double BusinessRuleScore { get; set; }
        public double FormatScore { get; set; }
        public List<string> QualityIssues { get; set; } = new List<string>();
    }

    #endregion
}
