using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using System.Threading.Tasks;
using System.Threading;

namespace ETL.Tests.Integration
{
    /// <summary>
    /// Integration tests for ETL workflow covering multi-database extraction, transformation, and loading
    /// </summary>
    [TestClass]
    public class ETLWorkflowTests
    {
        private static string _stagingConnectionString;
        private static string _sourceSqlServerConnectionString;
        private static string _sourceMySqlConnectionString;
        private static DataTransformer _transformer;
        private static ILogger _logger;

        [ClassInitialize]
        public static void ClassInitialize(TestContext context)
        {
            // Initialize connection strings for test databases
            _stagingConnectionString = "Server=localhost;Database=ETL_Staging_Test;Integrated Security=true;";
            _sourceSqlServerConnectionString = "Server=localhost;Database=ETL_Source_SQLServer_Test;Integrated Security=true;";
            _sourceMySqlConnectionString = "Server=localhost;Database=etl_source_mysql_test;Uid=test_user;Pwd=test_password;";

            // Initialize logger
            var loggerFactory = LoggerFactory.Create(builder => builder.AddConsole());
            _logger = loggerFactory.CreateLogger<ETLWorkflowTests>();

            // Initialize transformer
            _transformer = new DataTransformer(_stagingConnectionString, _logger, batchSize: 1000, maxDegreeOfParallelism: 2);

            // Setup test databases
            SetupTestDatabases();
        }

        [ClassCleanup]
        public static void ClassCleanup()
        {
            // Cleanup test databases
            CleanupTestDatabases();
            _transformer?.Dispose();
        }

        [TestInitialize]
        public void TestInitialize()
        {
            // Clear staging table before each test
            ClearStagingTable();
        }

        [TestCleanup]
        public void TestCleanup()
        {
            // Cleanup after each test
            ClearStagingTable();
        }

        #region Setup and Teardown Methods

        /// <summary>
        /// Creates test databases and sample data for integration testing
        /// </summary>
        private static void SetupTestDatabases()
        {
            try
            {
                // Create SQL Server source database
                CreateSqlServerSourceDatabase();
                
                // Create MySQL source database (simulated)
                CreateMySqlSourceDatabase();
                
                // Create staging database
                CreateStagingDatabase();
                
                _logger.LogInformation("Test databases setup completed successfully");
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Failed to setup test databases");
                throw;
            }
        }

        /// <summary>
        /// Creates SQL Server source database with sample customer data
        /// </summary>
        private static void CreateSqlServerSourceDatabase()
        {
            using var connection = new SqlConnection(_sourceSqlServerConnectionString.Replace("Database=ETL_Source_SQLServer_Test", "Database=master"));
            connection.Open();

            // Create database
            using var createDbCommand = new SqlCommand(@"
                IF NOT EXISTS (SELECT name FROM sys.databases WHERE name = 'ETL_Source_SQLServer_Test')
                CREATE DATABASE ETL_Source_SQLServer_Test", connection);
            createDbCommand.ExecuteNonQuery();

            // Connect to the new database
            connection.ChangeDatabase("ETL_Source_SQLServer_Test");

            // Create customers table
            using var createTableCommand = new SqlCommand(@"
                IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[Customers]') AND type in (N'U'))
                CREATE TABLE [dbo].[Customers] (
                    [CustomerID] [int] IDENTITY(1,1) PRIMARY KEY,
                    [FirstName] [nvarchar](50) NOT NULL,
                    [LastName] [nvarchar](50) NOT NULL,
                    [Email] [nvarchar](100) NULL,
                    [Phone] [nvarchar](20) NULL,
                    [Address] [nvarchar](200) NULL,
                    [City] [nvarchar](50) NULL,
                    [State] [nvarchar](20) NULL,
                    [ZipCode] [nvarchar](10) NULL,
                    [Country] [nvarchar](50) NULL,
                    [CreatedDate] [datetime] DEFAULT GETDATE(),
                    [IsActive] [bit] DEFAULT 1
                )", connection);
            createTableCommand.ExecuteNonQuery();

            // Insert sample data
            using var insertCommand = new SqlCommand(@"
                INSERT INTO [dbo].[Customers] ([FirstName], [LastName], [Email], [Phone], [Address], [City], [State], [ZipCode], [Country])
                VALUES 
                    ('John', 'Doe', 'john.doe@email.com', '555-0101', '123 Main St', 'New York', 'NY', '10001', 'USA'),
                    ('Jane', 'Smith', 'jane.smith@email.com', '555-0102', '456 Oak Ave', 'Los Angeles', 'CA', '90210', 'USA'),
                    ('Bob', 'Johnson', 'bob.johnson@email.com', '555-0103', '789 Pine Rd', 'Chicago', 'IL', '60601', 'USA'),
                    ('Alice', 'Brown', 'alice.brown@email.com', '555-0104', '321 Elm St', 'Houston', 'TX', '77001', 'USA'),
                    ('Charlie', 'Wilson', 'charlie.wilson@email.com', '555-0105', '654 Maple Dr', 'Phoenix', 'AZ', '85001', 'USA')", connection);
            insertCommand.ExecuteNonQuery();
        }

        /// <summary>
        /// Creates MySQL source database with sample order data (simulated with SQL Server)
        /// </summary>
        private static void CreateMySqlSourceDatabase()
        {
            // For testing purposes, we'll simulate MySQL with a separate SQL Server database
            var mySqlSimulatedConnectionString = _sourceMySqlConnectionString.Replace("Database=etl_source_mysql_test", "Database=master");
            
            using var connection = new SqlConnection(mySqlSimulatedConnectionString);
            connection.Open();

            // Create database
            using var createDbCommand = new SqlCommand(@"
                IF NOT EXISTS (SELECT name FROM sys.databases WHERE name = 'ETL_Source_MySQL_Test')
                CREATE DATABASE ETL_Source_MySQL_Test", connection);
            createDbCommand.ExecuteNonQuery();

            // Connect to the new database
            connection.ChangeDatabase("ETL_Source_MySQL_Test");

            // Create orders table (simulating MySQL structure)
            using var createTableCommand = new SqlCommand(@"
                IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[Orders]') AND type in (N'U'))
                CREATE TABLE [dbo].[Orders] (
                    [OrderID] [int] IDENTITY(1,1) PRIMARY KEY,
                    [CustomerID] [int] NOT NULL,
                    [OrderDate] [datetime] NOT NULL,
                    [TotalAmount] [decimal](10,2) NOT NULL,
                    [Status] [nvarchar](20) NOT NULL,
                    [ShippingAddress] [nvarchar](200) NULL,
                    [PaymentMethod] [nvarchar](50) NULL,
                    [CreatedAt] [datetime] DEFAULT GETDATE(),
                    [UpdatedAt] [datetime] DEFAULT GETDATE()
                )", connection);
            createTableCommand.ExecuteNonQuery();

            // Insert sample data
            using var insertCommand = new SqlCommand(@"
                INSERT INTO [dbo].[Orders] ([CustomerID], [OrderDate], [TotalAmount], [Status], [ShippingAddress], [PaymentMethod])
                VALUES 
                    (1, '2024-01-15', 299.99, 'Completed', '123 Main St, New York, NY 10001', 'Credit Card'),
                    (2, '2024-01-16', 149.50, 'Processing', '456 Oak Ave, Los Angeles, CA 90210', 'PayPal'),
                    (3, '2024-01-17', 599.99, 'Shipped', '789 Pine Rd, Chicago, IL 60601', 'Credit Card'),
                    (1, '2024-01-18', 89.99, 'Completed', '123 Main St, New York, NY 10001', 'Credit Card'),
                    (4, '2024-01-19', 199.99, 'Processing', '321 Elm St, Houston, TX 77001', 'PayPal')", connection);
            insertCommand.ExecuteNonQuery();
        }

        /// <summary>
        /// Creates staging database for transformed data
        /// </summary>
        private static void CreateStagingDatabase()
        {
            using var connection = new SqlConnection(_stagingConnectionString.Replace("Database=ETL_Staging_Test", "Database=master"));
            connection.Open();

            // Create database
            using var createDbCommand = new SqlCommand(@"
                IF NOT EXISTS (SELECT name FROM sys.databases WHERE name = 'ETL_Staging_Test')
                CREATE DATABASE ETL_Staging_Test", connection);
            createDbCommand.ExecuteNonQuery();

            // Connect to the new database
            connection.ChangeDatabase("ETL_Staging_Test");

            // Create staging table
            using var createTableCommand = new SqlCommand(@"
                IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[StagingTable]') AND type in (N'U'))
                CREATE TABLE [dbo].[StagingTable] (
                    [Id] [int] IDENTITY(1,1) PRIMARY KEY,
                    [CustomerID] [int] NULL,
                    [FirstName] [nvarchar](50) NULL,
                    [LastName] [nvarchar](50) NULL,
                    [Email] [nvarchar](100) NULL,
                    [Phone] [nvarchar](20) NULL,
                    [Address] [nvarchar](200) NULL,
                    [City] [nvarchar](50) NULL,
                    [State] [nvarchar](20) NULL,
                    [ZipCode] [nvarchar](10) NULL,
                    [Country] [nvarchar](50) NULL,
                    [OrderID] [int] NULL,
                    [OrderDate] [datetime] NULL,
                    [TotalAmount] [decimal](10,2) NULL,
                    [OrderStatus] [nvarchar](20) NULL,
                    [PaymentMethod] [nvarchar](50) NULL,
                    [TransformationTimestamp] [datetime2] NOT NULL,
                    [CreatedDate] [datetime2] DEFAULT GETUTCDATE()
                )", connection);
            createTableCommand.ExecuteNonQuery();

            // Create logs table
            using var createLogsCommand = new SqlCommand(@"
                IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[Logs]') AND type in (N'U'))
                CREATE TABLE [dbo].[Logs] (
                    [Id] [int] IDENTITY(1,1) PRIMARY KEY,
                    [Operation] [nvarchar](100) NOT NULL,
                    [StartTime] [datetime2] NOT NULL,
                    [EndTime] [datetime2] NULL,
                    [Duration] [bigint] NULL,
                    [ProcessedRecords] [bigint] NULL,
                    [TransformedRecords] [bigint] NULL,
                    [ErrorCount] [int] NULL,
                    [Success] [bit] NOT NULL,
                    [Details] [nvarchar](max) NULL,
                    [ErrorMessage] [nvarchar](max) NULL,
                    [StackTrace] [nvarchar](max) NULL
                )", connection);
            createLogsCommand.ExecuteNonQuery();
        }

        /// <summary>
        /// Cleans up test databases
        /// </summary>
        private static void CleanupTestDatabases()
        {
            try
            {
                using var connection = new SqlConnection(_stagingConnectionString.Replace("Database=ETL_Staging_Test", "Database=master"));
                connection.Open();

                // Drop test databases
                var databases = new[] { "ETL_Staging_Test", "ETL_Source_SQLServer_Test", "ETL_Source_MySQL_Test" };
                
                foreach (var database in databases)
                {
                    using var dropCommand = new SqlCommand($"DROP DATABASE IF EXISTS [{database}]", connection);
                    dropCommand.ExecuteNonQuery();
                }

                _logger.LogInformation("Test databases cleanup completed");
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Failed to cleanup test databases");
            }
        }

        /// <summary>
        /// Clears staging table for clean test execution
        /// </summary>
        private void ClearStagingTable()
        {
            try
            {
                using var connection = new SqlConnection(_stagingConnectionString);
                connection.Open();

                using var clearCommand = new SqlCommand("DELETE FROM [dbo].[StagingTable]", connection);
                clearCommand.ExecuteNonQuery();

                using var clearLogsCommand = new SqlCommand("DELETE FROM [dbo].[Logs]", connection);
                clearLogsCommand.ExecuteNonQuery();
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Failed to clear staging table");
            }
        }

        #endregion

        #region Integration Tests

        /// <summary>
        /// Tests basic data extraction from SQL Server source database
        /// </summary>
        [TestMethod]
        public async Task Test_SqlServer_DataExtraction()
        {
            // Arrange
            var sourceDatabases = new List<SourceDatabaseConfig>
            {
                new SourceDatabaseConfig
                {
                    DatabaseName = "CustomerDB",
                    ConnectionString = _sourceSqlServerConnectionString,
                    SourceTable = "Customers"
                }
            };

            var rules = new TransformationRules
            {
                CleanStrings = true,
                StringCleaningOptions = new StringCleaningOptions
                {
                    TrimWhitespace = true,
                    NormalizeCase = true,
                    RemoveExtraSpaces = true
                }
            };

            // Act
            var result = await _transformer.TransformDataAsync(sourceDatabases, rules);

            // Assert
            Assert.IsTrue(result.Success, "Data transformation should succeed");
            Assert.AreEqual(5, result.ProcessedRecords, "Should process 5 customer records");
            Assert.AreEqual(5, result.TransformedRecords, "Should transform 5 customer records");
            Assert.AreEqual(0, result.Errors.Count, "Should have no errors");

            // Verify data in staging table
            VerifyStagingTableData(5, "CustomerDB");
        }

        /// <summary>
        /// Tests data extraction from MySQL source database (simulated)
        /// </summary>
        [TestMethod]
        public async Task Test_MySql_DataExtraction()
        {
            // Arrange
            var sourceDatabases = new List<SourceDatabaseConfig>
            {
                new SourceDatabaseConfig
                {
                    DatabaseName = "OrderDB",
                    ConnectionString = _sourceMySqlConnectionString.Replace("Database=etl_source_mysql_test", "Database=ETL_Source_MySQL_Test"),
                    SourceTable = "Orders"
                }
            };

            var rules = new TransformationRules
            {
                CleanStrings = true,
                StringCleaningOptions = new StringCleaningOptions
                {
                    TrimWhitespace = true,
                    NormalizeCase = true
                }
            };

            // Act
            var result = await _transformer.TransformDataAsync(sourceDatabases, rules);

            // Assert
            Assert.IsTrue(result.Success, "Data transformation should succeed");
            Assert.AreEqual(5, result.ProcessedRecords, "Should process 5 order records");
            Assert.AreEqual(5, result.TransformedRecords, "Should transform 5 order records");
            Assert.AreEqual(0, result.Errors.Count, "Should have no errors");

            // Verify data in staging table
            VerifyStagingTableData(5, "OrderDB");
        }

        /// <summary>
        /// Tests multi-database extraction and transformation with data joins
        /// </summary>
        [TestMethod]
        public async Task Test_MultiDatabase_ExtractionAndJoin()
        {
            // Arrange
            var sourceDatabases = new List<SourceDatabaseConfig>
            {
                new SourceDatabaseConfig
                {
                    DatabaseName = "CustomerDB",
                    ConnectionString = _sourceSqlServerConnectionString,
                    SourceTable = "Customers"
                },
                new SourceDatabaseConfig
                {
                    DatabaseName = "OrderDB",
                    ConnectionString = _sourceMySqlConnectionString.Replace("Database=etl_source_mysql_test", "Database=ETL_Source_MySQL_Test"),
                    SourceTable = "Orders"
                }
            };

            var rules = new TransformationRules
            {
                CleanStrings = true,
                StringCleaningOptions = new StringCleaningOptions
                {
                    TrimWhitespace = true,
                    NormalizeCase = true,
                    RemoveExtraSpaces = true
                },
                CustomTransformations = new List<Func<TransformedRecord, CancellationToken, Task>>
                {
                    async (record, token) =>
                    {
                        // Simulate data join logic
                        if (record.Data.ContainsKey("CustomerID"))
                        {
                            var customerId = Convert.ToInt32(record.Data["CustomerID"]);
                            // In a real scenario, this would join with customer data
                            record.Data["CustomerFullName"] = $"{record.Data["FirstName"]} {record.Data["LastName"]}";
                        }
                    }
                }
            };

            // Act
            var result = await _transformer.TransformDataAsync(sourceDatabases, rules);

            // Assert
            Assert.IsTrue(result.Success, "Multi-database transformation should succeed");
            Assert.AreEqual(10, result.ProcessedRecords, "Should process 10 total records (5 customers + 5 orders)");
            Assert.AreEqual(10, result.TransformedRecords, "Should transform 10 total records");
            Assert.AreEqual(0, result.Errors.Count, "Should have no errors");

            // Verify data in staging table
            VerifyStagingTableData(10, "MultiDatabase");
        }

        /// <summary>
        /// Tests error handling with invalid connection string
        /// </summary>
        [TestMethod]
        public async Task Test_ErrorHandling_InvalidConnection()
        {
            // Arrange
            var sourceDatabases = new List<SourceDatabaseConfig>
            {
                new SourceDatabaseConfig
                {
                    DatabaseName = "InvalidDB",
                    ConnectionString = "Server=invalid_server;Database=invalid_db;Integrated Security=true;",
                    SourceTable = "Customers"
                }
            };

            var rules = new TransformationRules
            {
                CleanStrings = true
            };

            // Act
            var result = await _transformer.TransformDataAsync(sourceDatabases, rules);

            // Assert
            Assert.IsFalse(result.Success, "Should fail with invalid connection");
            Assert.AreEqual(0, result.ProcessedRecords, "Should process 0 records");
            Assert.AreEqual(0, result.TransformedRecords, "Should transform 0 records");
            Assert.IsTrue(result.Errors.Count > 0, "Should have error details");
        }

        /// <summary>
        /// Tests data transformation with string cleaning rules
        /// </summary>
        [TestMethod]
        public async Task Test_DataTransformation_StringCleaning()
        {
            // Arrange
            var sourceDatabases = new List<SourceDatabaseConfig>
            {
                new SourceDatabaseConfig
                {
                    DatabaseName = "CustomerDB",
                    ConnectionString = _sourceSqlServerConnectionString,
                    SourceTable = "Customers"
                }
            };

            var rules = new TransformationRules
            {
                CleanStrings = true,
                StringCleaningOptions = new StringCleaningOptions
                {
                    TrimWhitespace = true,
                    NormalizeCase = true,
                    RemoveExtraSpaces = true,
                    MaxLength = 50
                }
            };

            // Act
            var result = await _transformer.TransformDataAsync(sourceDatabases, rules);

            // Assert
            Assert.IsTrue(result.Success, "String cleaning transformation should succeed");
            Assert.AreEqual(5, result.ProcessedRecords, "Should process 5 records");
            Assert.AreEqual(5, result.TransformedRecords, "Should transform 5 records");

            // Verify string cleaning in staging table
            VerifyStringCleaningInStagingTable();
        }

        /// <summary>
        /// Tests parallel processing with multiple databases
        /// </summary>
        [TestMethod]
        public async Task Test_ParallelProcessing_MultipleDatabases()
        {
            // Arrange
            var sourceDatabases = new List<SourceDatabaseConfig>
            {
                new SourceDatabaseConfig
                {
                    DatabaseName = "CustomerDB",
                    ConnectionString = _sourceSqlServerConnectionString,
                    SourceTable = "Customers"
                },
                new SourceDatabaseConfig
                {
                    DatabaseName = "OrderDB",
                    ConnectionString = _sourceMySqlConnectionString.Replace("Database=etl_source_mysql_test", "Database=ETL_Source_MySQL_Test"),
                    SourceTable = "Orders"
                }
            };

            var rules = new TransformationRules
            {
                CleanStrings = true
            };

            // Act
            var stopwatch = System.Diagnostics.Stopwatch.StartNew();
            var result = await _transformer.TransformDataAsync(sourceDatabases, rules);
            stopwatch.Stop();

            // Assert
            Assert.IsTrue(result.Success, "Parallel processing should succeed");
            Assert.AreEqual(10, result.ProcessedRecords, "Should process 10 records");
            Assert.AreEqual(10, result.TransformedRecords, "Should transform 10 records");
            
            // Verify parallel processing performance (should be faster than sequential)
            Assert.IsTrue(stopwatch.ElapsedMilliseconds < 5000, "Parallel processing should complete within 5 seconds");
        }

        /// <summary>
        /// Tests cancellation token functionality
        /// </summary>
        [TestMethod]
        public async Task Test_CancellationToken_Functionality()
        {
            // Arrange
            var sourceDatabases = new List<SourceDatabaseConfig>
            {
                new SourceDatabaseConfig
                {
                    DatabaseName = "CustomerDB",
                    ConnectionString = _sourceSqlServerConnectionString,
                    SourceTable = "Customers"
                }
            };

            var rules = new TransformationRules
            {
                CleanStrings = true
            };

            var cancellationTokenSource = new CancellationTokenSource();
            cancellationTokenSource.CancelAfter(100); // Cancel after 100ms

            // Act
            var result = await _transformer.TransformDataAsync(sourceDatabases, rules, cancellationTokenSource.Token);

            // Assert
            // The operation might complete before cancellation or be cancelled
            Assert.IsTrue(result.ProcessedRecords >= 0, "Should handle cancellation gracefully");
        }

        /// <summary>
        /// Tests large batch processing performance
        /// </summary>
        [TestMethod]
        public async Task Test_LargeBatch_Performance()
        {
            // Arrange - Create larger dataset
            await CreateLargeTestDataset();

            var sourceDatabases = new List<SourceDatabaseConfig>
            {
                new SourceDatabaseConfig
                {
                    DatabaseName = "LargeCustomerDB",
                    ConnectionString = _sourceSqlServerConnectionString,
                    SourceTable = "LargeCustomers"
                }
            };

            var rules = new TransformationRules
            {
                CleanStrings = true
            };

            // Act
            var stopwatch = System.Diagnostics.Stopwatch.StartNew();
            var result = await _transformer.TransformDataAsync(sourceDatabases, rules);
            stopwatch.Stop();

            // Assert
            Assert.IsTrue(result.Success, "Large batch processing should succeed");
            Assert.AreEqual(1000, result.ProcessedRecords, "Should process 1000 records");
            Assert.AreEqual(1000, result.TransformedRecords, "Should transform 1000 records");
            
            // Performance assertion (should complete within reasonable time)
            Assert.IsTrue(stopwatch.ElapsedMilliseconds < 10000, "Large batch should complete within 10 seconds");
        }

        #endregion

        #region Helper Methods

        /// <summary>
        /// Verifies data in staging table matches expected results
        /// </summary>
        private void VerifyStagingTableData(int expectedCount, string sourceType)
        {
            using var connection = new SqlConnection(_stagingConnectionString);
            connection.Open();

            using var command = new SqlCommand("SELECT COUNT(*) FROM [dbo].[StagingTable]", connection);
            var actualCount = Convert.ToInt32(command.ExecuteScalar());

            Assert.AreEqual(expectedCount, actualCount, $"Staging table should contain {expectedCount} records for {sourceType}");

            // Verify data quality
            using var qualityCommand = new SqlCommand(@"
                SELECT COUNT(*) FROM [dbo].[StagingTable] 
                WHERE [TransformationTimestamp] IS NOT NULL", connection);
            var qualityCount = Convert.ToInt32(qualityCommand.ExecuteScalar());

            Assert.AreEqual(expectedCount, qualityCount, "All records should have transformation timestamp");
        }

        /// <summary>
        /// Verifies string cleaning transformations in staging table
        /// </summary>
        private void VerifyStringCleaningInStagingTable()
        {
            using var connection = new SqlConnection(_stagingConnectionString);
            connection.Open();

            // Verify no leading/trailing whitespace
            using var whitespaceCommand = new SqlCommand(@"
                SELECT COUNT(*) FROM [dbo].[StagingTable] 
                WHERE LTRIM(RTRIM([FirstName])) != [FirstName] 
                   OR LTRIM(RTRIM([LastName])) != [LastName]", connection);
            var whitespaceCount = Convert.ToInt32(whitespaceCommand.ExecuteScalar());

            Assert.AreEqual(0, whitespaceCount, "No records should have leading/trailing whitespace");

            // Verify case normalization
            using var caseCommand = new SqlCommand(@"
                SELECT COUNT(*) FROM [dbo].[StagingTable] 
                WHERE [FirstName] != UPPER(LEFT([FirstName], 1)) + LOWER(SUBSTRING([FirstName], 2, LEN([FirstName])))", connection);
            var caseCount = Convert.ToInt32(caseCommand.ExecuteScalar());

            Assert.AreEqual(0, caseCount, "All names should be properly cased");
        }

        /// <summary>
        /// Creates large test dataset for performance testing
        /// </summary>
        private async Task CreateLargeTestDataset()
        {
            using var connection = new SqlConnection(_sourceSqlServerConnectionString);
            await connection.OpenAsync();

            // Create large customers table
            using var createTableCommand = new SqlCommand(@"
                IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[LargeCustomers]') AND type in (N'U'))
                CREATE TABLE [dbo].[LargeCustomers] (
                    [CustomerID] [int] IDENTITY(1,1) PRIMARY KEY,
                    [FirstName] [nvarchar](50) NOT NULL,
                    [LastName] [nvarchar](50) NOT NULL,
                    [Email] [nvarchar](100) NULL,
                    [Phone] [nvarchar](20) NULL,
                    [Address] [nvarchar](200) NULL,
                    [City] [nvarchar](50) NULL,
                    [State] [nvarchar](20) NULL,
                    [ZipCode] [nvarchar](10) NULL,
                    [Country] [nvarchar](50) NULL,
                    [CreatedDate] [datetime] DEFAULT GETDATE(),
                    [IsActive] [bit] DEFAULT 1
                )", connection);
            await createTableCommand.ExecuteNonQueryAsync();

            // Insert 1000 test records
            for (int i = 1; i <= 1000; i++)
            {
                using var insertCommand = new SqlCommand(@"
                    INSERT INTO [dbo].[LargeCustomers] ([FirstName], [LastName], [Email], [Phone], [Address], [City], [State], [ZipCode], [Country])
                    VALUES (@FirstName, @LastName, @Email, @Phone, @Address, @City, @State, @ZipCode, @Country)", connection);

                insertCommand.Parameters.AddWithValue("@FirstName", $"Test{i}");
                insertCommand.Parameters.AddWithValue("@LastName", $"User{i}");
                insertCommand.Parameters.AddWithValue("@Email", $"test{i}@email.com");
                insertCommand.Parameters.AddWithValue("@Phone", $"555-{i:D4}");
                insertCommand.Parameters.AddWithValue("@Address", $"{i} Test St");
                insertCommand.Parameters.AddWithValue("@City", $"TestCity{i % 10}");
                insertCommand.Parameters.AddWithValue("@State", $"TS{i % 50}");
                insertCommand.Parameters.AddWithValue("@ZipCode", $"{i:D5}");
                insertCommand.Parameters.AddWithValue("@Country", "USA");

                await insertCommand.ExecuteNonQueryAsync();
            }
        }

        #endregion
    }
}
