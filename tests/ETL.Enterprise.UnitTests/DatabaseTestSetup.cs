using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Moq;
using Microsoft.Data.Sqlite;
using System.IO;

namespace ETL.Tests.Unit
{
    /// <summary>
    /// Database test setup and mocking utilities
    /// Provides in-memory database setup, connection mocking, and test data management
    /// </summary>
    public class DatabaseTestSetup : IDisposable
    {
        private readonly string _testDatabasePath;
        private readonly string _connectionString;
        private bool _disposed = false;

        public DatabaseTestSetup()
        {
            _testDatabasePath = Path.Combine(Path.GetTempPath(), $"ETL_Test_{Guid.NewGuid()}.db");
            _connectionString = $"Data Source={_testDatabasePath};";
        }

        /// <summary>
        /// Creates an in-memory SQLite database for testing
        /// </summary>
        public async Task<IDbConnection> CreateTestDatabaseAsync()
        {
            var connection = new SqliteConnection(_connectionString);
            await connection.OpenAsync();
            
            // Create test tables
            await CreateTestTablesAsync(connection);
            
            // Insert test data
            await InsertTestDataAsync(connection);
            
            return connection;
        }

        /// <summary>
        /// Creates mock database connection for unit testing
        /// </summary>
        public Mock<IDbConnection> CreateMockConnection()
        {
            var mockConnection = new Mock<IDbConnection>();
            var mockCommand = new Mock<IDbCommand>();
            var mockDataReader = new Mock<IDataReader>();

            // Setup connection behavior
            mockConnection.Setup(conn => conn.CreateCommand()).Returns(mockCommand.Object);
            mockConnection.Setup(conn => conn.OpenAsync()).Returns(Task.CompletedTask);
            mockConnection.Setup(conn => conn.CloseAsync()).Returns(Task.CompletedTask);
            mockConnection.Setup(conn => conn.State).Returns(ConnectionState.Open);

            // Setup command behavior
            mockCommand.Setup(cmd => cmd.ExecuteReaderAsync()).ReturnsAsync(mockDataReader.Object);
            mockCommand.Setup(cmd => cmd.ExecuteNonQueryAsync()).ReturnsAsync(1);
            mockCommand.Setup(cmd => cmd.ExecuteScalarAsync()).ReturnsAsync(1);
            mockCommand.Setup(cmd => cmd.CommandTimeout).Returns(30);

            return mockConnection;
        }

        /// <summary>
        /// Creates mock database command
        /// </summary>
        public Mock<IDbCommand> CreateMockCommand()
        {
            var mockCommand = new Mock<IDbCommand>();
            var mockParameters = new Mock<IDataParameterCollection>();
            var mockParameter = new Mock<IDbDataParameter>();

            mockCommand.Setup(cmd => cmd.Parameters).Returns(mockParameters.Object);
            mockCommand.Setup(cmd => cmd.CreateParameter()).Returns(mockParameter.Object);
            mockCommand.Setup(cmd => cmd.CommandTimeout).Returns(30);
            mockCommand.Setup(cmd => cmd.CommandText).Returns("SELECT 1");

            return mockCommand;
        }

        /// <summary>
        /// Creates mock data reader with test data
        /// </summary>
        public Mock<IDataReader> CreateMockDataReader<T>(List<T> testData)
        {
            var mockReader = new Mock<IDataReader>();
            var dataTable = ConvertToDataTable(testData);
            var dataReader = dataTable.CreateDataReader();
            var currentRow = -1;

            mockReader.Setup(reader => reader.Read())
                .Returns(() =>
                {
                    currentRow++;
                    return currentRow < testData.Count;
                });

            mockReader.Setup(reader => reader.GetOrdinal(It.IsAny<string>()))
                .Returns<string>(columnName => dataReader.GetOrdinal(columnName));

            mockReader.Setup(reader => reader.GetValue(It.IsAny<int>()))
                .Returns<int>(index => currentRow < testData.Count ? dataReader.GetValue(index) : null);

            mockReader.Setup(reader => reader.IsDBNull(It.IsAny<int>()))
                .Returns<int>(index => currentRow < testData.Count ? dataReader.IsDBNull(index) : true);

            // Type-specific getters
            mockReader.Setup(reader => reader.GetInt32(It.IsAny<int>()))
                .Returns<int>(index => currentRow < testData.Count ? dataReader.GetInt32(index) : 0);

            mockReader.Setup(reader => reader.GetString(It.IsAny<int>()))
                .Returns<int>(index => currentRow < testData.Count ? dataReader.GetString(index) : string.Empty);

            mockReader.Setup(reader => reader.GetDecimal(It.IsAny<int>()))
                .Returns<int>(index => currentRow < testData.Count ? dataReader.GetDecimal(index) : 0m);

            mockReader.Setup(reader => reader.GetDateTime(It.IsAny<int>()))
                .Returns<int>(index => currentRow < testData.Count ? dataReader.GetDateTime(index) : DateTime.MinValue);

            mockReader.Setup(reader => reader.GetBoolean(It.IsAny<int>()))
                .Returns<int>(index => currentRow < testData.Count ? dataReader.GetBoolean(index) : false);

            mockReader.Setup(reader => reader.GetGuid(It.IsAny<int>()))
                .Returns<int>(index => currentRow < testData.Count ? dataReader.GetGuid(index) : Guid.Empty);

            return mockReader;
        }

        /// <summary>
        /// Creates test database with specific schema
        /// </summary>
        public async Task<IDbConnection> CreateTestDatabaseWithSchemaAsync(DatabaseSchema schema)
        {
            var connection = new SqliteConnection(_connectionString);
            await connection.OpenAsync();

            switch (schema)
            {
                case DatabaseSchema.ETLMetadata:
                    await CreateETLMetadataSchemaAsync(connection);
                    break;
                case DatabaseSchema.CustomerOrders:
                    await CreateCustomerOrdersSchemaAsync(connection);
                    break;
                case DatabaseSchema.SalesReporting:
                    await CreateSalesReportingSchemaAsync(connection);
                    break;
                default:
                    await CreateTestTablesAsync(connection);
                    break;
            }

            return connection;
        }

        /// <summary>
        /// Simulates database connection errors
        /// </summary>
        public Mock<IDbConnection> CreateFailingConnection(DatabaseErrorType errorType)
        {
            var mockConnection = new Mock<IDbConnection>();

            switch (errorType)
            {
                case DatabaseErrorType.ConnectionTimeout:
                    mockConnection.Setup(conn => conn.OpenAsync())
                        .ThrowsAsync(new TimeoutException("Connection timeout"));
                    break;
                case DatabaseErrorType.AuthenticationFailed:
                    mockConnection.Setup(conn => conn.OpenAsync())
                        .ThrowsAsync(new SqlException("Login failed for user"));
                    break;
                case DatabaseErrorType.DatabaseNotFound:
                    mockConnection.Setup(conn => conn.OpenAsync())
                        .ThrowsAsync(new SqlException("Cannot open database"));
                    break;
                case DatabaseErrorType.NetworkError:
                    mockConnection.Setup(conn => conn.OpenAsync())
                        .ThrowsAsync(new InvalidOperationException("Network error"));
                    break;
                default:
                    mockConnection.Setup(conn => conn.OpenAsync())
                        .ThrowsAsync(new Exception("Unknown database error"));
                    break;
            }

            return mockConnection;
        }

        /// <summary>
        /// Creates test data for specific scenarios
        /// </summary>
        public async Task InsertTestDataForScenarioAsync(IDbConnection connection, TestDataScenario scenario)
        {
            switch (scenario)
            {
                case TestDataScenario.LargeDataset:
                    await InsertLargeDatasetAsync(connection);
                    break;
                case TestDataScenario.EmptyTables:
                    // Tables are already empty
                    break;
                case TestDataScenario.InvalidData:
                    await InsertInvalidDataAsync(connection);
                    break;
                case TestDataScenario.MixedDataTypes:
                    await InsertMixedDataTypesAsync(connection);
                    break;
                case TestDataScenario.PerformanceTest:
                    await InsertPerformanceTestDataAsync(connection);
                    break;
                default:
                    await InsertTestDataAsync(connection);
                    break;
            }
        }

        #region Private Helper Methods

        private async Task CreateTestTablesAsync(IDbConnection connection)
        {
            var createTablesScript = @"
                CREATE TABLE IF NOT EXISTS Customers (
                    CustomerID INTEGER PRIMARY KEY,
                    CustomerName TEXT NOT NULL,
                    Email TEXT,
                    Phone TEXT,
                    CreatedDate DATETIME DEFAULT CURRENT_TIMESTAMP,
                    ModifiedDate DATETIME,
                    IsActive BOOLEAN DEFAULT 1
                );

                CREATE TABLE IF NOT EXISTS Orders (
                    OrderID INTEGER PRIMARY KEY,
                    CustomerID INTEGER,
                    OrderDate DATETIME,
                    TotalAmount DECIMAL(10,2),
                    Status TEXT,
                    FOREIGN KEY (CustomerID) REFERENCES Customers(CustomerID)
                );

                CREATE TABLE IF NOT EXISTS Products (
                    ProductID INTEGER PRIMARY KEY,
                    ProductName TEXT NOT NULL,
                    Price DECIMAL(10,2),
                    CategoryID INTEGER,
                    Discontinued BOOLEAN DEFAULT 0
                );

                CREATE TABLE IF NOT EXISTS OrderDetails (
                    OrderDetailID INTEGER PRIMARY KEY,
                    OrderID INTEGER,
                    ProductID INTEGER,
                    Quantity INTEGER,
                    UnitPrice DECIMAL(10,2),
                    FOREIGN KEY (OrderID) REFERENCES Orders(OrderID),
                    FOREIGN KEY (ProductID) REFERENCES Products(ProductID)
                );
            ";

            using var command = connection.CreateCommand();
            command.CommandText = createTablesScript;
            await command.ExecuteNonQueryAsync();
        }

        private async Task CreateETLMetadataSchemaAsync(IDbConnection connection)
        {
            var createSchemaScript = @"
                CREATE TABLE IF NOT EXISTS Connections (
                    ConnectionID INTEGER PRIMARY KEY,
                    DatabaseName TEXT NOT NULL,
                    ConnectionString TEXT NOT NULL,
                    IsCDCEnabled BOOLEAN DEFAULT 0,
                    LastProcessedLSN BLOB,
                    LastRunTimestamp DATETIME,
                    DeltaCondition TEXT,
                    TableName TEXT NOT NULL,
                    SchemaName TEXT DEFAULT 'dbo',
                    StagingTableName TEXT,
                    ProcessingEngine TEXT DEFAULT 'CSharp',
                    IsActive BOOLEAN DEFAULT 1,
                    CreatedDate DATETIME DEFAULT CURRENT_TIMESTAMP,
                    LastUpdated DATETIME DEFAULT CURRENT_TIMESTAMP
                );

                CREATE TABLE IF NOT EXISTS Logs (
                    LogID INTEGER PRIMARY KEY,
                    BatchID TEXT NOT NULL,
                    DatabaseName TEXT,
                    TableName TEXT,
                    LogLevel TEXT NOT NULL,
                    LogMessage TEXT NOT NULL,
                    ErrorMessage TEXT,
                    ErrorCode INTEGER,
                    ErrorSource TEXT,
                    RecordsProcessed INTEGER,
                    RecordsFailed INTEGER,
                    ProcessingTimeSeconds INTEGER,
                    CreatedDate DATETIME DEFAULT CURRENT_TIMESTAMP,
                    PackageName TEXT,
                    TaskName TEXT,
                    ExecutionID INTEGER
                );

                CREATE TABLE IF NOT EXISTS ProcessingHistory (
                    HistoryID INTEGER PRIMARY KEY,
                    BatchID TEXT NOT NULL,
                    ConnectionID INTEGER,
                    DatabaseName TEXT NOT NULL,
                    TableName TEXT NOT NULL,
                    ProcessingEngine TEXT NOT NULL,
                    StartTime DATETIME NOT NULL,
                    EndTime DATETIME,
                    RecordsProcessed INTEGER DEFAULT 0,
                    RecordsFailed INTEGER DEFAULT 0,
                    LastProcessedLSN BLOB,
                    LastRunTimestamp DATETIME,
                    Status TEXT DEFAULT 'RUNNING',
                    ErrorMessage TEXT,
                    RetryCount INTEGER DEFAULT 0,
                    FilePath TEXT,
                    FileSizeMB DECIMAL(10,2),
                    CompressionRatio DECIMAL(5,2),
                    CreatedDate DATETIME DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (ConnectionID) REFERENCES Connections(ConnectionID)
                );
            ";

            using var command = connection.CreateCommand();
            command.CommandText = createSchemaScript;
            await command.ExecuteNonQueryAsync();
        }

        private async Task CreateCustomerOrdersSchemaAsync(IDbConnection connection)
        {
            await CreateTestTablesAsync(connection);
        }

        private async Task CreateSalesReportingSchemaAsync(IDbConnection connection)
        {
            var createSchemaScript = @"
                CREATE TABLE IF NOT EXISTS Sales (
                    SaleID INTEGER PRIMARY KEY,
                    CustomerID INTEGER,
                    ProductID INTEGER,
                    SaleDate DATETIME,
                    Quantity INTEGER,
                    UnitPrice DECIMAL(10,2),
                    TotalAmount DECIMAL(10,2),
                    Region TEXT,
                    SalesRepID INTEGER
                );

                CREATE TABLE IF NOT EXISTS SalesReps (
                    SalesRepID INTEGER PRIMARY KEY,
                    FirstName TEXT,
                    LastName TEXT,
                    Email TEXT,
                    Region TEXT,
                    HireDate DATETIME
                );

                CREATE TABLE IF NOT EXISTS Regions (
                    RegionID INTEGER PRIMARY KEY,
                    RegionName TEXT,
                    ManagerID INTEGER,
                    Budget DECIMAL(15,2)
                );
            ";

            using var command = connection.CreateCommand();
            command.CommandText = createSchemaScript;
            await command.ExecuteNonQueryAsync();
        }

        private async Task InsertTestDataAsync(IDbConnection connection)
        {
            var insertDataScript = @"
                INSERT INTO Customers (CustomerID, CustomerName, Email, Phone, CreatedDate, IsActive) VALUES
                (1, 'John Doe', 'john@example.com', '555-1234', '2024-01-01', 1),
                (2, 'Jane Smith', 'jane@example.com', '555-5678', '2024-01-02', 1),
                (3, 'Bob Johnson', 'bob@example.com', '555-9012', '2024-01-03', 0);

                INSERT INTO Products (ProductID, ProductName, Price, CategoryID, Discontinued) VALUES
                (1, 'Laptop', 999.99, 1, 0),
                (2, 'Mouse', 29.99, 1, 0),
                (3, 'Keyboard', 79.99, 1, 0),
                (4, 'Monitor', 299.99, 1, 0);

                INSERT INTO Orders (OrderID, CustomerID, OrderDate, TotalAmount, Status) VALUES
                (1, 1, '2024-06-01', 999.99, 'Completed'),
                (2, 1, '2024-06-02', 29.99, 'Completed'),
                (3, 2, '2024-06-03', 379.98, 'Pending');

                INSERT INTO OrderDetails (OrderDetailID, OrderID, ProductID, Quantity, UnitPrice) VALUES
                (1, 1, 1, 1, 999.99),
                (2, 2, 2, 1, 29.99),
                (3, 3, 3, 1, 79.99),
                (4, 3, 4, 1, 299.99);
            ";

            using var command = connection.CreateCommand();
            command.CommandText = insertDataScript;
            await command.ExecuteNonQueryAsync();
        }

        private async Task InsertLargeDatasetAsync(IDbConnection connection)
        {
            // Insert 10,000 customers
            for (int i = 1; i <= 10000; i++)
            {
                using var command = connection.CreateCommand();
                command.CommandText = @"
                    INSERT INTO Customers (CustomerID, CustomerName, Email, Phone, CreatedDate, IsActive) 
                    VALUES (@id, @name, @email, @phone, @date, 1)";
                
                command.Parameters.Add(new SqliteParameter("@id", i));
                command.Parameters.Add(new SqliteParameter("@name", $"Customer {i}"));
                command.Parameters.Add(new SqliteParameter("@email", $"customer{i}@example.com"));
                command.Parameters.Add(new SqliteParameter("@phone", $"555-{i:D4}"));
                command.Parameters.Add(new SqliteParameter("@date", DateTime.Now.AddDays(-i)));
                
                await command.ExecuteNonQueryAsync();
            }
        }

        private async Task InsertInvalidDataAsync(IDbConnection connection)
        {
            // This would insert data that violates constraints
            // For testing error handling scenarios
        }

        private async Task InsertMixedDataTypesAsync(IDbConnection connection)
        {
            var insertScript = @"
                INSERT INTO Customers (CustomerID, CustomerName, Email, Phone, CreatedDate, ModifiedDate, IsActive) VALUES
                (100, 'Test Customer', 'test@example.com', '555-0000', '2024-01-01', NULL, 1),
                (101, 'Another Customer', 'another@example.com', '555-0001', '2024-01-02', '2024-01-03', 0);
            ";

            using var command = connection.CreateCommand();
            command.CommandText = insertScript;
            await command.ExecuteNonQueryAsync();
        }

        private async Task InsertPerformanceTestDataAsync(IDbConnection connection)
        {
            // Insert data specifically designed for performance testing
            await InsertLargeDatasetAsync(connection);
        }

        private DataTable ConvertToDataTable<T>(List<T> data)
        {
            var dataTable = new DataTable();
            
            if (data.Count > 0)
            {
                var properties = typeof(T).GetProperties();
                foreach (var property in properties)
                {
                    dataTable.Columns.Add(property.Name, property.PropertyType);
                }

                foreach (var item in data)
                {
                    var row = dataTable.NewRow();
                    foreach (var property in properties)
                    {
                        row[property.Name] = property.GetValue(item) ?? DBNull.Value;
                    }
                    dataTable.Rows.Add(row);
                }
            }

            return dataTable;
        }

        #endregion

        public void Dispose()
        {
            if (!_disposed)
            {
                if (File.Exists(_testDatabasePath))
                {
                    File.Delete(_testDatabasePath);
                }
                _disposed = true;
            }
        }
    }

    #region Enums

    public enum DatabaseSchema
    {
        Default,
        ETLMetadata,
        CustomerOrders,
        SalesReporting
    }

    public enum DatabaseErrorType
    {
        ConnectionTimeout,
        AuthenticationFailed,
        DatabaseNotFound,
        NetworkError,
        Unknown
    }

    public enum TestDataScenario
    {
        Default,
        LargeDataset,
        EmptyTables,
        InvalidData,
        MixedDataTypes,
        PerformanceTest
    }

    #endregion
}
