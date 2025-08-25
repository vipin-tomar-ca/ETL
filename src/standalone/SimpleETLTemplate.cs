using System;
using System.Collections.Generic;
using System.Data;
using Microsoft.Data.SqlClient;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;

namespace ETL.Scalable
{
    /// <summary>
    /// Simplified ETL Template for cross-platform compatibility
    /// This template provides a basic structure for ETL operations
    /// that can be extended with actual Spark or other processing engines
    /// </summary>
    public class SimpleETLTemplate
    {
        private readonly ILogger<SimpleETLTemplate> _logger;
        private readonly IConfiguration _configuration;
        private readonly string _sourceConnectionString;
        private readonly string _targetConnectionString;

        public SimpleETLTemplate(IConfiguration configuration, ILogger<SimpleETLTemplate> logger)
        {
            _configuration = configuration;
            _logger = logger;
            _sourceConnectionString = _configuration["ConnectionStrings:SourceDB"];
            _targetConnectionString = _configuration["ConnectionStrings:TargetDB"];
        }

        /// <summary>
        /// Main ETL process method
        /// </summary>
        public async Task ExecuteETLAsync()
        {
            try
            {
                _logger.LogInformation("Starting ETL process");

                // Step 1: Extract data from source
                var sourceData = await ExtractDataAsync();

                // Step 2: Transform data
                var transformedData = TransformData(sourceData);

                // Step 3: Load data to target
                await LoadDataAsync(transformedData);

                _logger.LogInformation("ETL process completed successfully");
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error during ETL process");
                throw;
            }
        }

        /// <summary>
        /// Extract data from source database
        /// </summary>
        private async Task<DataTable> ExtractDataAsync()
        {
            _logger.LogInformation("Extracting data from source database");

            var dataTable = new DataTable();

            using (var connection = new SqlConnection(_sourceConnectionString))
            {
                await connection.OpenAsync();
                using (var command = new SqlCommand("SELECT * FROM SourceTable", connection))
                using (var reader = await command.ExecuteReaderAsync())
                {
                    dataTable.Load(reader);
                }
            }

            _logger.LogInformation($"Extracted {dataTable.Rows.Count} rows from source");
            return dataTable;
        }

        /// <summary>
        /// Transform the extracted data
        /// </summary>
        private DataTable TransformData(DataTable sourceData)
        {
            _logger.LogInformation("Transforming data");

            var transformedData = sourceData.Clone();

            foreach (DataRow sourceRow in sourceData.Rows)
            {
                var newRow = transformedData.NewRow();
                
                // Apply transformations here
                foreach (DataColumn column in sourceData.Columns)
                {
                    var value = sourceRow[column];
                    
                    // Example transformation: convert string columns to uppercase
                    if (column.DataType == typeof(string) && value != DBNull.Value)
                    {
                        newRow[column.ColumnName] = value.ToString().ToUpper();
                    }
                    else
                    {
                        newRow[column.ColumnName] = value;
                    }
                }
                
                transformedData.Rows.Add(newRow);
            }

            _logger.LogInformation($"Transformed {transformedData.Rows.Count} rows");
            return transformedData;
        }

        /// <summary>
        /// Load transformed data to target database
        /// </summary>
        private async Task LoadDataAsync(DataTable data)
        {
            _logger.LogInformation("Loading data to target database");

            using (var connection = new SqlConnection(_targetConnectionString))
            {
                await connection.OpenAsync();
                
                // Clear existing data
                using (var command = new SqlCommand("DELETE FROM TargetTable", connection))
                {
                    await command.ExecuteNonQueryAsync();
                }

                // Bulk insert new data
                using (var bulkCopy = new SqlBulkCopy(connection))
                {
                    bulkCopy.DestinationTableName = "TargetTable";
                    
                    foreach (DataColumn column in data.Columns)
                    {
                        bulkCopy.ColumnMappings.Add(column.ColumnName, column.ColumnName);
                    }
                    
                    await bulkCopy.WriteToServerAsync(data);
                }
            }

            _logger.LogInformation($"Loaded {data.Rows.Count} rows to target database");
        }
    }

    /// <summary>
    /// Program entry point for the simplified ETL template
    /// </summary>
    public class Program
    {
        public static async Task Main(string[] args)
        {
            var configuration = new ConfigurationBuilder()
                .SetBasePath(Directory.GetCurrentDirectory())
                .AddJsonFile("appsettings.json", optional: false, reloadOnChange: true)
                .AddJsonFile($"appsettings.{Environment.GetEnvironmentVariable("ASPNETCORE_ENVIRONMENT") ?? "Production"}.json", optional: true)
                .AddEnvironmentVariables()
                .Build();

            var loggerFactory = LoggerFactory.Create(builder =>
            {
                builder.AddConsole();
                builder.AddConfiguration(configuration.GetSection("Logging"));
            });

            var logger = loggerFactory.CreateLogger<SimpleETLTemplate>();

            try
            {
                var etlTemplate = new SimpleETLTemplate(configuration, logger);
                await etlTemplate.ExecuteETLAsync();
                
                logger.LogInformation("ETL process completed successfully");
            }
            catch (Exception ex)
            {
                logger.LogError(ex, "ETL process failed");
                Environment.Exit(1);
            }
        }
    }
}
