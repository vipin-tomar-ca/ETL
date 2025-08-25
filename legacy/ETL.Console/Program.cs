using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using ETL.Core;
using System.Linq;

namespace ETL.Console
{
    /// <summary>
    /// Console application demonstrating the ETL DataTransformer usage
    /// </summary>
    class Program
    {
        static async Task Main(string[] args)
        {
            System.Console.WriteLine("=== ETL Scalable - Data Transformation Demo ===");
            System.Console.WriteLine();

            // Create logger
            var loggerFactory = LoggerFactory.Create(builder => 
                builder.AddConsole()
                       .SetMinimumLevel(LogLevel.Information));

            var logger = loggerFactory.CreateLogger<DataTransformer>();

            try
            {
                // Demo 1: Basic Usage
                await RunBasicDemo(logger);

                System.Console.WriteLine();
                System.Console.WriteLine("Press any key to continue to Advanced Demo...");
                System.Console.ReadKey();

                // Demo 2: Advanced Usage
                await RunAdvancedDemo(logger);

                System.Console.WriteLine();
                System.Console.WriteLine("Press any key to continue to Performance Demo...");
                System.Console.ReadKey();

                // Demo 3: Performance Demo
                await RunPerformanceDemo(logger);

                System.Console.WriteLine();
                System.Console.WriteLine("All demos completed successfully!");
            }
            catch (Exception ex)
            {
                System.Console.WriteLine($"Error running demos: {ex.Message}");
                logger.LogError(ex, "Demo execution failed");
            }

            System.Console.WriteLine();
            System.Console.WriteLine("Press any key to exit...");
            System.Console.ReadKey();
        }

        /// <summary>
        /// Demonstrates basic usage of the DataTransformer
        /// </summary>
        static async Task RunBasicDemo(ILogger<DataTransformer> logger)
        {
            System.Console.WriteLine("=== Basic Usage Demo ===");

            // Configure source databases (using sample data)
            var sourceDatabases = new List<SourceDatabaseConfig>
            {
                new SourceDatabaseConfig
                {
                    DatabaseName = "SampleDB1",
                    ConnectionString = "Server=localhost;Database=SampleDB1;Trusted_Connection=true;",
                    SourceTable = "Customers"
                },
                new SourceDatabaseConfig
                {
                    DatabaseName = "SampleDB2",
                    ConnectionString = "Server=localhost;Database=SampleDB2;Trusted_Connection=true;",
                    SourceTable = "Orders"
                }
            };

            // Basic transformation rules
            var rules = new TransformationRules
            {
                CleanStrings = true,
                StringCleaningOptions = new StringCleaningOptions
                {
                    TrimWhitespace = true,
                    RemoveExtraSpaces = true,
                    NormalizeCase = true
                }
            };

            // Initialize transformer
            using var transformer = new DataTransformer(
                connectionString: "Server=localhost;Database=ETL_Staging;Trusted_Connection=true;",
                logger: logger,
                batchSize: 1000, // Small batch size for demo
                maxDegreeOfParallelism: 2
            );

            System.Console.WriteLine("Starting basic transformation...");
            var startTime = DateTime.UtcNow;

            // Execute transformation
            var result = await transformer.TransformDataAsync(sourceDatabases, rules);

            var endTime = DateTime.UtcNow;
            var duration = endTime - startTime;

            // Display results
            System.Console.WriteLine($"Basic Demo Results:");
            System.Console.WriteLine($"  Duration: {duration}");
            System.Console.WriteLine($"  Source Databases: {result.SourceDatabases}");
            System.Console.WriteLine($"  Processed Records: {result.ProcessedRecords:N0}");
            System.Console.WriteLine($"  Transformed Records: {result.TransformedRecords:N0}");
            System.Console.WriteLine($"  Success: {result.Success}");
            System.Console.WriteLine($"  Error Count: {result.Errors.Count}");

            if (result.Errors.Any())
            {
                System.Console.WriteLine("  Errors:");
                foreach (var error in result.Errors.Take(3))
                {
                    System.Console.WriteLine($"    - {error.Severity}: {error.Message}");
                }
            }
        }

        /// <summary>
        /// Demonstrates advanced usage with custom transformations
        /// </summary>
        static async Task RunAdvancedDemo(ILogger<DataTransformer> logger)
        {
            System.Console.WriteLine("=== Advanced Usage Demo ===");

            var sourceDatabases = new List<SourceDatabaseConfig>
            {
                new SourceDatabaseConfig
                {
                    DatabaseName = "CustomerDB",
                    ConnectionString = "Server=localhost;Database=CustomerDB;Trusted_Connection=true;",
                    SourceTable = "Customers"
                }
            };

            // Advanced transformation rules with custom logic
            var rules = new TransformationRules
            {
                CleanStrings = true,
                StringCleaningOptions = new StringCleaningOptions
                {
                    TrimWhitespace = true,
                    RemoveSpecialCharacters = true,
                    NormalizeCase = true,
                    RemoveExtraSpaces = true,
                    MaxLength = 255
                },
                CustomTransformations = new List<Func<TransformedRecord, CancellationToken, Task>>
                {
                    // Data validation
                    async (record, token) =>
                    {
                        if (record.Data.ContainsKey("Age"))
                        {
                            var ageValue = record.Data["Age"];
                            if (ageValue != DBNull.Value)
                            {
                                var age = Convert.ToInt32(ageValue);
                                if (age < 0 || age > 150)
                                {
                                    record.Data["Age"] = DBNull.Value;
                                    record.Data["AgeValidationError"] = "Invalid age value";
                                }
                            }
                        }
                    },

                    // Data enrichment
                    async (record, token) =>
                    {
                        if (record.Data.ContainsKey("Country"))
                        {
                            var country = record.Data["Country"]?.ToString();
                            if (!string.IsNullOrEmpty(country))
                            {
                                record.Data["Region"] = GetRegionFromCountry(country);
                                record.Data["Continent"] = GetContinentFromCountry(country);
                            }
                        }
                    },

                    // Business logic
                    async (record, token) =>
                    {
                        if (record.Data.ContainsKey("Income"))
                        {
                            var incomeValue = record.Data["Income"];
                            if (incomeValue != DBNull.Value)
                            {
                                var income = Convert.ToDecimal(incomeValue);
                                record.Data["IncomeCategory"] = income switch
                                {
                                    < 30000 => "Low",
                                    < 75000 => "Medium",
                                    < 150000 => "High",
                                    _ => "Premium"
                                };
                            }
                        }
                    }
                }
            };

            using var transformer = new DataTransformer(
                connectionString: "Server=localhost;Database=ETL_Staging;Trusted_Connection=true;",
                logger: logger,
                batchSize: 5000,
                maxDegreeOfParallelism: 4
            );

            System.Console.WriteLine("Starting advanced transformation...");
            var startTime = DateTime.UtcNow;

            var result = await transformer.TransformDataAsync(sourceDatabases, rules);

            var endTime = DateTime.UtcNow;
            var duration = endTime - startTime;

            System.Console.WriteLine($"Advanced Demo Results:");
            System.Console.WriteLine($"  Duration: {duration}");
            System.Console.WriteLine($"  Processed Records: {result.ProcessedRecords:N0}");
            System.Console.WriteLine($"  Transformed Records: {result.TransformedRecords:N0}");
            System.Console.WriteLine($"  Success Rate: {(double)result.TransformedRecords / result.ProcessedRecords:P2}");
            System.Console.WriteLine($"  Success: {result.Success}");
        }

        /// <summary>
        /// Demonstrates high-performance processing
        /// </summary>
        static async Task RunPerformanceDemo(ILogger<DataTransformer> logger)
        {
            System.Console.WriteLine("=== Performance Demo ===");

            // Simulate multiple source databases
            var sourceDatabases = new List<SourceDatabaseConfig>();
            
            for (int i = 1; i <= 5; i++)
            {
                sourceDatabases.Add(new SourceDatabaseConfig
                {
                    DatabaseName = $"PerformanceDB{i}",
                    ConnectionString = $"Server=localhost;Database=PerformanceDB{i};Trusted_Connection=true;",
                    SourceTable = $"Table{i}"
                });
            }

            // Optimized transformation rules for performance
            var rules = new TransformationRules
            {
                CleanStrings = true,
                StringCleaningOptions = new StringCleaningOptions
                {
                    TrimWhitespace = true,
                    RemoveExtraSpaces = true,
                    MaxLength = 100 // Shorter max length for performance
                },
                CustomTransformations = new List<Func<TransformedRecord, CancellationToken, Task>>
                {
                    // Minimal transformation for performance
                    async (record, token) =>
                    {
                        if (record.Data.ContainsKey("Status"))
                        {
                            var status = record.Data["Status"]?.ToString();
                            if (string.IsNullOrEmpty(status))
                            {
                                record.Data["Status"] = "Unknown";
                            }
                        }
                    }
                }
            };

            // High-performance configuration
            using var transformer = new DataTransformer(
                connectionString: "Server=localhost;Database=ETL_Staging;Trusted_Connection=true;",
                logger: logger,
                batchSize: 50000,          // Larger batches
                maxDegreeOfParallelism: Environment.ProcessorCount // Use all cores
            );

            System.Console.WriteLine("Starting performance transformation...");
            System.Console.WriteLine($"  Batch Size: 50,000");
            System.Console.WriteLine($"  Parallelism: {Environment.ProcessorCount}");
            System.Console.WriteLine($"  Source Databases: {sourceDatabases.Count}");

            var startTime = DateTime.UtcNow;

            var result = await transformer.TransformDataAsync(sourceDatabases, rules);

            var endTime = DateTime.UtcNow;
            var duration = endTime - startTime;

            // Performance metrics
            var recordsPerSecond = result.ProcessedRecords / duration.TotalSeconds;
            var throughput = result.ProcessedRecords / duration.TotalMinutes;

            System.Console.WriteLine($"Performance Demo Results:");
            System.Console.WriteLine($"  Duration: {duration}");
            System.Console.WriteLine($"  Processed Records: {result.ProcessedRecords:N0}");
            System.Console.WriteLine($"  Transformed Records: {result.TransformedRecords:N0}");
            System.Console.WriteLine($"  Records per Second: {recordsPerSecond:N0}");
            System.Console.WriteLine($"  Throughput: {throughput:N0} records/minute");
            System.Console.WriteLine($"  Success Rate: {(double)result.TransformedRecords / result.ProcessedRecords:P2}");
        }

        // Helper methods
        private static string GetRegionFromCountry(string country)
        {
            return country?.ToLowerInvariant() switch
            {
                "usa" or "canada" => "North America",
                "uk" or "france" or "germany" => "Europe",
                "japan" or "china" or "india" => "Asia",
                "australia" or "new zealand" => "Oceania",
                "brazil" or "argentina" => "South America",
                "south africa" or "egypt" => "Africa",
                _ => "Unknown"
            };
        }

        private static string GetContinentFromCountry(string country)
        {
            return country?.ToLowerInvariant() switch
            {
                "usa" or "canada" => "North America",
                "uk" or "france" or "germany" or "italy" or "spain" => "Europe",
                "japan" or "china" or "india" or "korea" => "Asia",
                "australia" or "new zealand" => "Oceania",
                "brazil" or "argentina" or "chile" => "South America",
                "south africa" or "egypt" or "nigeria" => "Africa",
                _ => "Unknown"
            };
        }
    }
}
