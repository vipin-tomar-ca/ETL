using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Configuration;
using ETL.Spark;
using System;
using System.Threading.Tasks;

namespace ETL.Spark.Program
{
    /// <summary>
    /// Main program for running the Spark MultiDB Delta processing application
    /// </summary>
    public class SparkMultiDBDeltaProgram
    {
        public static async Task Main(string[] args)
        {
            try
            {
                // Build configuration
                var configuration = new ConfigurationBuilder()
                    .SetBasePath(System.IO.Directory.GetCurrentDirectory())
                    .AddJsonFile("appsettings.json", optional: false, reloadOnChange: true)
                    .AddJsonFile($"appsettings.{Environment.GetEnvironmentVariable("ASPNETCORE_ENVIRONMENT") ?? "Production"}.json", optional: true)
                    .AddEnvironmentVariables()
                    .AddCommandLine(args)
                    .Build();

                // Build host with dependency injection
                var host = Host.CreateDefaultBuilder(args)
                    .ConfigureServices((context, services) =>
                    {
                        // Add configuration
                        services.AddSingleton<IConfiguration>(configuration);

                        // Add logging
                        services.AddLogging(builder =>
                        {
                            builder.AddConsole();
                            builder.AddDebug();
                            builder.AddConfiguration(configuration.GetSection("Logging"));
                        });

                        // Add Spark processor
                        services.AddTransient<SparkMultiDBDeltaProcessor>();

                        // Add health checks
                        services.AddHealthChecks()
                            .AddSqlServer(configuration.GetConnectionString("MetadataDB"), name: "metadata-db")
                            .AddSqlServer(configuration.GetConnectionString("TargetDB"), name: "target-db");
                    })
                    .Build();

                // Get logger
                var logger = host.Services.GetRequiredService<ILogger<SparkMultiDBDeltaProgram>>();
                logger.LogInformation("Starting Spark MultiDB Delta processing application");

                // Validate configuration
                ValidateConfiguration(configuration, logger);

                // Run health checks
                await RunHealthChecksAsync(host, logger);

                // Get Spark processor and run processing
                using (var scope = host.Services.CreateScope())
                {
                    var sparkProcessor = scope.ServiceProvider.GetRequiredService<SparkMultiDBDeltaProcessor>();
                    
                    logger.LogInformation("Initializing Spark processor");
                    
                    // Process delta data
                    await sparkProcessor.ProcessDeltaDataAsync();
                    
                    logger.LogInformation("Spark MultiDB Delta processing completed successfully");
                }

                // Graceful shutdown
                await host.StopAsync();
                logger.LogInformation("Application shutdown completed");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Fatal error: {ex.Message}");
                Console.WriteLine($"Stack trace: {ex.StackTrace}");
                Environment.Exit(1);
            }
        }

        /// <summary>
        /// Validates application configuration
        /// </summary>
        private static void ValidateConfiguration(IConfiguration configuration, ILogger logger)
        {
            logger.LogInformation("Validating application configuration");

            // Check required connection strings
            var metadataConnectionString = configuration.GetConnectionString("MetadataDB");
            var targetConnectionString = configuration.GetConnectionString("TargetDB");

            if (string.IsNullOrEmpty(metadataConnectionString))
            {
                throw new InvalidOperationException("MetadataDB connection string is required");
            }

            if (string.IsNullOrEmpty(targetConnectionString))
            {
                throw new InvalidOperationException("TargetDB connection string is required");
            }

            // Check Spark configuration
            var sparkMaster = configuration["Spark:Master"];
            var sparkAppName = configuration["Spark:AppName"];

            if (string.IsNullOrEmpty(sparkMaster))
            {
                logger.LogWarning("Spark Master not configured, using local mode");
            }

            if (string.IsNullOrEmpty(sparkAppName))
            {
                logger.LogWarning("Spark AppName not configured, using default");
            }

            // Check processing configuration
            var batchSize = configuration.GetValue<int>("Processing:BatchSize", 10000);
            var timeoutMinutes = configuration.GetValue<int>("Processing:TimeoutMinutes", 30);

            logger.LogInformation($"Configuration validation completed - BatchSize: {batchSize}, TimeoutMinutes: {timeoutMinutes}");
        }

        /// <summary>
        /// Runs health checks for dependencies
        /// </summary>
        private static async Task RunHealthChecksAsync(IHost host, ILogger logger)
        {
            logger.LogInformation("Running health checks");

            try
            {
                var healthCheckService = host.Services.GetRequiredService<Microsoft.Extensions.Diagnostics.HealthChecks.IHealthCheckService>();
                var healthReport = await healthCheckService.CheckHealthAsync();

                if (healthReport.Status == Microsoft.Extensions.Diagnostics.HealthChecks.HealthStatus.Healthy)
                {
                    logger.LogInformation("All health checks passed");
                }
                else
                {
                    logger.LogWarning("Some health checks failed:");
                    foreach (var entry in healthReport.Entries)
                    {
                        if (entry.Value.Status != Microsoft.Extensions.Diagnostics.HealthChecks.HealthStatus.Healthy)
                        {
                            logger.LogWarning($"Health check '{entry.Key}' failed: {entry.Value.Description}");
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                logger.LogError(ex, "Error running health checks");
                throw;
            }
        }
    }

    /// <summary>
    /// Configuration validation helper
    /// </summary>
    public static class ConfigurationValidation
    {
        /// <summary>
        /// Validates SQL Server connection string
        /// </summary>
        public static bool IsValidSqlServerConnectionString(string connectionString)
        {
            if (string.IsNullOrEmpty(connectionString))
                return false;

            try
            {
                using (var connection = new System.Data.SqlClient.SqlConnection(connectionString))
                {
                    connection.Open();
                    return true;
                }
            }
            catch
            {
                return false;
            }
        }

        /// <summary>
        /// Validates Spark configuration
        /// </summary>
        public static bool IsValidSparkConfiguration(string master, string appName)
        {
            if (string.IsNullOrEmpty(master) && string.IsNullOrEmpty(appName))
                return false;

            // Add more validation as needed
            return true;
        }
    }
}
