using System;
using ETL.Scalable;

namespace ETL.Scalable.Program
{
    /// <summary>
    /// Main program entry point for the Multi-Database Transform application
    /// 
    /// This program demonstrates how to use the MultiDBTransform class to perform
    /// distributed multi-database queries using .NET Spark.
    /// </summary>
    class Program
    {
        static void Main(string[] args)
        {
            try
            {
                Console.WriteLine("=== Multi-Database Transform Application ===");
                Console.WriteLine("Starting distributed ETL process using .NET Spark");
                Console.WriteLine();

                // Configuration - in production, these would come from configuration files
                var metadataConnectionString = GetMetadataConnectionString();
                
                // Initialize the MultiDBTransform application
                using (var multiDbTransform = new MultiDBTransform(metadataConnectionString))
                {
                    Console.WriteLine("MultiDBTransform initialized successfully");
                    Console.WriteLine("Executing distributed multi-database ETL process...");
                    Console.WriteLine();

                    // Execute the main ETL process
                    multiDbTransform.Execute();

                    Console.WriteLine();
                    Console.WriteLine("=== ETL Process Completed Successfully ===");
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Fatal error in main program: {ex.Message}");
                Console.WriteLine($"Stack trace: {ex.StackTrace}");
                Environment.Exit(1);
            }
        }

        /// <summary>
        /// Gets the metadata database connection string
        /// In production, this would be loaded from configuration files or environment variables
        /// </summary>
        /// <returns>Connection string to metadata database</returns>
        private static string GetMetadataConnectionString()
        {
            // Example connection string - replace with your actual metadata database
            return "Server=localhost;Database=ETLMetadata;Trusted_Connection=true;";
            
            // Alternative: Load from environment variable
            // return Environment.GetEnvironmentVariable("METADATA_CONNECTION_STRING");
            
            // Alternative: Load from configuration file
            // return ConfigurationManager.ConnectionStrings["MetadataDatabase"].ConnectionString;
        }
    }
}
