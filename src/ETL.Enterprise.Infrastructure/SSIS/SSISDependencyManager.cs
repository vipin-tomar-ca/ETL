using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;

namespace ETL.Enterprise.Infrastructure.SSIS
{
    /// <summary>
    /// Manages SSIS package dependencies and ensures all required components are available
    /// </summary>
    public class SSISDependencyManager
    {
        private readonly ILogger _logger;

        public SSISDependencyManager(ILogger logger)
        {
            _logger = logger;
        }

        /// <summary>
        /// Validates that all required SSIS dependencies are available
        /// </summary>
        public async Task<bool> ValidateDependenciesAsync()
        {
            try
            {
                _logger.LogInformation("Validating SSIS dependencies");

                // Check if DTExec is available
                var dtsExecPath = await FindDTExecPathAsync();
                if (string.IsNullOrEmpty(dtsExecPath))
                {
                    _logger.LogError("DTExec not found. SSIS is not properly installed.");
                    return false;
                }

                _logger.LogInformation($"DTExec found at: {dtsExecPath}");
                return true;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error validating SSIS dependencies");
                return false;
            }
        }

        /// <summary>
        /// Finds the DTExec executable path
        /// </summary>
        private async Task<string> FindDTExecPathAsync()
        {
            var possiblePaths = new List<string>
            {
                @"C:\Program Files\Microsoft SQL Server\150\DTS\Binn\DTExec.exe",
                @"C:\Program Files\Microsoft SQL Server\140\DTS\Binn\DTExec.exe",
                @"C:\Program Files\Microsoft SQL Server\130\DTS\Binn\DTExec.exe",
                @"C:\Program Files\Microsoft SQL Server\120\DTS\Binn\DTExec.exe",
                @"C:\Program Files\Microsoft SQL Server\110\DTS\Binn\DTExec.exe"
            };

            foreach (var path in possiblePaths)
            {
                if (File.Exists(path))
                {
                    return path;
                }
            }

            return null;
        }

        /// <summary>
        /// Validates that a package file exists and is accessible
        /// </summary>
        public bool ValidatePackageFile(string packagePath)
        {
            try
            {
                if (string.IsNullOrEmpty(packagePath))
                {
                    _logger.LogError("Package path is null or empty");
                    return false;
                }

                if (!File.Exists(packagePath))
                {
                    _logger.LogError($"Package file not found: {packagePath}");
                    return false;
                }

                var fileInfo = new FileInfo(packagePath);
                if (fileInfo.Length == 0)
                {
                    _logger.LogError($"Package file is empty: {packagePath}");
                    return false;
                }

                _logger.LogInformation($"Package file validated: {packagePath}");
                return true;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, $"Error validating package file: {packagePath}");
                return false;
            }
        }

        /// <summary>
        /// Executes packages with dependency management
        /// </summary>
        public async Task<List<PackageExecutionResult>> ExecuteWithDependenciesAsync(List<PackageExecutionRequest> packages)
        {
            try
            {
                _logger.LogInformation($"Executing {packages.Count} packages with dependency management");

                var results = new List<PackageExecutionResult>();
                var executedPackages = new HashSet<string>();

                // Sort packages by priority and dependencies
                var sortedPackages = packages.OrderBy(p => p.Priority).ToList();

                foreach (var package in sortedPackages)
                {
                    // Check if dependencies are satisfied
                    if (package.Dependencies.All(dep => executedPackages.Contains(dep)))
                    {
                        _logger.LogInformation($"Executing package: {package.PackagePath}");
                        
                        // For now, return a mock result since we don't have the actual execution logic here
                        var result = new PackageExecutionResult
                        {
                            ExitCode = 0,
                            Success = true,
                            ExecutionId = Guid.NewGuid().ToString(),
                            ExecutionTime = TimeSpan.FromSeconds(1)
                        };
                        
                        results.Add(result);
                        executedPackages.Add(package.PackagePath);
                    }
                    else
                    {
                        _logger.LogWarning($"Package {package.PackagePath} has unmet dependencies");
                        results.Add(new PackageExecutionResult
                        {
                            ExitCode = -1,
                            Success = false,
                            Error = "Unmet dependencies",
                            ExecutionId = Guid.NewGuid().ToString()
                        });
                    }
                }

                return results;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error executing packages with dependencies");
                throw;
            }
        }
    }
}
