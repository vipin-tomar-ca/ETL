using Microsoft.VisualStudio.TestTools.UnitTesting;
using Microsoft.Extensions.Logging;
using ETL.Enterprise.Infrastructure.SSIS;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using System.IO;
using System.Diagnostics;

namespace ETL.Tests.Integration
{
    /// <summary>
    /// Load and stress tests for SSIS package execution and orchestration
    /// </summary>
    [TestClass]
    public class SSISLoadTests
    {
        private static ILogger<SSISPackageOrchestrator> _logger;
        private static string _testPackagePath;
        private static string _testConnectionString;

        [ClassInitialize]
        public static void ClassInitialize(TestContext context)
        {
            // Initialize test environment
            _testPackagePath = Path.Combine(AppDomain.CurrentDomain.BaseDirectory, "TestPackages");
            _testConnectionString = "Server=localhost;Database=ETL_Test;Integrated Security=true;";
            
            // Create test package directory if it doesn't exist
            if (!Directory.Exists(_testPackagePath))
            {
                Directory.CreateDirectory(_testPackagePath);
            }

            // Initialize logger
            var loggerFactory = LoggerFactory.Create(builder => { });
            _logger = loggerFactory.CreateLogger<SSISPackageOrchestrator>();
        }

        #region Load Tests

        /// <summary>
        /// Tests SSIS package execution under normal load
        /// </summary>
        [TestMethod]
        public async Task Test_LoadTest_NormalLoad()
        {
            // Arrange
            var config = new Dictionary<string, string>
            {
                ["SSIS:DTExecPath"] = @"C:\Program Files\Microsoft SQL Server\150\DTS\Binn\DTExec.exe",
                ["ConnectionStrings:MonitoringDB"] = _testConnectionString
            };
            
            var orchestrator = new SSISPackageOrchestrator(_logger, new TestConfiguration(config));
            var packagePath = Path.Combine(_testPackagePath, "LoadTestPackage.dtsx");
            var parameters = new Dictionary<string, string>
            {
                ["LoadLevel"] = "normal",
                ["RecordCount"] = "10000"
            };

            // Act
            var stopwatch = Stopwatch.StartNew();
            var result = await orchestrator.ExecutePackageAsync(packagePath, parameters);
            stopwatch.Stop();

            // Assert
            Assert.IsNotNull(result, "Package execution result should not be null");
            Assert.IsTrue(result.Success, "Package execution should succeed under normal load");
            Assert.IsTrue(stopwatch.Elapsed.TotalSeconds < 30, "Normal load execution should complete within 30 seconds");
        }

        /// <summary>
        /// Tests SSIS package execution under high load
        /// </summary>
        [TestMethod]
        public async Task Test_LoadTest_HighLoad()
        {
            // Arrange
            var config = new Dictionary<string, string>
            {
                ["SSIS:DTExecPath"] = @"C:\Program Files\Microsoft SQL Server\150\DTS\Binn\DTExec.exe",
                ["ConnectionStrings:MonitoringDB"] = _testConnectionString
            };
            
            var orchestrator = new SSISPackageOrchestrator(_logger, new TestConfiguration(config));
            var packagePath = Path.Combine(_testPackagePath, "HighLoadPackage.dtsx");
            var parameters = new Dictionary<string, string>
            {
                ["LoadLevel"] = "high",
                ["RecordCount"] = "100000"
            };

            // Act
            var stopwatch = Stopwatch.StartNew();
            var result = await orchestrator.ExecutePackageAsync(packagePath, parameters);
            stopwatch.Stop();

            // Assert
            Assert.IsNotNull(result, "Package execution result should not be null");
            Assert.IsTrue(result.Success, "Package execution should succeed under high load");
            Assert.IsTrue(stopwatch.Elapsed.TotalSeconds < 60, "High load execution should complete within 60 seconds");
        }

        /// <summary>
        /// Tests SSIS package execution under extreme load
        /// </summary>
        [TestMethod]
        public async Task Test_LoadTest_ExtremeLoad()
        {
            // Arrange
            var config = new Dictionary<string, string>
            {
                ["SSIS:DTExecPath"] = @"C:\Program Files\Microsoft SQL Server\150\DTS\Binn\DTExec.exe",
                ["ConnectionStrings:MonitoringDB"] = _testConnectionString
            };
            
            var orchestrator = new SSISPackageOrchestrator(_logger, new TestConfiguration(config));
            var packagePath = Path.Combine(_testPackagePath, "ExtremeLoadPackage.dtsx");
            var parameters = new Dictionary<string, string>
            {
                ["LoadLevel"] = "extreme",
                ["RecordCount"] = "1000000"
            };

            // Act
            var stopwatch = Stopwatch.StartNew();
            var result = await orchestrator.ExecutePackageAsync(packagePath, parameters);
            stopwatch.Stop();

            // Assert
            Assert.IsNotNull(result, "Package execution result should not be null");
            Assert.IsTrue(result.Success, "Package execution should succeed under extreme load");
            Assert.IsTrue(stopwatch.Elapsed.TotalSeconds < 120, "Extreme load execution should complete within 120 seconds");
        }

        #endregion

        #region Stress Tests

        /// <summary>
        /// Tests SSIS package execution with memory stress
        /// </summary>
        [TestMethod]
        public async Task Test_StressTest_MemoryStress()
        {
            // Arrange
            var config = new Dictionary<string, string>
            {
                ["SSIS:DTExecPath"] = @"C:\Program Files\Microsoft SQL Server\150\DTS\Binn\DTExec.exe",
                ["ConnectionStrings:MonitoringDB"] = _testConnectionString
            };
            
            var orchestrator = new SSISPackageOrchestrator(_logger, new TestConfiguration(config));
            var packagePath = Path.Combine(_testPackagePath, "MemoryStressPackage.dtsx");
            var parameters = new Dictionary<string, string>
            {
                ["MemoryUsage"] = "high",
                ["DataSize"] = "500000"
            };

            // Act
            var initialMemory = GC.GetTotalMemory(false);
            var result = await orchestrator.ExecutePackageAsync(packagePath, parameters);
            var finalMemory = GC.GetTotalMemory(false);
            var memoryUsed = finalMemory - initialMemory;

            // Assert
            Assert.IsNotNull(result, "Package execution result should not be null");
            Assert.IsTrue(result.Success, "Package execution should succeed under memory stress");
            Assert.IsTrue(memoryUsed < 200 * 1024 * 1024, "Memory usage should be reasonable even under stress (< 200MB)");
        }

        /// <summary>
        /// Tests SSIS package execution with CPU stress
        /// </summary>
        [TestMethod]
        public async Task Test_StressTest_CPUStress()
        {
            // Arrange
            var config = new Dictionary<string, string>
            {
                ["SSIS:DTExecPath"] = @"C:\Program Files\Microsoft SQL Server\150\DTS\Binn\DTExec.exe",
                ["ConnectionStrings:MonitoringDB"] = _testConnectionString
            };
            
            var orchestrator = new SSISPackageOrchestrator(_logger, new TestConfiguration(config));
            var packagePath = Path.Combine(_testPackagePath, "CPUStressPackage.dtsx");
            var parameters = new Dictionary<string, string>
            {
                ["CPUUsage"] = "high",
                ["ProcessingComplexity"] = "maximum"
            };

            // Act
            var stopwatch = Stopwatch.StartNew();
            var result = await orchestrator.ExecutePackageAsync(packagePath, parameters);
            stopwatch.Stop();

            // Assert
            Assert.IsNotNull(result, "Package execution result should not be null");
            Assert.IsTrue(result.Success, "Package execution should succeed under CPU stress");
            Assert.IsTrue(stopwatch.Elapsed.TotalSeconds < 45, "CPU stress execution should complete within 45 seconds");
        }

        /// <summary>
        /// Tests SSIS package execution with I/O stress
        /// </summary>
        [TestMethod]
        public async Task Test_StressTest_IOStress()
        {
            // Arrange
            var config = new Dictionary<string, string>
            {
                ["SSIS:DTExecPath"] = @"C:\Program Files\Microsoft SQL Server\150\DTS\Binn\DTExec.exe",
                ["ConnectionStrings:MonitoringDB"] = _testConnectionString
            };
            
            var orchestrator = new SSISPackageOrchestrator(_logger, new TestConfiguration(config));
            var packagePath = Path.Combine(_testPackagePath, "IOStressPackage.dtsx");
            var parameters = new Dictionary<string, string>
            {
                ["IOOperations"] = "intensive",
                ["FileCount"] = "100",
                ["FileSize"] = "10000"
            };

            // Act
            var stopwatch = Stopwatch.StartNew();
            var result = await orchestrator.ExecutePackageAsync(packagePath, parameters);
            stopwatch.Stop();

            // Assert
            Assert.IsNotNull(result, "Package execution result should not be null");
            Assert.IsTrue(result.Success, "Package execution should succeed under I/O stress");
            Assert.IsTrue(stopwatch.Elapsed.TotalSeconds < 60, "I/O stress execution should complete within 60 seconds");
        }

        #endregion

        #region Concurrent Execution Tests

        /// <summary>
        /// Tests concurrent SSIS package execution
        /// </summary>
        [TestMethod]
        public async Task Test_ConcurrentExecution_MultiplePackages()
        {
            // Arrange
            var config = new Dictionary<string, string>
            {
                ["SSIS:DTExecPath"] = @"C:\Program Files\Microsoft SQL Server\150\DTS\Binn\DTExec.exe",
                ["ConnectionStrings:MonitoringDB"] = _testConnectionString
            };
            
            var orchestrator = new SSISPackageOrchestrator(_logger, new TestConfiguration(config));
            var packageCount = 10;
            var tasks = new List<Task<PackageExecutionResult>>();

            // Act
            for (int i = 0; i < packageCount; i++)
            {
                var packagePath = Path.Combine(_testPackagePath, $"ConcurrentPackage{i}.dtsx");
                var parameters = new Dictionary<string, string>
                {
                    ["PackageId"] = i.ToString(),
                    ["ExecutionMode"] = "concurrent"
                };
                tasks.Add(orchestrator.ExecutePackageAsync(packagePath, parameters));
            }

            var stopwatch = Stopwatch.StartNew();
            var results = await Task.WhenAll(tasks);
            stopwatch.Stop();

            // Assert
            Assert.AreEqual(packageCount, results.Length, "Should execute all packages concurrently");
            Assert.IsTrue(Array.TrueForAll(results, r => r.Success), "All concurrent packages should execute successfully");
            Assert.IsTrue(stopwatch.Elapsed.TotalSeconds < 30, "Concurrent execution should complete within 30 seconds");
        }

        /// <summary>
        /// Tests concurrent SSIS package execution with different load levels
        /// </summary>
        [TestMethod]
        public async Task Test_ConcurrentExecution_MixedLoads()
        {
            // Arrange
            var config = new Dictionary<string, string>
            {
                ["SSIS:DTExecPath"] = @"C:\Program Files\Microsoft SQL Server\150\DTS\Binn\DTExec.exe",
                ["ConnectionStrings:MonitoringDB"] = _testConnectionString
            };
            
            var orchestrator = new SSISPackageOrchestrator(_logger, new TestConfiguration(config));
            var tasks = new List<Task<PackageExecutionResult>>();

            // Light load packages
            for (int i = 0; i < 5; i++)
            {
                var packagePath = Path.Combine(_testPackagePath, $"LightLoadPackage{i}.dtsx");
                var parameters = new Dictionary<string, string>
                {
                    ["LoadLevel"] = "light",
                    ["RecordCount"] = "1000"
                };
                tasks.Add(orchestrator.ExecutePackageAsync(packagePath, parameters));
            }

            // Medium load packages
            for (int i = 0; i < 3; i++)
            {
                var packagePath = Path.Combine(_testPackagePath, $"MediumLoadPackage{i}.dtsx");
                var parameters = new Dictionary<string, string>
                {
                    ["LoadLevel"] = "medium",
                    ["RecordCount"] = "10000"
                };
                tasks.Add(orchestrator.ExecutePackageAsync(packagePath, parameters));
            }

            // Heavy load packages
            for (int i = 0; i < 2; i++)
            {
                var packagePath = Path.Combine(_testPackagePath, $"HeavyLoadPackage{i}.dtsx");
                var parameters = new Dictionary<string, string>
                {
                    ["LoadLevel"] = "heavy",
                    ["RecordCount"] = "50000"
                };
                tasks.Add(orchestrator.ExecutePackageAsync(packagePath, parameters));
            }

            // Act
            var stopwatch = Stopwatch.StartNew();
            var results = await Task.WhenAll(tasks);
            stopwatch.Stop();

            // Assert
            Assert.AreEqual(10, results.Length, "Should execute all packages with mixed loads");
            Assert.IsTrue(Array.TrueForAll(results, r => r.Success), "All mixed load packages should execute successfully");
            Assert.IsTrue(stopwatch.Elapsed.TotalSeconds < 90, "Mixed load execution should complete within 90 seconds");
        }

        #endregion

        #region Endurance Tests

        /// <summary>
        /// Tests SSIS package execution endurance over time
        /// </summary>
        [TestMethod]
        public async Task Test_EnduranceTest_LongRunningExecution()
        {
            // Arrange
            var config = new Dictionary<string, string>
            {
                ["SSIS:DTExecPath"] = @"C:\Program Files\Microsoft SQL Server\150\DTS\Binn\DTExec.exe",
                ["ConnectionStrings:MonitoringDB"] = _testConnectionString
            };
            
            var orchestrator = new SSISPackageOrchestrator(_logger, new TestConfiguration(config));
            var packagePath = Path.Combine(_testPackagePath, "EndurancePackage.dtsx");
            var parameters = new Dictionary<string, string>
            {
                ["ExecutionDuration"] = "300", // 5 minutes
                ["DataProcessing"] = "continuous"
            };

            // Act
            var stopwatch = Stopwatch.StartNew();
            var result = await orchestrator.ExecutePackageAsync(packagePath, parameters);
            stopwatch.Stop();

            // Assert
            Assert.IsNotNull(result, "Package execution result should not be null");
            Assert.IsTrue(result.Success, "Package execution should succeed during endurance test");
            Assert.IsTrue(stopwatch.Elapsed.TotalSeconds >= 300, "Endurance test should run for at least 5 minutes");
        }

        /// <summary>
        /// Tests SSIS package execution with repeated cycles
        /// </summary>
        [TestMethod]
        public async Task Test_EnduranceTest_RepeatedCycles()
        {
            // Arrange
            var config = new Dictionary<string, string>
            {
                ["SSIS:DTExecPath"] = @"C:\Program Files\Microsoft SQL Server\150\DTS\Binn\DTExec.exe",
                ["ConnectionStrings:MonitoringDB"] = _testConnectionString
            };
            
            var orchestrator = new SSISPackageOrchestrator(_logger, new TestConfiguration(config));
            var packagePath = Path.Combine(_testPackagePath, "CyclePackage.dtsx");
            var cycleCount = 50;
            var results = new List<PackageExecutionResult>();

            // Act
            var stopwatch = Stopwatch.StartNew();
            for (int i = 0; i < cycleCount; i++)
            {
                var parameters = new Dictionary<string, string>
                {
                    ["CycleNumber"] = i.ToString(),
                    ["CycleType"] = "repeated"
                };
                var result = await orchestrator.ExecutePackageAsync(packagePath, parameters);
                results.Add(result);
            }
            stopwatch.Stop();

            // Assert
            Assert.AreEqual(cycleCount, results.Count, "Should execute all cycles");
            Assert.IsTrue(results.TrueForAll(r => r.Success), "All cycles should execute successfully");
            Assert.IsTrue(stopwatch.Elapsed.TotalSeconds < 300, "Repeated cycles should complete within 5 minutes");
        }

        /// <summary>
        /// Tests SSIS package execution with memory leak detection
        /// </summary>
        [TestMethod]
        public async Task Test_EnduranceTest_MemoryLeakDetection()
        {
            // Arrange
            var config = new Dictionary<string, string>
            {
                ["SSIS:DTExecPath"] = @"C:\Program Files\Microsoft SQL Server\150\DTS\Binn\DTExec.exe",
                ["ConnectionStrings:MonitoringDB"] = _testConnectionString
            };
            
            var orchestrator = new SSISPackageOrchestrator(_logger, new TestConfiguration(config));
            var packagePath = Path.Combine(_testPackagePath, "MemoryLeakPackage.dtsx");
            var cycleCount = 100;
            var initialMemory = GC.GetTotalMemory(false);

            // Act
            for (int i = 0; i < cycleCount; i++)
            {
                var parameters = new Dictionary<string, string>
                {
                    ["CycleNumber"] = i.ToString(),
                    ["MemoryTest"] = "true"
                };
                var result = await orchestrator.ExecutePackageAsync(packagePath, parameters);
                Assert.IsTrue(result.Success, $"Cycle {i} should execute successfully");
            }

            // Force garbage collection
            GC.Collect();
            GC.WaitForPendingFinalizers();
            GC.Collect();

            var finalMemory = GC.GetTotalMemory(false);
            var memoryIncrease = finalMemory - initialMemory;

            // Assert
            Assert.IsTrue(memoryIncrease < 50 * 1024 * 1024, "Memory increase should be minimal (< 50MB) to detect memory leaks");
        }

        #endregion

        #region Scalability Tests

        /// <summary>
        /// Tests SSIS package execution scalability with increasing load
        /// </summary>
        [TestMethod]
        public async Task Test_ScalabilityTest_IncreasingLoad()
        {
            // Arrange
            var config = new Dictionary<string, string>
            {
                ["SSIS:DTExecPath"] = @"C:\Program Files\Microsoft SQL Server\150\DTS\Binn\DTExec.exe",
                ["ConnectionStrings:MonitoringDB"] = _testConnectionString
            };
            
            var orchestrator = new SSISPackageOrchestrator(_logger, new TestConfiguration(config));
            var packagePath = Path.Combine(_testPackagePath, "ScalabilityPackage.dtsx");
            var loadLevels = new[] { 1000, 5000, 10000, 25000, 50000 };
            var results = new List<(int load, PackageExecutionResult result, TimeSpan duration)>();

            // Act
            foreach (var load in loadLevels)
            {
                var parameters = new Dictionary<string, string>
                {
                    ["LoadLevel"] = load.ToString(),
                    ["ScalabilityTest"] = "true"
                };

                var stopwatch = Stopwatch.StartNew();
                var result = await orchestrator.ExecutePackageAsync(packagePath, parameters);
                stopwatch.Stop();

                results.Add((load, result, stopwatch.Elapsed));
            }

            // Assert
            Assert.AreEqual(loadLevels.Length, results.Count, "Should test all load levels");
            Assert.IsTrue(results.TrueForAll(r => r.result.Success), "All load levels should execute successfully");
            
            // Verify scalability - execution time should not increase exponentially
            for (int i = 1; i < results.Count; i++)
            {
                var loadRatio = (double)results[i].load / results[i-1].load;
                var timeRatio = results[i].duration.TotalMilliseconds / results[i-1].duration.TotalMilliseconds;
                Assert.IsTrue(timeRatio < loadRatio * 2, $"Time increase should be reasonable for load increase from {results[i-1].load} to {results[i].load}");
            }
        }

        /// <summary>
        /// Tests SSIS package execution scalability with increasing concurrency
        /// </summary>
        [TestMethod]
        public async Task Test_ScalabilityTest_IncreasingConcurrency()
        {
            // Arrange
            var config = new Dictionary<string, string>
            {
                ["SSIS:DTExecPath"] = @"C:\Program Files\Microsoft SQL Server\150\DTS\Binn\DTExec.exe",
                ["ConnectionStrings:MonitoringDB"] = _testConnectionString
            };
            
            var orchestrator = new SSISPackageOrchestrator(_logger, new TestConfiguration(config));
            var packagePath = Path.Combine(_testPackagePath, "ConcurrencyPackage.dtsx");
            var concurrencyLevels = new[] { 1, 5, 10, 20, 50 };
            var results = new List<(int concurrency, TimeSpan duration)>();

            // Act
            foreach (var concurrency in concurrencyLevels)
            {
                var tasks = new List<Task<PackageExecutionResult>>();
                
                for (int i = 0; i < concurrency; i++)
                {
                    var parameters = new Dictionary<string, string>
                    {
                        ["ConcurrencyLevel"] = concurrency.ToString(),
                        ["PackageId"] = i.ToString()
                    };
                    tasks.Add(orchestrator.ExecutePackageAsync(packagePath, parameters));
                }

                var stopwatch = Stopwatch.StartNew();
                var packageResults = await Task.WhenAll(tasks);
                stopwatch.Stop();

                Assert.IsTrue(Array.TrueForAll(packageResults, r => r.Success), $"All packages should succeed with concurrency level {concurrency}");
                results.Add((concurrency, stopwatch.Elapsed));
            }

            // Assert
            Assert.AreEqual(concurrencyLevels.Length, results.Count, "Should test all concurrency levels");
            
            // Verify that higher concurrency doesn't cause exponential time increase
            for (int i = 1; i < results.Count; i++)
            {
                var concurrencyRatio = (double)results[i].concurrency / results[i-1].concurrency;
                var timeRatio = results[i].duration.TotalMilliseconds / results[i-1].duration.TotalMilliseconds;
                Assert.IsTrue(timeRatio < concurrencyRatio * 1.5, $"Time increase should be reasonable for concurrency increase from {results[i-1].concurrency} to {results[i].concurrency}");
            }
        }

        #endregion
    }
}