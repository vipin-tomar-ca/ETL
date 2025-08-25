using ETL.Enterprise.Domain.Enums;

namespace ETL.Enterprise.Application.DTOs;

/// <summary>
/// Data Transfer Object for ETL Engine Configuration
/// </summary>
public class ETLEngineConfigurationDto
{
    /// <summary>
    /// The type of ETL engine to use
    /// </summary>
    public ETLEngineType EngineType { get; set; } = ETLEngineType.CustomCSharp;
    
    /// <summary>
    /// Whether to auto-select the best engine based on configuration
    /// </summary>
    public bool AutoSelectEngine { get; set; } = true;
    
    /// <summary>
    /// Engine-specific configuration parameters
    /// </summary>
    public Dictionary<string, string> EngineParameters { get; set; } = new();
    
    /// <summary>
    /// Connection string or endpoint for the ETL engine
    /// </summary>
    public string? EngineConnectionString { get; set; }
    
    /// <summary>
    /// Authentication credentials for the ETL engine
    /// </summary>
    public Dictionary<string, string> Authentication { get; set; } = new();
    
    /// <summary>
    /// Resource allocation settings
    /// </summary>
    public ResourceAllocationDto Resources { get; set; } = new();
    
    /// <summary>
    /// Fallback engine types if primary engine fails
    /// </summary>
    public List<ETLEngineType> FallbackEngines { get; set; } = new();
    
    /// <summary>
    /// Whether to enable engine-specific optimizations
    /// </summary>
    public bool EnableOptimizations { get; set; } = true;
    
    /// <summary>
    /// Engine-specific timeout settings
    /// </summary>
    public TimeSpan? EngineTimeout { get; set; }
    
    /// <summary>
    /// Whether to enable engine monitoring
    /// </summary>
    public bool EnableMonitoring { get; set; } = true;
}

/// <summary>
/// Data Transfer Object for Resource Allocation
/// </summary>
public class ResourceAllocationDto
{
    /// <summary>
    /// Number of CPU cores to allocate
    /// </summary>
    public int CpuCores { get; set; } = Environment.ProcessorCount;
    
    /// <summary>
    /// Memory allocation in MB
    /// </summary>
    public int MemoryMB { get; set; } = 1024;
    
    /// <summary>
    /// Number of executors (for distributed engines like Spark)
    /// </summary>
    public int Executors { get; set; } = 1;
    
    /// <summary>
    /// Executor memory in MB
    /// </summary>
    public int ExecutorMemoryMB { get; set; } = 512;
    
    /// <summary>
    /// Driver memory in MB (for Spark)
    /// </summary>
    public int DriverMemoryMB { get; set; } = 512;
    
    /// <summary>
    /// Whether to enable dynamic allocation
    /// </summary>
    public bool EnableDynamicAllocation { get; set; } = false;
    
    /// <summary>
    /// Maximum number of executors
    /// </summary>
    public int MaxExecutors { get; set; } = 10;
    
    /// <summary>
    /// Minimum number of executors
    /// </summary>
    public int MinExecutors { get; set; } = 1;
}
