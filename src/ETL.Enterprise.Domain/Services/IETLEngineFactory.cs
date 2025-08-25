using ETL.Enterprise.Domain.Entities;
using ETL.Enterprise.Domain.Enums;

namespace ETL.Enterprise.Domain.Services;

/// <summary>
/// Factory interface for creating ETL engine instances
/// </summary>
public interface IETLEngineFactory
{
    /// <summary>
    /// Creates an ETL engine instance based on the engine type
    /// </summary>
    IETLEngine CreateEngine(ETLEngineType engineType);
    
    /// <summary>
    /// Gets all available ETL engine types
    /// </summary>
    IEnumerable<ETLEngineType> GetAvailableEngineTypes();
    
    /// <summary>
    /// Checks if an ETL engine type is available
    /// </summary>
    bool IsEngineAvailable(ETLEngineType engineType);
    
    /// <summary>
    /// Gets the recommended engine type for a given configuration
    /// </summary>
    ETLEngineType GetRecommendedEngine(ETLJobConfiguration configuration);
    
    /// <summary>
    /// Gets engine capabilities for comparison
    /// </summary>
    ETLEngineCapabilities GetEngineCapabilities(ETLEngineType engineType);
}
