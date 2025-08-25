using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using ETL.Enterprise.Domain.Entities;
using ETL.Enterprise.Domain.Enums;
using ETL.Enterprise.Domain.Services;
using ETL.Enterprise.Infrastructure.ETL.Engines;

namespace ETL.Enterprise.Infrastructure.ETL;

/// <summary>
/// Factory implementation for creating ETL engine instances
/// </summary>
public class ETLEngineFactory : IETLEngineFactory
{
    private readonly IServiceProvider _serviceProvider;
    private readonly ILogger<ETLEngineFactory> _logger;
    private readonly Dictionary<ETLEngineType, Type> _engineImplementations;

    public ETLEngineFactory(IServiceProvider serviceProvider, ILogger<ETLEngineFactory> logger)
    {
        _serviceProvider = serviceProvider ?? throw new ArgumentNullException(nameof(serviceProvider));
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));
        
        // Register engine implementations
        _engineImplementations = new Dictionary<ETLEngineType, Type>
        {
            { ETLEngineType.CustomCSharp, typeof(CustomCSharpETLEngine) },
            { ETLEngineType.ApacheSpark, typeof(ApacheSparkETLEngine) },
            { ETLEngineType.SSIS, typeof(SSISETLEngine) }
        };
    }

    public IETLEngine CreateEngine(ETLEngineType engineType)
    {
        try
        {
            _logger.LogDebug("Creating ETL engine: {EngineType}", engineType);

            if (!_engineImplementations.TryGetValue(engineType, out var engineImplementationType))
            {
                throw new ArgumentException($"ETL engine type '{engineType}' is not supported");
            }

            var engine = (IETLEngine)ActivatorUtilities.CreateInstance(_serviceProvider, engineImplementationType);
            
            _logger.LogInformation("Successfully created ETL engine: {EngineType} - {EngineName} v{EngineVersion}", 
                engine.EngineType, engine.EngineName, engine.EngineVersion);

            return engine;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error creating ETL engine: {EngineType}", engineType);
            throw;
        }
    }

    public IEnumerable<ETLEngineType> GetAvailableEngineTypes()
    {
        return _engineImplementations.Keys;
    }

    public bool IsEngineAvailable(ETLEngineType engineType)
    {
        return _engineImplementations.ContainsKey(engineType);
    }

    public ETLEngineType GetRecommendedEngine(ETLJobConfiguration configuration)
    {
        try
        {
            _logger.LogDebug("Determining recommended ETL engine for configuration");

            // Analyze configuration to determine the best engine
            var estimatedDataSize = EstimateDataSize(configuration);
            var isRealTime = configuration.Source.Parameters.ContainsKey("realTime") && 
                            bool.Parse(configuration.Source.Parameters["realTime"]);
            var isStreaming = configuration.Source.Parameters.ContainsKey("streaming") && 
                             bool.Parse(configuration.Source.Parameters["streaming"]);

            // Decision logic based on requirements
            if (estimatedDataSize > 1024L * 1024 * 1024 * 10) // > 10GB
            {
                _logger.LogInformation("Large dataset detected ({DataSize} bytes), recommending Apache Spark", estimatedDataSize);
                return ETLEngineType.ApacheSpark;
            }

            if (isRealTime || isStreaming)
            {
                _logger.LogInformation("Real-time/streaming requirements detected, recommending Apache Spark");
                return ETLEngineType.ApacheSpark;
            }

            if (configuration.Source.Provider.Equals("SqlServer", StringComparison.OrdinalIgnoreCase) &&
                configuration.Target.Provider.Equals("SqlServer", StringComparison.OrdinalIgnoreCase))
            {
                _logger.LogInformation("SQL Server to SQL Server ETL detected, recommending SSIS");
                return ETLEngineType.SSIS;
            }

            // Default to Custom C# for smaller datasets and general use cases
            _logger.LogInformation("Defaulting to Custom C# ETL engine");
            return ETLEngineType.CustomCSharp;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error determining recommended ETL engine");
            return ETLEngineType.CustomCSharp; // Safe fallback
        }
    }

    public ETLEngineCapabilities GetEngineCapabilities(ETLEngineType engineType)
    {
        try
        {
            var engine = CreateEngine(engineType);
            return engine.GetCapabilities();
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error getting capabilities for engine: {EngineType}", engineType);
            return new ETLEngineCapabilities(); // Return empty capabilities
        }
    }

    private long EstimateDataSize(ETLJobConfiguration configuration)
    {
        // Simple estimation based on query complexity and parameters
        var baseSize = 1024L * 1024; // 1MB base

        // Adjust based on query complexity
        if (configuration.Source.Query.Contains("JOIN", StringComparison.OrdinalIgnoreCase))
            baseSize *= 2;

        if (configuration.Source.Query.Contains("GROUP BY", StringComparison.OrdinalIgnoreCase))
            baseSize = (long)(baseSize * 1.5);

        if (configuration.Source.Query.Contains("ORDER BY", StringComparison.OrdinalIgnoreCase))
            baseSize = (long)(baseSize * 1.2);

        // Adjust based on batch size
        baseSize *= configuration.Target.BatchSize;

        return baseSize;
    }
}
