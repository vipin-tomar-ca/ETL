namespace ETL.Enterprise.Domain.Enums;

/// <summary>
/// Represents the type of ETL engine to use for processing
/// </summary>
public enum ETLEngineType
{
    /// <summary>
    /// Custom C# implementation using .NET
    /// </summary>
    CustomCSharp,
    
    /// <summary>
    /// Apache Spark for big data processing
    /// </summary>
    ApacheSpark,
    
    /// <summary>
    /// SQL Server Integration Services (SSIS)
    /// </summary>
    SSIS,
    
    /// <summary>
    /// Azure Data Factory
    /// </summary>
    AzureDataFactory,
    
    /// <summary>
    /// Apache Airflow
    /// </summary>
    ApacheAirflow,
    
    /// <summary>
    /// Talend
    /// </summary>
    Talend,
    
    /// <summary>
    /// Informatica
    /// </summary>
    Informatica
}
