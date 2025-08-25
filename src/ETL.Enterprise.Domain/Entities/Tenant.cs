using System.ComponentModel.DataAnnotations;
using ETL.Enterprise.Domain.Enums;

namespace ETL.Enterprise.Domain.Entities;

/// <summary>
/// Represents a tenant in the multi-tenant ETL system
/// </summary>
public class Tenant
{
    public Guid Id { get; private set; }
    
    [Required]
    [StringLength(50)]
    public string TenantCode { get; private set; } = string.Empty;
    
    [Required]
    [StringLength(100)]
    public string Name { get; private set; } = string.Empty;
    
    [StringLength(500)]
    public string Description { get; private set; } = string.Empty;
    
    public TenantStatus Status { get; private set; }
    
    public DateTime CreatedAt { get; private set; }
    
    public DateTime? UpdatedAt { get; private set; }
    
    public DateTime? ExpiresAt { get; private set; }
    
    public string DatabaseConnectionString { get; private set; } = string.Empty;
    
    public string StorageConnectionString { get; private set; } = string.Empty;
    
    public Dictionary<string, string> CustomSettings { get; private set; } = new();
    
    public int MaxUsers { get; private set; } = 100;
    
    public int MaxStorageGB { get; private set; } = 10;
    
    // Navigation properties
    public List<ProcessingJob> ProcessingJobs { get; private set; } = new();
    public List<TenantUser> Users { get; private set; } = new();
    
    // Private constructor for EF Core
    private Tenant() { }
    
    public Tenant(string tenantCode, string name, string description = "")
    {
        Id = Guid.NewGuid();
        TenantCode = tenantCode ?? throw new ArgumentNullException(nameof(tenantCode));
        Name = name ?? throw new ArgumentNullException(nameof(name));
        Description = description;
        Status = TenantStatus.Active;
        CreatedAt = DateTime.UtcNow;
    }
    
    public void UpdateDetails(string name, string description)
    {
        Name = name ?? throw new ArgumentNullException(nameof(name));
        Description = description ?? string.Empty;
        UpdatedAt = DateTime.UtcNow;
    }
    
    public void SetStatus(TenantStatus status)
    {
        Status = status;
        UpdatedAt = DateTime.UtcNow;
    }
    
    public void SetExpiration(DateTime? expirationDate)
    {
        ExpiresAt = expirationDate;
        UpdatedAt = DateTime.UtcNow;
    }
    
    public void UpdateConnectionStrings(string databaseConnectionString, string storageConnectionString)
    {
        DatabaseConnectionString = databaseConnectionString ?? string.Empty;
        StorageConnectionString = storageConnectionString ?? string.Empty;
        UpdatedAt = DateTime.UtcNow;
    }
    
    public void AddCustomSetting(string key, string value)
    {
        CustomSettings[key] = value;
        UpdatedAt = DateTime.UtcNow;
    }
    
    public void RemoveCustomSetting(string key)
    {
        if (CustomSettings.ContainsKey(key))
        {
            CustomSettings.Remove(key);
            UpdatedAt = DateTime.UtcNow;
        }
    }
    
    public void UpdateLimits(int maxUsers, int maxStorageGB)
    {
        MaxUsers = maxUsers;
        MaxStorageGB = maxStorageGB;
        UpdatedAt = DateTime.UtcNow;
    }
    
    public bool IsActive()
    {
        return Status == TenantStatus.Active && 
               (ExpiresAt == null || ExpiresAt > DateTime.UtcNow);
    }
    
    public bool CanAddUser()
    {
        return Users.Count < MaxUsers;
    }
    
    public bool HasAvailableStorage(long requiredGB)
    {
        // This would need to be calculated based on actual storage usage
        return true; // Placeholder implementation
    }
}

/// <summary>
/// Configuration for a tenant
/// </summary>
public class TenantConfiguration
{
    public DatabaseConfiguration Database { get; set; } = new();
    public SFTPConfiguration SFTP { get; set; } = new();
    public FileProcessingConfiguration FileProcessing { get; set; } = new();
    public UserManagementConfiguration UserManagement { get; set; } = new();
    public MonitoringConfiguration Monitoring { get; set; } = new();
}

/// <summary>
/// Database configuration for tenant
/// </summary>
public class DatabaseConfiguration
{
    public string ConnectionString { get; set; } = string.Empty;
    public string DatabaseName { get; set; } = string.Empty;
    public string Schema { get; set; } = "dbo";
    public int MaxConnections { get; set; } = 100;
    public TimeSpan CommandTimeout { get; set; } = TimeSpan.FromMinutes(30);
}

/// <summary>
/// SFTP configuration for tenant
/// </summary>
public class SFTPConfiguration
{
    public string Host { get; set; } = string.Empty;
    public int Port { get; set; } = 22;
    public string Username { get; set; } = string.Empty;
    public string Password { get; set; } = string.Empty;
    public string PrivateKeyPath { get; set; } = string.Empty;
    public string RemoteDirectory { get; set; } = "/";
    public bool UseCompression { get; set; } = true;
    public int RetryAttempts { get; set; } = 3;
}

/// <summary>
/// File processing configuration for tenant
/// </summary>
public class FileProcessingConfiguration
{
    public string OutputDirectory { get; set; } = string.Empty;
    public string FileFormat { get; set; } = "ZIP";
    public bool IncludeHeaders { get; set; } = true;
    public string Delimiter { get; set; } = ",";
    public int MaxFileSizeMB { get; set; } = 100;
    public bool EnableCompression { get; set; } = true;
}

/// <summary>
/// User management configuration for tenant
/// </summary>
public class UserManagementConfiguration
{
    public bool AutoCreateUsers { get; set; } = true;
    public string DefaultRole { get; set; } = "User";
    public bool RequireEmailVerification { get; set; } = true;
    public int PasswordMinLength { get; set; } = 8;
    public bool RequireComplexPassword { get; set; } = true;
}

/// <summary>
/// Monitoring configuration for tenant
/// </summary>
public class MonitoringConfiguration
{
    public bool EnableJobMonitoring { get; set; } = true;
    public bool EnablePerformanceMonitoring { get; set; } = true;
    public bool EnableErrorAlerting { get; set; } = true;
    public string AlertEmail { get; set; } = string.Empty;
    public TimeSpan MonitoringInterval { get; set; } = TimeSpan.FromMinutes(5);
}
