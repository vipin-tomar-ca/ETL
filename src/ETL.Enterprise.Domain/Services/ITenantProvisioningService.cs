using ETL.Enterprise.Domain.Entities;
using ETL.Enterprise.Domain.Enums;

namespace ETL.Enterprise.Domain.Services;

/// <summary>
/// Service for provisioning and managing tenants
/// </summary>
public interface ITenantProvisioningService
{
    /// <summary>
    /// Creates a new tenant with all necessary resources
    /// </summary>
    Task<TenantProvisioningResult> ProvisionTenantAsync(
        string name, 
        string description, 
        string tenantCode, 
        TenantConfiguration configuration,
        CancellationToken cancellationToken = default);
    
    /// <summary>
    /// Deactivates a tenant and cleans up resources
    /// </summary>
    Task<TenantProvisioningResult> DeactivateTenantAsync(
        Guid tenantId, 
        CancellationToken cancellationToken = default);
    
    /// <summary>
    /// Updates tenant configuration
    /// </summary>
    Task<TenantProvisioningResult> UpdateTenantConfigurationAsync(
        Guid tenantId, 
        TenantConfiguration configuration,
        CancellationToken cancellationToken = default);
    
    /// <summary>
    /// Validates tenant configuration
    /// </summary>
    Task<ValidationResult> ValidateTenantConfigurationAsync(
        TenantConfiguration configuration,
        CancellationToken cancellationToken = default);
    
    /// <summary>
    /// Gets tenant provisioning status
    /// </summary>
    Task<TenantProvisioningStatus> GetTenantStatusAsync(
        Guid tenantId,
        CancellationToken cancellationToken = default);
}

/// <summary>
/// Result of tenant provisioning operation
/// </summary>
public class TenantProvisioningResult
{
    public bool IsSuccess { get; set; }
    public Tenant? Tenant { get; set; }
    public string? ErrorMessage { get; set; }
    public List<string> Warnings { get; set; } = new();
    public Dictionary<string, object>? Metadata { get; set; }
    
    public static TenantProvisioningResult Success(Tenant tenant) =>
        new() { IsSuccess = true, Tenant = tenant };
    
    public static TenantProvisioningResult Failure(string errorMessage) =>
        new() { IsSuccess = false, ErrorMessage = errorMessage };
}

/// <summary>
/// Status of tenant provisioning
/// </summary>
public class TenantProvisioningStatus
{
    public Guid TenantId { get; set; }
    public TenantStatus Status { get; set; }
    public bool IsDatabaseProvisioned { get; set; }
    public bool IsSFTPConfigured { get; set; }
    public bool IsFileSystemReady { get; set; }
    public bool IsUserManagementReady { get; set; }
    public List<string> PendingTasks { get; set; } = new();
    public List<string> CompletedTasks { get; set; } = new();
    public List<string> FailedTasks { get; set; } = new();
}
