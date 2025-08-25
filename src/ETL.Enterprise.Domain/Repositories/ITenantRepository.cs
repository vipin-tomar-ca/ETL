using ETL.Enterprise.Domain.Entities;
using ETL.Enterprise.Domain.Enums;

namespace ETL.Enterprise.Domain.Repositories;

/// <summary>
/// Repository interface for tenants
/// </summary>
public interface ITenantRepository
{
    /// <summary>
    /// Gets a tenant by ID
    /// </summary>
    Task<Tenant?> GetByIdAsync(Guid id, CancellationToken cancellationToken = default);
    
    /// <summary>
    /// Gets a tenant by tenant code
    /// </summary>
    Task<Tenant?> GetByTenantCodeAsync(string tenantCode, CancellationToken cancellationToken = default);
    
    /// <summary>
    /// Gets all tenants
    /// </summary>
    Task<IEnumerable<Tenant>> GetAllAsync(CancellationToken cancellationToken = default);
    
    /// <summary>
    /// Gets tenants by status
    /// </summary>
    Task<IEnumerable<Tenant>> GetByStatusAsync(
        TenantStatus status, 
        CancellationToken cancellationToken = default);
    
    /// <summary>
    /// Adds a new tenant
    /// </summary>
    Task AddAsync(Tenant tenant, CancellationToken cancellationToken = default);
    
    /// <summary>
    /// Updates an existing tenant
    /// </summary>
    Task UpdateAsync(Tenant tenant, CancellationToken cancellationToken = default);
    
    /// <summary>
    /// Deletes a tenant
    /// </summary>
    Task DeleteAsync(Guid id, CancellationToken cancellationToken = default);
    
    /// <summary>
    /// Saves changes to the underlying data store
    /// </summary>
    Task SaveChangesAsync(CancellationToken cancellationToken = default);
    
    /// <summary>
    /// Gets active tenants
    /// </summary>
    Task<IEnumerable<Tenant>> GetActiveTenantsAsync(CancellationToken cancellationToken = default);
    
    /// <summary>
    /// Gets tenants with filtering and pagination
    /// </summary>
    Task<(IEnumerable<Tenant> Tenants, int TotalCount)> GetTenantsAsync(
        TenantFilterCriteria filterCriteria,
        CancellationToken cancellationToken = default);
}

/// <summary>
/// Filter criteria for tenants
/// </summary>
public class TenantFilterCriteria
{
    public TenantStatus? Status { get; set; }
    public string? NamePattern { get; set; }
    public string? TenantCodePattern { get; set; }
    public DateTime? FromDate { get; set; }
    public DateTime? ToDate { get; set; }
    public int Page { get; set; } = 1;
    public int PageSize { get; set; } = 20;
}
