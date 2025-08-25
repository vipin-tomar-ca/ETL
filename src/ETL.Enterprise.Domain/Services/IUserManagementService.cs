using ETL.Enterprise.Domain.Entities;
using ETL.Enterprise.Domain.Enums;

namespace ETL.Enterprise.Domain.Services;

/// <summary>
/// Service for user management operations
/// </summary>
public interface IUserManagementService
{
    /// <summary>
    /// Creates a new user for a tenant
    /// </summary>
    Task<UserManagementResult> CreateUserAsync(
        Guid tenantId,
        string username,
        string email,
        string firstName,
        string lastName,
        UserRole role,
        List<UserPermissions> permissions,
        CancellationToken cancellationToken = default);
    
    /// <summary>
    /// Deletes a user from a tenant
    /// </summary>
    Task<UserManagementResult> DeleteUserAsync(
        Guid tenantId,
        Guid userId,
        CancellationToken cancellationToken = default);
    
    /// <summary>
    /// Updates user information
    /// </summary>
    Task<UserManagementResult> UpdateUserAsync(
        Guid tenantId,
        Guid userId,
        string firstName,
        string lastName,
        string email,
        UserRole role,
        List<UserPermissions> permissions,
        CancellationToken cancellationToken = default);
    
    /// <summary>
    /// Activates a user
    /// </summary>
    Task<UserManagementResult> ActivateUserAsync(
        Guid tenantId,
        Guid userId,
        CancellationToken cancellationToken = default);
    
    /// <summary>
    /// Deactivates a user
    /// </summary>
    Task<UserManagementResult> DeactivateUserAsync(
        Guid tenantId,
        Guid userId,
        CancellationToken cancellationToken = default);
    
    /// <summary>
    /// Gets user information
    /// </summary>
    Task<TenantUser?> GetUserAsync(
        Guid tenantId,
        Guid userId,
        CancellationToken cancellationToken = default);
    
    /// <summary>
    /// Lists all users for a tenant
    /// </summary>
    Task<List<TenantUser>> GetUsersAsync(
        Guid tenantId,
        CancellationToken cancellationToken = default);
    
    /// <summary>
    /// Validates user permissions
    /// </summary>
    Task<bool> ValidateUserPermissionAsync(
        Guid tenantId,
        Guid userId,
        string permission,
        CancellationToken cancellationToken = default);
}

/// <summary>
/// Result of user management operation
/// </summary>
public class UserManagementResult
{
    public bool IsSuccess { get; set; }
    public TenantUser? User { get; set; }
    public string? ErrorMessage { get; set; }
    public List<string> Warnings { get; set; } = new();
    public Dictionary<string, object>? Metadata { get; set; }
    
    public static UserManagementResult Success(TenantUser user) =>
        new() { IsSuccess = true, User = user };
    
    public static UserManagementResult Failure(string errorMessage) =>
        new() { IsSuccess = false, ErrorMessage = errorMessage };
}
