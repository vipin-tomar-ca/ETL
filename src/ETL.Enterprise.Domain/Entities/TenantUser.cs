using System.ComponentModel.DataAnnotations;
using ETL.Enterprise.Domain.Enums;

namespace ETL.Enterprise.Domain.Entities;

/// <summary>
/// Represents a user within a tenant
/// </summary>
public class TenantUser
{
    public Guid Id { get; private set; }
    
    public Guid TenantId { get; private set; }
    
    [Required]
    [StringLength(100)]
    public string Username { get; private set; } = string.Empty;
    
    [Required]
    [StringLength(100)]
    public string Email { get; private set; } = string.Empty;
    
    [StringLength(100)]
    public string FirstName { get; private set; } = string.Empty;
    
    [StringLength(100)]
    public string LastName { get; private set; } = string.Empty;
    
    public UserRole Role { get; private set; }
    
    public UserStatus Status { get; private set; }
    
    public DateTime CreatedAt { get; private set; }
    
    public DateTime? LastLoginAt { get; private set; }
    
    public DateTime? PasswordChangedAt { get; private set; }
    
    public DateTime? PasswordExpiresAt { get; private set; }
    
    public int LoginAttempts { get; private set; }
    
    public DateTime? LockedUntil { get; private set; }
    
    public List<UserPermissions> Permissions { get; private set; } = new();
    
    public Dictionary<string, string> CustomProperties { get; private set; } = new();
    
    // Navigation properties
    public Tenant Tenant { get; private set; } = null!;
    
    // Private constructor for EF Core
    private TenantUser() { }
    
    public TenantUser(Guid tenantId, string username, string email, string firstName, string lastName, UserRole role = UserRole.User)
    {
        Id = Guid.NewGuid();
        TenantId = tenantId;
        Username = username ?? throw new ArgumentNullException(nameof(username));
        Email = email ?? throw new ArgumentNullException(nameof(email));
        FirstName = firstName ?? string.Empty;
        LastName = lastName ?? string.Empty;
        Role = role;
        Status = UserStatus.Active;
        CreatedAt = DateTime.UtcNow;
        
        // Set default permissions based on role
        SetDefaultPermissions(role);
    }
    
    public void UpdateProfile(string firstName, string lastName, string email)
    {
        FirstName = firstName ?? string.Empty;
        LastName = lastName ?? string.Empty;
        Email = email ?? throw new ArgumentNullException(nameof(email));
    }
    
    public void SetRole(UserRole role)
    {
        Role = role;
        SetDefaultPermissions(role);
    }
    
    public void SetStatus(UserStatus status)
    {
        Status = status;
        
        if (status == UserStatus.Suspended)
        {
            LockedUntil = DateTime.UtcNow.AddHours(24);
        }
        else if (status == UserStatus.Active)
        {
            LockedUntil = null;
            LoginAttempts = 0;
        }
    }
    
    public void RecordLogin()
    {
        LastLoginAt = DateTime.UtcNow;
        LoginAttempts = 0;
        LockedUntil = null;
    }
    
    public void RecordFailedLogin()
    {
        LoginAttempts++;
        
        if (LoginAttempts >= 5)
        {
            LockedUntil = DateTime.UtcNow.AddMinutes(15);
            Status = UserStatus.Suspended;
        }
    }
    
    public void ChangePassword()
    {
        PasswordChangedAt = DateTime.UtcNow;
        PasswordExpiresAt = DateTime.UtcNow.AddDays(90);
        LoginAttempts = 0;
        LockedUntil = null;
    }
    
    public void AddPermission(UserPermissions permission)
    {
        if (!Permissions.Contains(permission))
        {
            Permissions.Add(permission);
        }
    }
    
    public void RemovePermission(UserPermissions permission)
    {
        Permissions.Remove(permission);
    }
    
    public bool HasPermission(UserPermissions permission)
    {
        return Permissions.Contains(permission) || Role == UserRole.Administrator;
    }
    
    public void AddCustomProperty(string key, string value)
    {
        CustomProperties[key] = value;
    }
    
    public void RemoveCustomProperty(string key)
    {
        if (CustomProperties.ContainsKey(key))
        {
            CustomProperties.Remove(key);
        }
    }
    
    public bool IsLocked()
    {
        return LockedUntil.HasValue && LockedUntil.Value > DateTime.UtcNow;
    }
    
    public bool IsPasswordExpired()
    {
        return PasswordExpiresAt.HasValue && PasswordExpiresAt.Value < DateTime.UtcNow;
    }
    
    public string GetFullName()
    {
        return $"{FirstName} {LastName}".Trim();
    }
    
    private void SetDefaultPermissions(UserRole role)
    {
        Permissions.Clear();
        
        switch (role)
        {
            case UserRole.Administrator:
                Permissions.AddRange(new[] { 
                    UserPermissions.Read, 
                    UserPermissions.Write, 
                    UserPermissions.Delete, 
                    UserPermissions.Execute, 
                    UserPermissions.Admin 
                });
                break;
            case UserRole.Manager:
                Permissions.AddRange(new[] { 
                    UserPermissions.Read, 
                    UserPermissions.Write, 
                    UserPermissions.Execute 
                });
                break;
            case UserRole.User:
                Permissions.AddRange(new[] { 
                    UserPermissions.Read, 
                    UserPermissions.Write 
                });
                break;
            case UserRole.Viewer:
                Permissions.Add(UserPermissions.Read);
                break;
            case UserRole.Guest:
                // No permissions by default
                break;
        }
    }
}

/// <summary>
/// User permissions within a tenant
/// </summary>
public class TenantUserPermissions
{
    public bool CanViewJobs { get; set; } = true;
    public bool CanCreateJobs { get; set; } = false;
    public bool CanModifyJobs { get; set; } = false;
    public bool CanDeleteJobs { get; set; } = false;
    public bool CanViewUsers { get; set; } = false;
    public bool CanCreateUsers { get; set; } = false;
    public bool CanModifyUsers { get; set; } = false;
    public bool CanDeleteUsers { get; set; } = false;
    public bool CanViewTenantSettings { get; set; } = false;
    public bool CanModifyTenantSettings { get; set; } = false;
    public bool CanViewReports { get; set; } = true;
    public bool CanExportData { get; set; } = false;
    public bool CanAccessSFTP { get; set; } = false;
    
    public List<string> CustomPermissions { get; set; } = new();
    
    public bool HasPermission(string permission)
    {
        return permission switch
        {
            "ViewJobs" => CanViewJobs,
            "CreateJobs" => CanCreateJobs,
            "ModifyJobs" => CanModifyJobs,
            "DeleteJobs" => CanDeleteJobs,
            "ViewUsers" => CanViewUsers,
            "CreateUsers" => CanCreateUsers,
            "ModifyUsers" => CanModifyUsers,
            "DeleteUsers" => CanDeleteUsers,
            "ViewTenantSettings" => CanViewTenantSettings,
            "ModifyTenantSettings" => CanModifyTenantSettings,
            "ViewReports" => CanViewReports,
            "ExportData" => CanExportData,
            "AccessSFTP" => CanAccessSFTP,
            _ => CustomPermissions.Contains(permission)
        };
    }
}
