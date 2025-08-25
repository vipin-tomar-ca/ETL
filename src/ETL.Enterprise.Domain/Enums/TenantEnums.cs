namespace ETL.Enterprise.Domain.Enums;

/// <summary>
/// Status of a tenant
/// </summary>
public enum TenantStatus
{
    Created,
    Active,
    Inactive,
    Suspended,
    Deleted
}

/// <summary>
/// Status of a user
/// </summary>
public enum UserStatus
{
    Active,
    Inactive,
    Suspended,
    PendingVerification
}

/// <summary>
/// Role of a user within a tenant
/// </summary>
public enum UserRole
{
    User,
    PowerUser,
    Manager,
    Administrator,
    SystemAdministrator,
    Viewer,
    Guest
}

/// <summary>
/// Type of data processing job
/// </summary>
public enum ProcessingJobType
{
    InitialLoad,
    DeltaLoad,
    FullRefresh,
    IncrementalUpdate,
    DataExport,
    FileGeneration,
    SFTPUpload
}

/// <summary>
/// Status of a processing job
/// </summary>
public enum ProcessingJobStatus
{
    Pending,
    Running,
    Completed,
    Failed,
    Cancelled,
    Paused
}

/// <summary>
/// Type of file format for export
/// </summary>
public enum FileFormat
{
    CSV,
    JSON,
    XML,
    Excel,
    Parquet,
    ZIP
}

/// <summary>
/// Type of SFTP operation
/// </summary>
public enum SFTPOperationType
{
    Upload,
    Download,
    Delete,
    List,
    CreateDirectory
}

/// <summary>
/// User permissions in the system
/// </summary>
public enum UserPermissions
{
    Read,
    Write,
    Delete,
    Execute,
    Admin
}
