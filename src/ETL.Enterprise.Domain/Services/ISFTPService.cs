using ETL.Enterprise.Domain.Entities;
using ETL.Enterprise.Domain.Enums;

namespace ETL.Enterprise.Domain.Services;

/// <summary>
/// Service for SFTP operations
/// </summary>
public interface ISFTPService
{
    /// <summary>
    /// Uploads a file to SFTP server
    /// </summary>
    Task<SFTPResult> UploadFileAsync(
        string localFilePath,
        string remoteFilePath,
        SFTPConfiguration configuration,
        CancellationToken cancellationToken = default);
    
    /// <summary>
    /// Downloads a file from SFTP server
    /// </summary>
    Task<SFTPResult> DownloadFileAsync(
        string remoteFilePath,
        string localFilePath,
        SFTPConfiguration configuration,
        CancellationToken cancellationToken = default);
    
    /// <summary>
    /// Lists files in a remote directory
    /// </summary>
    Task<SFTPListResult> ListFilesAsync(
        string remoteDirectory,
        SFTPConfiguration configuration,
        CancellationToken cancellationToken = default);
    
    /// <summary>
    /// Deletes a file from SFTP server
    /// </summary>
    Task<SFTPResult> DeleteFileAsync(
        string remoteFilePath,
        SFTPConfiguration configuration,
        CancellationToken cancellationToken = default);
    
    /// <summary>
    /// Creates a directory on SFTP server
    /// </summary>
    Task<SFTPResult> CreateDirectoryAsync(
        string remoteDirectory,
        SFTPConfiguration configuration,
        CancellationToken cancellationToken = default);
    
    /// <summary>
    /// Tests SFTP connection
    /// </summary>
    Task<SFTPConnectionResult> TestConnectionAsync(
        SFTPConfiguration configuration,
        CancellationToken cancellationToken = default);
}

/// <summary>
/// Result of SFTP operation
/// </summary>
public class SFTPResult
{
    public bool IsSuccess { get; set; }
    public SFTPOperationType OperationType { get; set; }
    public string? LocalPath { get; set; }
    public string? RemotePath { get; set; }
    public long FileSize { get; set; }
    public TimeSpan Duration { get; set; }
    public string? ErrorMessage { get; set; }
    public Dictionary<string, object>? Metadata { get; set; }
    
    public static SFTPResult Success(SFTPOperationType operationType, string? localPath, string? remotePath, long fileSize, TimeSpan duration) =>
        new() 
        { 
            IsSuccess = true, 
            OperationType = operationType, 
            LocalPath = localPath, 
            RemotePath = remotePath, 
            FileSize = fileSize, 
            Duration = duration 
        };
    
    public static SFTPResult Failure(SFTPOperationType operationType, string errorMessage) =>
        new() { IsSuccess = false, OperationType = operationType, ErrorMessage = errorMessage };
}

/// <summary>
/// Result of SFTP list operation
/// </summary>
public class SFTPListResult
{
    public bool IsSuccess { get; set; }
    public string RemoteDirectory { get; set; } = string.Empty;
    public List<SFTPFileInfo> Files { get; set; } = new();
    public TimeSpan Duration { get; set; }
    public string? ErrorMessage { get; set; }
    
    public static SFTPListResult Success(string remoteDirectory, List<SFTPFileInfo> files, TimeSpan duration) =>
        new() { IsSuccess = true, RemoteDirectory = remoteDirectory, Files = files, Duration = duration };
    
    public static SFTPListResult Failure(string remoteDirectory, string errorMessage) =>
        new() { IsSuccess = false, RemoteDirectory = remoteDirectory, ErrorMessage = errorMessage };
}

/// <summary>
/// SFTP file information
/// </summary>
public class SFTPFileInfo
{
    public string Name { get; set; } = string.Empty;
    public string FullPath { get; set; } = string.Empty;
    public long Size { get; set; }
    public DateTime ModifiedDate { get; set; }
    public bool IsDirectory { get; set; }
    public string Permissions { get; set; } = string.Empty;
}

/// <summary>
/// Result of SFTP connection test
/// </summary>
public class SFTPConnectionResult
{
    public bool IsSuccess { get; set; }
    public string Host { get; set; } = string.Empty;
    public int Port { get; set; }
    public TimeSpan ResponseTime { get; set; }
    public string? ErrorMessage { get; set; }
    public Dictionary<string, object>? ConnectionInfo { get; set; }
    
    public static SFTPConnectionResult Success(string host, int port, TimeSpan responseTime) =>
        new() { IsSuccess = true, Host = host, Port = port, ResponseTime = responseTime };
    
    public static SFTPConnectionResult Failure(string host, int port, string errorMessage) =>
        new() { IsSuccess = false, Host = host, Port = port, ErrorMessage = errorMessage };
}
