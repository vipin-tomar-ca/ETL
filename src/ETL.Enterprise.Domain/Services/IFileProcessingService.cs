using ETL.Enterprise.Domain.Entities;
using ETL.Enterprise.Domain.Enums;

namespace ETL.Enterprise.Domain.Services;

/// <summary>
/// Service for file processing operations
/// </summary>
public interface IFileProcessingService
{
    /// <summary>
    /// Generates files from database data
    /// </summary>
    Task<FileProcessingResult> GenerateFileAsync(
        Guid tenantId,
        string query,
        FileExportConfiguration configuration,
        CancellationToken cancellationToken = default);
    
    /// <summary>
    /// Compresses files into ZIP format
    /// </summary>
    Task<FileProcessingResult> CompressFilesAsync(
        string sourceDirectory,
        string outputFilePath,
        bool deleteSourceFiles,
        CancellationToken cancellationToken = default);
    
    /// <summary>
    /// Validates generated files
    /// </summary>
    Task<FileValidationResult> ValidateFileAsync(
        string filePath,
        FileFormat format,
        CancellationToken cancellationToken = default);
    
    /// <summary>
    /// Splits large files into smaller chunks
    /// </summary>
    Task<FileProcessingResult> SplitLargeFileAsync(
        string sourceFilePath,
        string outputDirectory,
        int maxFileSizeMB,
        CancellationToken cancellationToken = default);
    
    /// <summary>
    /// Converts files between different formats
    /// </summary>
    Task<FileProcessingResult> ConvertFileFormatAsync(
        string sourceFilePath,
        string outputFilePath,
        FileFormat targetFormat,
        CancellationToken cancellationToken = default);
}

/// <summary>
/// Result of file processing operation
/// </summary>
public class FileProcessingResult
{
    public bool IsSuccess { get; set; }
    public string? FilePath { get; set; }
    public long FileSize { get; set; }
    public int RecordCount { get; set; }
    public TimeSpan Duration { get; set; }
    public string? ErrorMessage { get; set; }
    public List<string> Warnings { get; set; } = new();
    public Dictionary<string, object>? Metadata { get; set; }
    
    public static FileProcessingResult Success(string filePath, long fileSize, int recordCount, TimeSpan duration) =>
        new() 
        { 
            IsSuccess = true, 
            FilePath = filePath, 
            FileSize = fileSize, 
            RecordCount = recordCount, 
            Duration = duration 
        };
    
    public static FileProcessingResult Failure(string errorMessage) =>
        new() { IsSuccess = false, ErrorMessage = errorMessage };
}

/// <summary>
/// Result of file validation operation
/// </summary>
public class FileValidationResult
{
    public bool IsValid { get; set; }
    public string FilePath { get; set; } = string.Empty;
    public long FileSize { get; set; }
    public int RecordCount { get; set; }
    public List<string> ValidationErrors { get; set; } = new();
    public List<string> ValidationWarnings { get; set; } = new();
    public Dictionary<string, object>? ValidationMetrics { get; set; }
    
    public static FileValidationResult Valid(string filePath, long fileSize, int recordCount) =>
        new() { IsValid = true, FilePath = filePath, FileSize = fileSize, RecordCount = recordCount };
    
    public static FileValidationResult Invalid(string filePath, List<string> errors) =>
        new() { IsValid = false, FilePath = filePath, ValidationErrors = errors };
}
