namespace ETL.Enterprise.Domain.Enums;

/// <summary>
/// Type of processing step
/// </summary>
public enum ProcessingStepType
{
    // Data Processing Steps
    DataExtraction,
    DataTransformation,
    DataValidation,
    DataLoad,
    
    // File Processing Steps
    FileGeneration,
    FileCompression,
    FileValidation,
    FileTransfer,
    
    // SSIS Steps
    SSISPackageExecution,
    SSISDataFlow,
    SSISControlFlow,
    
    // SFTP Steps
    SFTPConnection,
    SFTPUpload,
    SFTPDownload,
    SFTPFileList,
    
    // User Management Steps
    UserCreation,
    UserDeletion,
    UserUpdate,
    UserValidation,
    
    // Monitoring Steps
    JobMonitoring,
    PerformanceMonitoring,
    ErrorMonitoring,
    
    // Custom Steps
    CustomScript,
    CustomTransformation,
    CustomValidation
}

/// <summary>
/// Status of a processing step
/// </summary>
public enum ProcessingStepStatus
{
    Pending,
    Running,
    Completed,
    Failed,
    Skipped,
    Cancelled
}

/// <summary>
/// Log level for processing steps
/// </summary>
public enum ProcessingStepLogLevel
{
    Debug,
    Information,
    Warning,
    Error,
    Critical
}

/// <summary>
/// Log level for processing jobs
/// </summary>
public enum ProcessingJobLogLevel
{
    Debug,
    Information,
    Warning,
    Error,
    Critical
}
