# SSIS Package Deployment Script

This PowerShell script (`deploy_ssis.ps1`) automates the deployment of SSIS packages to the SSIS Catalog with comprehensive error handling, logging, and environment variable configuration.

## Features

- **SQL Server Connection**: Supports both Windows Authentication and SQL Server Authentication
- **Package Deployment**: Deploys all `.dtsx` files from a specified directory
- **Environment Variables**: Configures dynamic connection strings and other environment variables
- **Error Handling**: Comprehensive error handling with detailed logging
- **Logging**: Logs all operations to both console and file
- **Folder Management**: Creates SSIS Catalog folders if they don't exist
- **Force Deployment**: Option to overwrite existing packages

## Prerequisites

1. **SQL Server**: SQL Server instance with SSIS Catalog enabled
2. **PowerShell**: PowerShell 5.1 or later
3. **SSIS Tools**: `dtutil.exe` must be available in the system PATH
4. **Permissions**: User must have appropriate permissions on the SSIS Catalog
5. **Network Access**: Access to the SQL Server instance and file system

## Parameters

| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| `SqlServerInstance` | string | Yes | - | SQL Server instance name (e.g., "localhost" or "server\instance") |
| `DatabaseName` | string | Yes | - | Database name (typically "SSISDB") |
| `Username` | string | No | - | SQL Server username (required if not using Windows Authentication) |
| `Password` | string | No | - | SQL Server password (required if not using Windows Authentication) |
| `FolderName` | string | No | "ETLFramework" | SSIS Catalog folder name |
| `ProjectName` | string | No | "ETLProject" | SSIS project name |
| `PackagesPath` | string | No | ".\ETL.SSIS\Packages" | Path to directory containing `.dtsx` files |
| `LogFilePath` | string | No | ".\deploy_ssis.log" | Path to log file |
| `UseWindowsAuthentication` | switch | No | $true | Use Windows Authentication (default) |
| `Force` | switch | No | $false | Overwrite existing packages |

## Usage Examples

### Basic Usage (Windows Authentication)
```powershell
.\deploy_ssis.ps1 -SqlServerInstance "localhost" -DatabaseName "SSISDB"
```

### SQL Server Authentication
```powershell
.\deploy_ssis.ps1 -SqlServerInstance "server\instance" -DatabaseName "SSISDB" -Username "sa" -Password "password" -UseWindowsAuthentication:$false
```

### Custom Folder and Project Names
```powershell
.\deploy_ssis.ps1 -SqlServerInstance "localhost" -DatabaseName "SSISDB" -FolderName "MyETL" -ProjectName "DataWarehouse"
```

### Custom Packages Path
```powershell
.\deploy_ssis.ps1 -SqlServerInstance "localhost" -DatabaseName "SSISDB" -PackagesPath "C:\SSIS\Packages"
```

### Force Deployment (Overwrite Existing)
```powershell
.\deploy_ssis.ps1 -SqlServerInstance "localhost" -DatabaseName "SSISDB" -Force
```

### Custom Log File
```powershell
.\deploy_ssis.ps1 -SqlServerInstance "localhost" -DatabaseName "SSISDB" -LogFilePath "C:\Logs\ssis_deployment.log"
```

## Directory Structure

The script expects the following directory structure:
```
ETL-scalable/
├── deploy_ssis.ps1
├── ETL.SSIS/
│   └── Packages/
│       ├── Package1.dtsx
│       ├── Package2.dtsx
│       └── ...
└── deploy_ssis.log
```

## Environment Variables

The script configures the following environment variables in the SSIS Catalog:

| Variable Name | Description | Default Value |
|---------------|-------------|---------------|
| `SourceConnectionString` | Source database connection string | `Server=SourceServer;Database=SourceDB;Integrated Security=true;` |
| `TargetConnectionString` | Target database connection string | `Server=TargetServer;Database=TargetDB;Integrated Security=true;` |
| `FileSharePath` | File share path for data files | `\\server\share\data` |
| `LogLevel` | Logging level | `INFO` |
| `BatchSize` | Batch size for data processing | `1000` |

### Customizing Environment Variables

To customize environment variables, modify the `$envVars` hashtable in the `Set-SsisEnvironmentVariables` function:

```powershell
$envVars = @{
    "SourceConnectionString" = "Server=YourSourceServer;Database=YourSourceDB;Integrated Security=true;"
    "TargetConnectionString" = "Server=YourTargetServer;Database=YourTargetDB;Integrated Security=true;"
    "FileSharePath" = "\\yourserver\yourshare\data"
    "LogLevel" = "DEBUG"
    "BatchSize" = "5000"
    "CustomVariable" = "CustomValue"
}
```

## Logging

The script provides comprehensive logging with the following levels:
- **INFO**: General information messages
- **SUCCESS**: Successful operations
- **WARNING**: Warning messages (non-critical issues)
- **ERROR**: Error messages (critical issues)

Log messages include timestamps and are written to both:
1. Console output
2. Log file (specified by `LogFilePath` parameter)

### Sample Log Output
```
[2024-01-15 10:30:15] [INFO] Starting SSIS deployment process...
[2024-01-15 10:30:15] [INFO] Parameters: Server=localhost, Database=SSISDB, Folder=ETLFramework
[2024-01-15 10:30:16] [SUCCESS] Successfully connected to SQL Server: localhost
[2024-01-15 10:30:16] [INFO] SSIS Folder operation: Folder created successfully
[2024-01-15 10:30:17] [INFO] Found 3 package(s) to deploy
[2024-01-15 10:30:17] [INFO] Deploying package: Package1.dtsx
[2024-01-15 10:30:18] [SUCCESS] Successfully deployed package: Package1.dtsx
[2024-01-15 10:30:19] [SUCCESS] Successfully configured environment variables
[2024-01-15 10:30:19] [SUCCESS] SSIS deployment process completed
```

## Error Handling

The script includes comprehensive error handling:

1. **Connection Testing**: Validates SQL Server connection before proceeding
2. **File Existence**: Checks for package files and directories
3. **Deployment Errors**: Captures and logs deployment failures
4. **Environment Variable Errors**: Handles configuration failures gracefully
5. **Exception Handling**: Catches and logs all exceptions

## Troubleshooting

### Common Issues

1. **Connection Failed**
   - Verify SQL Server instance name
   - Check network connectivity
   - Ensure proper authentication credentials
   - Verify user has access to SSISDB

2. **Packages Not Found**
   - Verify `PackagesPath` parameter
   - Ensure `.dtsx` files exist in the specified directory
   - Check file permissions

3. **Deployment Failed**
   - Verify `dtutil.exe` is in PATH
   - Check SSIS Catalog permissions
   - Review package dependencies
   - Use `-Force` parameter to overwrite existing packages

4. **Environment Variable Errors**
   - Check SSIS Catalog permissions
   - Verify folder and environment names
   - Review variable names for special characters

### Debug Mode

To enable verbose logging, modify the script to include `-Verbose` parameter:

```powershell
# Add this line at the beginning of the main execution block
$VerbosePreference = "Continue"
```

### Manual Verification

After deployment, verify the deployment in SQL Server Management Studio:

1. Connect to the SQL Server instance
2. Navigate to Integration Services Catalogs > SSISDB
3. Check the specified folder and project
4. Verify packages are deployed
5. Check environment variables

## Security Considerations

1. **Credentials**: Store credentials securely (consider using encrypted credentials)
2. **Permissions**: Use least-privilege accounts for deployment
3. **Network**: Ensure secure network communication
4. **Log Files**: Secure log files containing sensitive information

## Best Practices

1. **Testing**: Test deployment in a non-production environment first
2. **Backup**: Backup existing packages before force deployment
3. **Versioning**: Use version control for package management
4. **Documentation**: Document environment-specific configurations
5. **Monitoring**: Monitor deployment logs for issues

## Support

For issues or questions:
1. Check the log file for detailed error messages
2. Verify all prerequisites are met
3. Test with minimal parameters first
4. Review SQL Server error logs if applicable
