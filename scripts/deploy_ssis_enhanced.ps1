# Enhanced SSIS Package Deployment Script
# This script deploys SSIS packages to the SSIS Catalog with configuration file support
# Author: ETL Framework Team
# Date: $(Get-Date -Format "yyyy-MM-dd")

param(
    [Parameter(Mandatory=$false)]
    [string]$ConfigFile = ".\deploy_ssis_config.json",
    
    [Parameter(Mandatory=$false)]
    [string]$SqlServerInstance,
    
    [Parameter(Mandatory=$false)]
    [string]$DatabaseName,
    
    [Parameter(Mandatory=$false)]
    [string]$Username,
    
    [Parameter(Mandatory=$false)]
    [string]$Password,
    
    [Parameter(Mandatory=$false)]
    [string]$FolderName,
    
    [Parameter(Mandatory=$false)]
    [string]$ProjectName,
    
    [Parameter(Mandatory=$false)]
    [string]$PackagesPath,
    
    [Parameter(Mandatory=$false)]
    [string]$LogFilePath,
    
    [Parameter(Mandatory=$false)]
    [switch]$UseWindowsAuthentication,
    
    [Parameter(Mandatory=$false)]
    [switch]$Force = $false,
    
    [Parameter(Mandatory=$false)]
    [switch]$ValidateOnly = $false,
    
    [Parameter(Mandatory=$false)]
    [switch]$BackupBeforeDeploy = $false
)

# Global configuration variable
$GlobalConfig = $null

# Function to write log messages with timestamp
function Write-Log {
    param(
        [string]$Message,
        [string]$Level = "INFO"
    )
    
    $timestamp = Get-Date -Format "yyyy-MM-dd HH:mm:ss"
    $logMessage = "[$timestamp] [$Level] $Message"
    
    # Write to console with color coding
    switch ($Level) {
        "SUCCESS" { Write-Host $logMessage -ForegroundColor Green }
        "ERROR" { Write-Host $logMessage -ForegroundColor Red }
        "WARNING" { Write-Host $logMessage -ForegroundColor Yellow }
        default { Write-Host $logMessage -ForegroundColor White }
    }
    
    # Write to log file
    Add-Content -Path $LogFilePath -Value $logMessage
}

# Function to load configuration from JSON file
function Load-Configuration {
    param([string]$ConfigPath)
    
    try {
        if (Test-Path $ConfigPath) {
            $config = Get-Content -Path $ConfigPath -Raw | ConvertFrom-Json
            Write-Log "Configuration loaded from: $ConfigPath" "INFO"
            return $config
        } else {
            Write-Log "Configuration file not found: $ConfigPath" "WARNING"
            return $null
        }
    }
    catch {
        Write-Log "Failed to load configuration: $($_.Exception.Message)" "ERROR"
        return $null
    }
}

# Function to merge command line parameters with configuration
function Merge-Parameters {
    param($Config)
    
    $params = @{}
    
    # Load from config if available
    if ($Config) {
        $params.SqlServerInstance = $Config.deployment.sqlServer.instance
        $params.DatabaseName = $Config.deployment.sqlServer.database
        $params.UseWindowsAuthentication = $Config.deployment.sqlServer.useWindowsAuthentication
        $params.Username = $Config.deployment.sqlServer.username
        $params.Password = $Config.deployment.sqlServer.password
        $params.FolderName = $Config.deployment.ssis.folderName
        $params.ProjectName = $Config.deployment.ssis.projectName
        $params.PackagesPath = $Config.deployment.ssis.packagesPath
        $params.LogFilePath = $Config.deployment.logging.logFilePath
        $params.Force = $Config.deployment.ssis.forceDeployment
        $params.BackupBeforeDeploy = $Config.settings.backupBeforeDeploy
    }
    
    # Override with command line parameters if provided
    if ($SqlServerInstance) { $params.SqlServerInstance = $SqlServerInstance }
    if ($DatabaseName) { $params.DatabaseName = $DatabaseName }
    if ($Username) { $params.Username = $Username }
    if ($Password) { $params.Password = $Password }
    if ($FolderName) { $params.FolderName = $FolderName }
    if ($ProjectName) { $params.ProjectName = $ProjectName }
    if ($PackagesPath) { $params.PackagesPath = $PackagesPath }
    if ($LogFilePath) { $params.LogFilePath = $LogFilePath }
    if ($UseWindowsAuthentication) { $params.UseWindowsAuthentication = $UseWindowsAuthentication }
    if ($Force) { $params.Force = $Force }
    if ($BackupBeforeDeploy) { $params.BackupBeforeDeploy = $BackupBeforeDeploy }
    
    return $params
}

# Function to validate configuration
function Test-Configuration {
    param($Params)
    
    $errors = @()
    
    # Required parameters
    if (-not $Params.SqlServerInstance) { $errors += "SqlServerInstance is required" }
    if (-not $Params.DatabaseName) { $errors += "DatabaseName is required" }
    if (-not $Params.FolderName) { $errors += "FolderName is required" }
    if (-not $Params.ProjectName) { $errors += "ProjectName is required" }
    if (-not $Params.PackagesPath) { $errors += "PackagesPath is required" }
    
    # Authentication validation
    if (-not $Params.UseWindowsAuthentication) {
        if (-not $Params.Username) { $errors += "Username is required when not using Windows Authentication" }
        if (-not $Params.Password) { $errors += "Password is required when not using Windows Authentication" }
    }
    
    # Path validation
    if (-not (Test-Path $Params.PackagesPath)) {
        $errors += "PackagesPath does not exist: $($Params.PackagesPath)"
    }
    
    if ($errors.Count -gt 0) {
        foreach ($error in $errors) {
            Write-Log "Configuration Error: $error" "ERROR"
        }
        return $false
    }
    
    Write-Log "Configuration validation passed" "SUCCESS"
    return $true
}

# Function to backup existing packages
function Backup-SsisPackages {
    param(
        [string]$FolderName,
        [string]$ProjectName,
        [string]$BackupPath,
        [string]$ServerInstance,
        [string]$Database,
        [string]$Username,
        [string]$Password,
        [bool]$UseWindowsAuth
    )
    
    try {
        $timestamp = Get-Date -Format "yyyyMMdd_HHmmss"
        $backupDir = Join-Path $BackupPath "SSIS_Backup_$timestamp"
        
        if (-not (Test-Path $backupDir)) {
            New-Item -ItemType Directory -Path $backupDir -Force | Out-Null
        }
        
        Write-Log "Creating backup in: $backupDir" "INFO"
        
        # Export existing packages using dtutil
        $exportCmd = "dtutil.exe /Source SSISDB;`$FolderName`/$ProjectName /Destination `"$backupDir`" /Copy"
        
        $result = Invoke-Expression $exportCmd 2>&1
        
        if ($LASTEXITCODE -eq 0) {
            Write-Log "Backup completed successfully" "SUCCESS"
            return $true
        } else {
            Write-Log "Backup failed: $result" "WARNING"
            return $false
        }
    }
    catch {
        Write-Log "Backup exception: $($_.Exception.Message)" "ERROR"
        return $false
    }
}

# Function to validate packages before deployment
function Test-SsisPackages {
    param([string]$PackagesPath)
    
    try {
        $packageFiles = Get-ChildItem -Path $PackagesPath -Filter "*.dtsx" -Recurse
        $validationResults = @()
        
        Write-Log "Validating $($packageFiles.Count) package(s)..." "INFO"
        
        foreach ($package in $packageFiles) {
            Write-Log "Validating package: $($package.Name)" "INFO"
            
            # Basic validation - check if file is readable and has valid XML structure
            try {
                $xml = [xml](Get-Content $package.FullName)
                if ($xml.DTSExecutable) {
                    $validationResults += @{
                        Package = $package.Name
                        Status = "Valid"
                        Message = "Package structure is valid"
                    }
                } else {
                    $validationResults += @{
                        Package = $package.Name
                        Status = "Invalid"
                        Message = "Invalid package structure"
                    }
                }
            }
            catch {
                $validationResults += @{
                    Package = $package.Name
                    Status = "Error"
                    Message = $_.Exception.Message
                }
            }
        }
        
        # Report validation results
        $validCount = ($validationResults | Where-Object { $_.Status -eq "Valid" }).Count
        $invalidCount = ($validationResults | Where-Object { $_.Status -ne "Valid" }).Count
        
        Write-Log "Validation complete: $validCount valid, $invalidCount invalid" "INFO"
        
        if ($invalidCount -gt 0) {
            foreach ($result in $validationResults | Where-Object { $_.Status -ne "Valid" }) {
                Write-Log "Package $($result.Package): $($result.Message)" "WARNING"
            }
        }
        
        return ($invalidCount -eq 0)
    }
    catch {
        Write-Log "Package validation failed: $($_.Exception.Message)" "ERROR"
        return $false
    }
}

# Function to test SQL Server connection
function Test-SqlConnection {
    param(
        [string]$ServerInstance,
        [string]$Database,
        [string]$Username,
        [string]$Password,
        [bool]$UseWindowsAuth
    )
    
    try {
        $connectionString = if ($UseWindowsAuth) {
            "Server=$ServerInstance;Database=$Database;Integrated Security=true;"
        } else {
            "Server=$ServerInstance;Database=$Database;User ID=$Username;Password=$Password;"
        }
        
        $connection = New-Object System.Data.SqlClient.SqlConnection($connectionString)
        $connection.Open()
        $connection.Close()
        
        Write-Log "Successfully connected to SQL Server: $ServerInstance" "SUCCESS"
        return $true
    }
    catch {
        Write-Log "Failed to connect to SQL Server: $($_.Exception.Message)" "ERROR"
        return $false
    }
}

# Function to create SSIS Catalog folder if it doesn't exist
function New-SsisFolder {
    param(
        [string]$FolderName,
        [string]$ServerInstance,
        [string]$Database,
        [string]$Username,
        [string]$Password,
        [bool]$UseWindowsAuth
    )
    
    try {
        $connectionString = if ($UseWindowsAuth) {
            "Server=$ServerInstance;Database=$Database;Integrated Security=true;"
        } else {
            "Server=$ServerInstance;Database=$Database;User ID=$Username;Password=$Password;"
        }
        
        $query = @"
        IF NOT EXISTS (SELECT 1 FROM [SSISDB].[catalog].[folders] WHERE [name] = '$FolderName')
        BEGIN
            EXEC [SSISDB].[catalog].[create_folder] @folder_name = '$FolderName'
            SELECT 'Folder created successfully' as Result
        END
        ELSE
        BEGIN
            SELECT 'Folder already exists' as Result
        END
"@
        
        $connection = New-Object System.Data.SqlClient.SqlConnection($connectionString)
        $command = New-Object System.Data.SqlClient.SqlCommand($query, $connection)
        $connection.Open()
        $result = $command.ExecuteScalar()
        $connection.Close()
        
        Write-Log "SSIS Folder operation: $result" "INFO"
        return $true
    }
    catch {
        Write-Log "Failed to create SSIS folder: $($_.Exception.Message)" "ERROR"
        return $false
    }
}

# Function to configure environment variables
function Set-SsisEnvironmentVariables {
    param(
        [string]$FolderName,
        [string]$ProjectName,
        [string]$ServerInstance,
        [string]$Database,
        [string]$Username,
        [string]$Password,
        [bool]$UseWindowsAuth,
        $EnvironmentVariables
    )
    
    try {
        $connectionString = if ($UseWindowsAuth) {
            "Server=$ServerInstance;Database=$Database;Integrated Security=true;"
        } else {
            "Server=$ServerInstance;Database=$Database;User ID=$Username;Password=$Password;"
        }
        
        $connection = New-Object System.Data.SqlClient.SqlConnection($connectionString)
        $connection.Open()
        
        foreach ($var in $EnvironmentVariables.PSObject.Properties) {
            $query = @"
            IF NOT EXISTS (SELECT 1 FROM [SSISDB].[catalog].[environment_variables] 
                          WHERE [name] = '$($var.Name)' AND [folder_name] = '$FolderName')
            BEGIN
                EXEC [SSISDB].[catalog].[create_environment_variable]
                    @folder_name = '$FolderName',
                    @environment_name = 'Production',
                    @variable_name = '$($var.Name)',
                    @data_type = 'String',
                    @value = '$($var.Value)'
            END
            ELSE
            BEGIN
                EXEC [SSISDB].[catalog].[set_environment_variable_value]
                    @folder_name = '$FolderName',
                    @environment_name = 'Production',
                    @variable_name = '$($var.Name)',
                    @value = '$($var.Value)'
            END
"@
            
            $command = New-Object System.Data.SqlClient.SqlCommand($query, $connection)
            $command.ExecuteNonQuery() | Out-Null
            
            Write-Log "Configured environment variable: $($var.Name)" "INFO"
        }
        
        $connection.Close()
        Write-Log "Successfully configured environment variables" "SUCCESS"
        return $true
    }
    catch {
        Write-Log "Failed to configure environment variables: $($_.Exception.Message)" "ERROR"
        return $false
    }
}

# Function to deploy individual packages
function Deploy-SsisPackages {
    param(
        [string]$PackagesPath,
        [string]$FolderName,
        [string]$ProjectName,
        [string]$ServerInstance,
        [string]$Database,
        [string]$Username,
        [string]$Password,
        [bool]$UseWindowsAuth,
        [bool]$Force
    )
    
    try {
        # Check if packages directory exists
        if (-not (Test-Path $PackagesPath)) {
            Write-Log "Packages directory not found: $PackagesPath" "ERROR"
            return $false
        }
        
        # Get all .dtsx files
        $packageFiles = Get-ChildItem -Path $PackagesPath -Filter "*.dtsx" -Recurse
        
        if ($packageFiles.Count -eq 0) {
            Write-Log "No .dtsx files found in directory: $PackagesPath" "WARNING"
            return $false
        }
        
        Write-Log "Found $($packageFiles.Count) package(s) to deploy" "INFO"
        
        $successCount = 0
        $failureCount = 0
        
        foreach ($package in $packageFiles) {
            Write-Log "Deploying package: $($package.Name)" "INFO"
            
            # Deploy individual package
            $deployCmd = "dtutil.exe /File `"$($package.FullName)`" /Deploy SSISDB;`$FolderName`/$ProjectName"
            
            if ($Force) {
                $deployCmd += " /Overwrite"
            }
            
            $result = Invoke-Expression $deployCmd 2>&1
            
            if ($LASTEXITCODE -eq 0) {
                Write-Log "Successfully deployed package: $($package.Name)" "SUCCESS"
                $successCount++
            } else {
                Write-Log "Failed to deploy package $($package.Name): $result" "ERROR"
                $failureCount++
            }
        }
        
        Write-Log "Package deployment summary: $successCount successful, $failureCount failed" "INFO"
        return ($failureCount -eq 0)
    }
    catch {
        Write-Log "Exception during package deployment: $($_.Exception.Message)" "ERROR"
        return $false
    }
}

# Main execution block
try {
    Write-Log "Starting Enhanced SSIS deployment process..." "INFO"
    
    # Load configuration
    $GlobalConfig = Load-Configuration -ConfigPath $ConfigFile
    
    # Merge parameters
    $params = Merge-Parameters -Config $GlobalConfig
    
    # Set log file path
    $LogFilePath = $params.LogFilePath
    
    # Validate configuration
    if (-not (Test-Configuration -Params $params)) {
        throw "Configuration validation failed"
    }
    
    Write-Log "Parameters: Server=$($params.SqlServerInstance), Database=$($params.DatabaseName), Folder=$($params.FolderName)" "INFO"
    
    # Test SQL Server connection
    if (-not (Test-SqlConnection -ServerInstance $params.SqlServerInstance -Database $params.DatabaseName -Username $params.Username -Password $params.Password -UseWindowsAuth $params.UseWindowsAuthentication)) {
        throw "Failed to connect to SQL Server"
    }
    
    # Validate packages if requested
    if ($GlobalConfig.settings.validatePackages -or $ValidateOnly) {
        if (-not (Test-SsisPackages -PackagesPath $params.PackagesPath)) {
            if ($ValidateOnly) {
                Write-Log "Package validation failed. Exiting due to ValidateOnly flag." "ERROR"
                exit 1
            } else {
                Write-Log "Package validation failed, but continuing with deployment" "WARNING"
            }
        }
    }
    
    if ($ValidateOnly) {
        Write-Log "Validation only mode - exiting" "INFO"
        exit 0
    }
    
    # Create backup if requested
    if ($params.BackupBeforeDeploy) {
        $backupPath = ".\SSIS_Backups"
        if (-not (Test-Path $backupPath)) {
            New-Item -ItemType Directory -Path $backupPath -Force | Out-Null
        }
        
        Backup-SsisPackages -FolderName $params.FolderName -ProjectName $params.ProjectName -BackupPath $backupPath -ServerInstance $params.SqlServerInstance -Database $params.DatabaseName -Username $params.Username -Password $params.Password -UseWindowsAuth $params.UseWindowsAuthentication
    }
    
    # Create SSIS folder if it doesn't exist
    if (-not (New-SsisFolder -FolderName $params.FolderName -ServerInstance $params.SqlServerInstance -Database $params.DatabaseName -Username $params.Username -Password $params.Password -UseWindowsAuth $params.UseWindowsAuthentication)) {
        throw "Failed to create SSIS folder"
    }
    
    # Deploy packages
    if (-not (Deploy-SsisPackages -PackagesPath $params.PackagesPath -FolderName $params.FolderName -ProjectName $params.ProjectName -ServerInstance $params.SqlServerInstance -Database $params.DatabaseName -Username $params.Username -Password $params.Password -UseWindowsAuth $params.UseWindowsAuthentication -Force $params.Force)) {
        Write-Log "Some packages failed to deploy" "WARNING"
    }
    
    # Configure environment variables
    if ($GlobalConfig.environmentVariables) {
        if (-not (Set-SsisEnvironmentVariables -FolderName $params.FolderName -ProjectName $params.ProjectName -ServerInstance $params.SqlServerInstance -Database $params.DatabaseName -Username $params.Username -Password $params.Password -UseWindowsAuth $params.UseWindowsAuthentication -EnvironmentVariables $GlobalConfig.environmentVariables)) {
            Write-Log "Failed to configure environment variables" "WARNING"
        }
    }
    
    Write-Log "Enhanced SSIS deployment process completed" "SUCCESS"
}
catch {
    Write-Log "Deployment failed with error: $($_.Exception.Message)" "ERROR"
    exit 1
}

Write-Log "Script execution completed. Check log file: $LogFilePath" "INFO"
