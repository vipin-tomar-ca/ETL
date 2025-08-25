# SSIS Package Deployment Script
# This script deploys SSIS packages to the SSIS Catalog with environment variable configuration
# Author: ETL Framework Team
# Date: $(Get-Date -Format "yyyy-MM-dd")

param(
    [Parameter(Mandatory=$true)]
    [string]$SqlServerInstance,
    
    [Parameter(Mandatory=$true)]
    [string]$DatabaseName,
    
    [Parameter(Mandatory=$false)]
    [string]$Username,
    
    [Parameter(Mandatory=$false)]
    [string]$Password,
    
    [Parameter(Mandatory=$false)]
    [string]$FolderName = "ETLFramework",
    
    [Parameter(Mandatory=$false)]
    [string]$ProjectName = "ETLProject",
    
    [Parameter(Mandatory=$false)]
    [string]$PackagesPath = ".\ETL.SSIS\Packages",
    
    [Parameter(Mandatory=$false)]
    [string]$LogFilePath = ".\deploy_ssis.log",
    
    [Parameter(Mandatory=$false)]
    [switch]$UseWindowsAuthentication = $true,
    
    [Parameter(Mandatory=$false)]
    [switch]$Force = $false
)

# Function to write log messages with timestamp
function Write-Log {
    param(
        [string]$Message,
        [string]$Level = "INFO"
    )
    
    $timestamp = Get-Date -Format "yyyy-MM-dd HH:mm:ss"
    $logMessage = "[$timestamp] [$Level] $Message"
    
    # Write to console
    Write-Host $logMessage
    
    # Write to log file
    Add-Content -Path $LogFilePath -Value $logMessage
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

# Function to deploy SSIS project
function Deploy-SsisProject {
    param(
        [string]$ProjectPath,
        [string]$ProjectName,
        [string]$FolderName,
        [string]$ServerInstance,
        [string]$Database,
        [string]$Username,
        [string]$Password,
        [bool]$UseWindowsAuth,
        [bool]$Force
    )
    
    try {
        # Check if project file exists
        if (-not (Test-Path $ProjectPath)) {
            Write-Log "Project file not found: $ProjectPath" "ERROR"
            return $false
        }
        
        # Build deployment command
        $deployCmd = "dtutil.exe /File `"$ProjectPath`" /Deploy SSISDB;`$FolderName`/$ProjectName"
        
        if ($Force) {
            $deployCmd += " /Overwrite"
        }
        
        Write-Log "Executing deployment command: $deployCmd" "INFO"
        
        # Execute deployment
        $result = Invoke-Expression $deployCmd 2>&1
        
        if ($LASTEXITCODE -eq 0) {
            Write-Log "Successfully deployed project: $ProjectName" "SUCCESS"
            return $true
        } else {
            Write-Log "Failed to deploy project: $result" "ERROR"
            return $false
        }
    }
    catch {
        Write-Log "Exception during project deployment: $($_.Exception.Message)" "ERROR"
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
        [bool]$UseWindowsAuth
    )
    
    try {
        $connectionString = if ($UseWindowsAuth) {
            "Server=$ServerInstance;Database=$Database;Integrated Security=true;"
        } else {
            "Server=$ServerInstance;Database=$Database;User ID=$Username;Password=$Password;"
        }
        
        # Define environment variables (customize as needed)
        $envVars = @{
            "SourceConnectionString" = "Server=SourceServer;Database=SourceDB;Integrated Security=true;"
            "TargetConnectionString" = "Server=TargetServer;Database=TargetDB;Integrated Security=true;"
            "FileSharePath" = "\\server\share\data"
            "LogLevel" = "INFO"
            "BatchSize" = "1000"
        }
        
        $connection = New-Object System.Data.SqlClient.SqlConnection($connectionString)
        $connection.Open()
        
        foreach ($var in $envVars.GetEnumerator()) {
            $query = @"
            IF NOT EXISTS (SELECT 1 FROM [SSISDB].[catalog].[environment_variables] 
                          WHERE [name] = '$($var.Key)' AND [folder_name] = '$FolderName')
            BEGIN
                EXEC [SSISDB].[catalog].[create_environment_variable]
                    @folder_name = '$FolderName',
                    @environment_name = 'Production',
                    @variable_name = '$($var.Key)',
                    @data_type = 'String',
                    @value = '$($var.Value)'
            END
            ELSE
            BEGIN
                EXEC [SSISDB].[catalog].[set_environment_variable_value]
                    @folder_name = '$FolderName',
                    @environment_name = 'Production',
                    @variable_name = '$($var.Key)',
                    @value = '$($var.Value)'
            END
"@
            
            $command = New-Object System.Data.SqlClient.SqlCommand($query, $connection)
            $command.ExecuteNonQuery() | Out-Null
            
            Write-Log "Configured environment variable: $($var.Key)" "INFO"
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
    Write-Log "Starting SSIS deployment process..." "INFO"
    Write-Log "Parameters: Server=$SqlServerInstance, Database=$DatabaseName, Folder=$FolderName" "INFO"
    
    # Test SQL Server connection
    if (-not (Test-SqlConnection -ServerInstance $SqlServerInstance -Database $DatabaseName -Username $Username -Password $Password -UseWindowsAuth $UseWindowsAuthentication)) {
        throw "Failed to connect to SQL Server"
    }
    
    # Create SSIS folder if it doesn't exist
    if (-not (New-SsisFolder -FolderName $FolderName -ServerInstance $SqlServerInstance -Database $DatabaseName -Username $Username -Password $Password -UseWindowsAuth $UseWindowsAuthentication)) {
        throw "Failed to create SSIS folder"
    }
    
    # Deploy packages
    if (-not (Deploy-SsisPackages -PackagesPath $PackagesPath -FolderName $FolderName -ProjectName $ProjectName -ServerInstance $SqlServerInstance -Database $DatabaseName -Username $Username -Password $Password -UseWindowsAuth $UseWindowsAuthentication -Force $Force)) {
        Write-Log "Some packages failed to deploy" "WARNING"
    }
    
    # Configure environment variables
    if (-not (Set-SsisEnvironmentVariables -FolderName $FolderName -ProjectName $ProjectName -ServerInstance $SqlServerInstance -Database $DatabaseName -Username $Username -Password $Password -UseWindowsAuth $UseWindowsAuthentication)) {
        Write-Log "Failed to configure environment variables" "WARNING"
    }
    
    Write-Log "SSIS deployment process completed" "SUCCESS"
}
catch {
    Write-Log "Deployment failed with error: $($_.Exception.Message)" "ERROR"
    exit 1
}

Write-Log "Script execution completed. Check log file: $LogFilePath" "INFO"
