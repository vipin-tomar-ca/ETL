# =============================================
# ETL Package Deployment and Execution Script
# =============================================

param(
    [string]$ServerInstance = "localhost",
    [string]$CatalogName = "SSISDB",
    [string]$FolderName = "ETL_Projects",
    [string]$ProjectName = "ETL_Scalable",
    [string]$PackageName = "Extract.dtsx",
    [string]$EnvironmentName = "Production",
    [switch]$DeployOnly,
    [switch]$ExecuteOnly,
    [switch]$CreateEnvironment
)

# Import SQL Server module
Import-Module SqlServer -ErrorAction SilentlyContinue

# Function to write colored output
function Write-ColorOutput {
    param(
        [string]$Message,
        [string]$Color = "White"
    )
    Write-Host $Message -ForegroundColor $Color
}

# Function to check if SQL Server instance is accessible
function Test-SqlConnection {
    param([string]$ServerInstance)
    
    try {
        $connection = New-Object System.Data.SqlClient.SqlConnection
        $connection.ConnectionString = "Server=$ServerInstance;Integrated Security=true;"
        $connection.Open()
        $connection.Close()
        return $true
    }
    catch {
        Write-ColorOutput "Error connecting to SQL Server instance: $ServerInstance" "Red"
        Write-ColorOutput $_.Exception.Message "Red"
        return $false
    }
}

# Function to deploy SSIS package
function Deploy-SSISPackage {
    param(
        [string]$ServerInstance,
        [string]$CatalogName,
        [string]$FolderName,
        [string]$ProjectName,
        [string]$PackageName
    )
    
    Write-ColorOutput "Deploying SSIS package..." "Yellow"
    
    try {
        # Create catalog if it doesn't exist
        $catalogExists = Get-ChildItem SQLSERVER:\SSIS\$ServerInstance\Catalogs\$CatalogName -ErrorAction SilentlyContinue
        if (-not $catalogExists) {
            Write-ColorOutput "Creating SSIS catalog: $CatalogName" "Yellow"
            New-Item SQLSERVER:\SSIS\$ServerInstance\Catalogs\$CatalogName -Force | Out-Null
        }
        
        # Create folder if it doesn't exist
        $folderExists = Get-ChildItem SQLSERVER:\SSIS\$ServerInstance\Catalogs\$CatalogName\Folders\$FolderName -ErrorAction SilentlyContinue
        if (-not $folderExists) {
            Write-ColorOutput "Creating folder: $FolderName" "Yellow"
            New-Item SQLSERVER:\SSIS\$ServerInstance\Catalogs\$CatalogName\Folders\$FolderName -Force | Out-Null
        }
        
        # Deploy project
        $projectPath = Join-Path $PSScriptRoot "ETL_Scalable.ispac"
        if (Test-Path $projectPath) {
            Write-ColorOutput "Deploying project from: $projectPath" "Yellow"
            Deploy-ISProject -ServerInstance $ServerInstance -Catalog $CatalogName -Folder $FolderName -Project $ProjectName -ProjectFilePath $projectPath -Force
            Write-ColorOutput "Project deployed successfully!" "Green"
        }
        else {
            Write-ColorOutput "Project file not found: $projectPath" "Red"
            Write-ColorOutput "Please build the SSIS project first." "Yellow"
            return $false
        }
        
        return $true
    }
    catch {
        Write-ColorOutput "Error deploying SSIS package:" "Red"
        Write-ColorOutput $_.Exception.Message "Red"
        return $false
    }
}

# Function to create SSIS environment
function New-SSISEnvironment {
    param(
        [string]$ServerInstance,
        [string]$CatalogName,
        [string]$FolderName,
        [string]$EnvironmentName
    )
    
    Write-ColorOutput "Creating SSIS environment: $EnvironmentName" "Yellow"
    
    try {
        $environmentPath = "SQLSERVER:\SSIS\$ServerInstance\Catalogs\$CatalogName\Folders\$FolderName\Environments\$EnvironmentName"
        $environmentExists = Get-ChildItem $environmentPath -ErrorAction SilentlyContinue
        
        if (-not $environmentExists) {
            New-Item $environmentPath -Force | Out-Null
            
            # Add environment variables
            $variables = @{
                "BatchSize" = "100000"
                "MaxParallelTasks" = "4"
                "LogConnectionString" = "Data Source=$ServerInstance;Initial Catalog=ETL_Metadata;Integrated Security=SSPI;"
                "ParquetOutputPath" = "C:\ETL\Parquet"
                "StagingConnectionString" = "Data Source=$ServerInstance;Initial Catalog=ETL_Staging;Integrated Security=SSPI;"
            }
            
            foreach ($var in $variables.GetEnumerator()) {
                New-Item "$environmentPath\Variables\$($var.Key)" -Value $var.Value -Force | Out-Null
            }
            
            Write-ColorOutput "Environment created with variables!" "Green"
        }
        else {
            Write-ColorOutput "Environment already exists: $EnvironmentName" "Yellow"
        }
        
        return $true
    }
    catch {
        Write-ColorOutput "Error creating SSIS environment:" "Red"
        Write-ColorOutput $_.Exception.Message "Red"
        return $false
    }
}

# Function to execute SSIS package
function Start-SSISPackage {
    param(
        [string]$ServerInstance,
        [string]$CatalogName,
        [string]$FolderName,
        [string]$ProjectName,
        [string]$PackageName,
        [string]$EnvironmentName
    )
    
    Write-ColorOutput "Executing SSIS package..." "Yellow"
    
    try {
        $executionPath = "SQLSERVER:\SSIS\$ServerInstance\Catalogs\$CatalogName\Folders\$FolderName\Projects\$ProjectName\Packages\$PackageName"
        $packageExists = Get-ChildItem $executionPath -ErrorAction SilentlyContinue
        
        if (-not $packageExists) {
            Write-ColorOutput "Package not found: $PackageName" "Red"
            return $false
        }
        
        # Create execution instance
        $execution = New-Object Microsoft.SqlServer.Management.IntegrationServices.PackageExecution
        $execution.PackagePath = "/$FolderName/$ProjectName/$PackageName"
        $execution.EnvironmentName = $EnvironmentName
        $execution.ExecuteOutOfProcess = $true
        
        # Start execution
        $executionId = Start-ISPackage -ServerInstance $ServerInstance -Catalog $CatalogName -Folder $FolderName -Project $ProjectName -Package $PackageName -Environment $EnvironmentName
        
        Write-ColorOutput "Package execution started with ID: $executionId" "Green"
        
        # Monitor execution
        $maxWaitTime = 3600 # 1 hour
        $waitInterval = 10 # 10 seconds
        $elapsedTime = 0
        
        while ($elapsedTime -lt $maxWaitTime) {
            $executionStatus = Get-ISExecution -ServerInstance $ServerInstance -ExecutionId $executionId
            
            switch ($executionStatus.Status) {
                "Succeeded" {
                    Write-ColorOutput "Package execution completed successfully!" "Green"
                    return $true
                }
                "Failed" {
                    Write-ColorOutput "Package execution failed!" "Red"
                    Write-ColorOutput "Error: $($executionStatus.ErrorMessage)" "Red"
                    return $false
                }
                "Canceled" {
                    Write-ColorOutput "Package execution was canceled!" "Yellow"
                    return $false
                }
                default {
                    Write-ColorOutput "Execution status: $($executionStatus.Status)" "Yellow"
                    Start-Sleep -Seconds $waitInterval
                    $elapsedTime += $waitInterval
                }
            }
        }
        
        Write-ColorOutput "Execution timeout reached!" "Red"
        return $false
    }
    catch {
        Write-ColorOutput "Error executing SSIS package:" "Red"
        Write-ColorOutput $_.Exception.Message "Red"
        return $false
    }
}

# Function to setup database
function Setup-Database {
    param([string]$ServerInstance)
    
    Write-ColorOutput "Setting up ETL databases..." "Yellow"
    
    try {
        $sqlScript = Get-Content (Join-Path $PSScriptRoot "database_setup.sql") -Raw
        Invoke-Sqlcmd -ServerInstance $ServerInstance -Query $sqlScript
        Write-ColorOutput "Database setup completed successfully!" "Green"
        return $true
    }
    catch {
        Write-ColorOutput "Error setting up database:" "Red"
        Write-ColorOutput $_.Exception.Message "Red"
        return $false
    }
}

# Main execution
Write-ColorOutput "=== ETL Package Deployment and Execution Script ===" "Cyan"
Write-ColorOutput "Server Instance: $ServerInstance" "White"
Write-ColorOutput "Catalog: $CatalogName" "White"
Write-ColorOutput "Folder: $FolderName" "White"
Write-ColorOutput "Project: $ProjectName" "White"
Write-ColorOutput "Package: $PackageName" "White"
Write-ColorOutput "Environment: $EnvironmentName" "White"
Write-ColorOutput ""

# Test SQL Server connection
if (-not (Test-SqlConnection -ServerInstance $ServerInstance)) {
    exit 1
}

# Setup database if not in execute-only mode
if (-not $ExecuteOnly) {
    if (-not (Setup-Database -ServerInstance $ServerInstance)) {
        exit 1
    }
}

# Create environment if requested
if ($CreateEnvironment) {
    if (-not (New-SSISEnvironment -ServerInstance $ServerInstance -CatalogName $CatalogName -FolderName $FolderName -EnvironmentName $EnvironmentName)) {
        exit 1
    }
}

# Deploy package if not in execute-only mode
if (-not $ExecuteOnly) {
    if (-not (Deploy-SSISPackage -ServerInstance $ServerInstance -CatalogName $CatalogName -FolderName $FolderName -ProjectName $ProjectName -PackageName $PackageName)) {
        exit 1
    }
}

# Execute package if not in deploy-only mode
if (-not $DeployOnly) {
    if (-not (Start-SSISPackage -ServerInstance $ServerInstance -CatalogName $CatalogName -FolderName $FolderName -ProjectName $ProjectName -PackageName $PackageName -EnvironmentName $EnvironmentName)) {
        exit 1
    }
}

Write-ColorOutput "Script completed successfully!" "Green"
