# =============================================
# SSIS Scalable ETL - Deployment Script
# =============================================
# This PowerShell script automates the deployment and monitoring of the SSIS ETL solution

param(
    [Parameter(Mandatory=$true)]
    [string]$ServerName,
    
    [Parameter(Mandatory=$true)]
    [string]$DatabaseName,
    
    [Parameter(Mandatory=$false)]
    [string]$PackagePath = "C:\SSIS\Packages\SSIS_Scalable_ETL.dtsx",
    
    [Parameter(Mandatory=$false)]
    [int]$RetentionDays = 30,
    
    [Parameter(Mandatory=$false)]
    [switch]$DeployOnly,
    
    [Parameter(Mandatory=$false)]
    [switch]$MonitorOnly
)

# Import SQL Server module
Import-Module SqlServer -ErrorAction SilentlyContinue

# =============================================
# Functions
# =============================================

function Write-ColorOutput {
    param(
        [string]$Message,
        [string]$Color = "White"
    )
    Write-Host $Message -ForegroundColor $Color
}

function Test-SqlConnection {
    param([string]$Server, [string]$Database)
    
    try {
        $connectionString = "Server=$Server;Database=$Database;Integrated Security=true;Connection Timeout=30;"
        $connection = New-Object System.Data.SqlClient.SqlConnection($connectionString)
        $connection.Open()
        $connection.Close()
        return $true
    }
    catch {
        Write-ColorOutput "Failed to connect to SQL Server: $($_.Exception.Message)" "Red"
        return $false
    }
}

function Invoke-SqlScript {
    param(
        [string]$Server,
        [string]$Database,
        [string]$ScriptPath
    )
    
    try {
        Write-ColorOutput "Executing SQL script: $ScriptPath" "Yellow"
        $script = Get-Content $ScriptPath -Raw
        $script = $script.Replace("[YourStagingDatabase]", $Database)
        
        Invoke-Sqlcmd -ServerInstance $Server -Database $Database -Query $script -QueryTimeout 300
        Write-ColorOutput "SQL script executed successfully" "Green"
        return $true
    }
    catch {
        Write-ColorOutput "Failed to execute SQL script: $($_.Exception.Message)" "Red"
        return $false
    }
}

function Get-ProcessingStatistics {
    param([string]$Server, [string]$Database, [int]$HoursBack = 24)
    
    try {
        $query = @"
        EXEC sp_GetStagingStatistics $HoursBack
"@
        $results = Invoke-Sqlcmd -ServerInstance $Server -Database $Database -Query $query
        
        Write-ColorOutput "`n=== Processing Statistics (Last $HoursBack hours) ===" "Cyan"
        
        foreach ($row in $results) {
            if ($row.ReportType -eq "Processing Statistics") {
                Write-ColorOutput "Total Rows: $($row.TotalRows)" "White"
                Write-ColorOutput "Success Rows: $($row.SuccessRows)" "Green"
                Write-ColorOutput "Error Rows: $($row.ErrorRows)" "Red"
                Write-ColorOutput "Error Percentage: $($row.ErrorPercentage)%" "Yellow"
                Write-ColorOutput "Rows/Second: $($row.RowsPerSecond)" "Cyan"
                Write-ColorOutput "Processing Duration: $($row.ProcessingDurationMinutes) minutes" "White"
            }
        }
        
        return $true
    }
    catch {
        Write-ColorOutput "Failed to get processing statistics: $($_.Exception.Message)" "Red"
        return $false
    }
}

function Start-ContinuousMonitoring {
    param([string]$Server, [string]$Database, [int]$IntervalSeconds = 30)
    
    Write-ColorOutput "Starting continuous monitoring (Press Ctrl+C to stop)..." "Yellow"
    
    try {
        while ($true) {
            Clear-Host
            Write-ColorOutput "=== SSIS ETL Monitoring Dashboard ===" "Cyan"
            Write-ColorOutput "Server: $Server" "White"
            Write-ColorOutput "Database: $Database" "White"
            Write-ColorOutput "Last Updated: $(Get-Date -Format 'yyyy-MM-dd HH:mm:ss')" "White"
            Write-ColorOutput "Refresh Interval: $IntervalSeconds seconds" "White"
            Write-ColorOutput ""
            
            # Get real-time statistics
            $query = "SELECT * FROM vw_StagingMonitoring"
            $stats = Invoke-Sqlcmd -ServerInstance $Server -Database $Database -Query $query
            
            if ($stats) {
                Write-ColorOutput "Total Rows: $($stats.TotalRows)" "White"
                Write-ColorOutput "Success Rows: $($stats.SuccessRows)" "Green"
                Write-ColorOutput "Error Rows: $($stats.ErrorRows)" "Red"
                Write-ColorOutput "Error Percentage: $($stats.ErrorPercentage)%" "Yellow"
                Write-ColorOutput "Total Batches: $($stats.TotalBatches)" "Cyan"
                Write-ColorOutput "Processing Duration: $($stats.ProcessingDurationMinutes) minutes" "White"
                
                # Calculate throughput
                if ($stats.ProcessingDurationMinutes -gt 0) {
                    $throughput = [math]::Round($stats.TotalRows / ($stats.ProcessingDurationMinutes * 60), 0)
                    Write-ColorOutput "Average Throughput: $throughput rows/second" "Cyan"
                }
            }
            
            Write-ColorOutput "`nPress Ctrl+C to stop monitoring..." "Gray"
            Start-Sleep -Seconds $IntervalSeconds
        }
    }
    catch {
        Write-ColorOutput "Monitoring stopped: $($_.Exception.Message)" "Red"
    }
}

function Test-SSISPackage {
    param([string]$PackagePath)
    
    if (-not (Test-Path $PackagePath)) {
        Write-ColorOutput "SSIS package not found: $PackagePath" "Red"
        return $false
    }
    
    Write-ColorOutput "SSIS package found: $PackagePath" "Green"
    return $true
}

# =============================================
# Main Script
# =============================================

Write-ColorOutput "=============================================" "Cyan"
Write-ColorOutput "SSIS Scalable ETL - Deployment Script" "Cyan"
Write-ColorOutput "=============================================" "Cyan"
Write-ColorOutput ""

# Validate parameters
Write-ColorOutput "Validating parameters..." "Yellow"
Write-ColorOutput "Server: $ServerName" "White"
Write-ColorOutput "Database: $DatabaseName" "White"
Write-ColorOutput "Package Path: $PackagePath" "White"
Write-ColorOutput "Retention Days: $RetentionDays" "White"
Write-ColorOutput ""

# Test SQL connection
Write-ColorOutput "Testing SQL Server connection..." "Yellow"
if (-not (Test-SqlConnection -Server $ServerName -Database $DatabaseName)) {
    Write-ColorOutput "Cannot proceed without database connection" "Red"
    exit 1
}
Write-ColorOutput "SQL Server connection successful" "Green"

# Deploy database objects
if (-not $MonitorOnly) {
    Write-ColorOutput "`nDeploying database objects..." "Yellow"
    
    $scriptPath = Join-Path $PSScriptRoot "setup_database.sql"
    if (Test-Path $scriptPath) {
        if (Invoke-SqlScript -Server $ServerName -Database $DatabaseName -ScriptPath $scriptPath) {
            Write-ColorOutput "Database deployment completed successfully" "Green"
        } else {
            Write-ColorOutput "Database deployment failed" "Red"
            exit 1
        }
    } else {
        Write-ColorOutput "Database setup script not found: $scriptPath" "Red"
        exit 1
    }
    
    # Test SSIS package
    if (-not $DeployOnly) {
        Write-ColorOutput "`nTesting SSIS package..." "Yellow"
        if (-not (Test-SSISPackage -PackagePath $PackagePath)) {
            Write-ColorOutput "SSIS package validation failed" "Red"
            exit 1
        }
    }
}

# Show initial statistics
if (-not $DeployOnly) {
    Write-ColorOutput "`nGetting initial processing statistics..." "Yellow"
    Get-ProcessingStatistics -Server $ServerName -Database $DatabaseName -HoursBack 24
}

# Start monitoring if requested
if (-not $DeployOnly -and -not $MonitorOnly) {
    Write-ColorOutput "`nWould you like to start continuous monitoring? (Y/N)" "Yellow"
    $response = Read-Host
    if ($response -eq "Y" -or $response -eq "y") {
        Start-ContinuousMonitoring -Server $ServerName -Database $DatabaseName
    }
} elseif ($MonitorOnly) {
    Start-ContinuousMonitoring -Server $ServerName -Database $DatabaseName
}

# Show final summary
Write-ColorOutput "`n=============================================" "Cyan"
Write-ColorOutput "Deployment Summary" "Cyan"
Write-ColorOutput "=============================================" "Cyan"
Write-ColorOutput "Server: $ServerName" "White"
Write-ColorOutput "Database: $DatabaseName" "White"
Write-ColorOutput "Deployment Status: Success" "Green"
Write-ColorOutput ""
Write-ColorOutput "Next Steps:" "Yellow"
Write-ColorOutput "1. Configure your SSIS package with the provided C# code" "White"
Write-ColorOutput "2. Update connection strings in SSIS_Configuration.xml" "White"
Write-ColorOutput "3. Test with a small dataset first" "White"
Write-ColorOutput "4. Monitor performance and adjust batch sizes as needed" "White"
Write-ColorOutput ""
Write-ColorOutput "Useful Commands:" "Yellow"
Write-ColorOutput "- .\Deploy_SSIS_ETL.ps1 -ServerName YourServer -DatabaseName YourDB -MonitorOnly" "White"
Write-ColorOutput "- .\Deploy_SSIS_ETL.ps1 -ServerName YourServer -DatabaseName YourDB -DeployOnly" "White"
Write-ColorOutput "=============================================" "Cyan"
