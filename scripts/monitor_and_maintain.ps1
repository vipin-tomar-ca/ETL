# =============================================
# ETL Monitoring and Maintenance Script
# =============================================

param(
    [string]$ServerInstance = "localhost",
    [string]$MetadataDatabase = "ETL_Metadata",
    [string]$StagingDatabase = "ETL_Staging",
    [switch]$MonitorOnly,
    [switch]$MaintenanceOnly,
    [switch]$ArchiveLogs,
    [switch]$CleanupStaging,
    [switch]$CleanupParquet,
    [int]$LogRetentionDays = 90,
    [int]$StagingRetentionDays = 30,
    [int]$ParquetRetentionDays = 7
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

# Function to get database connection
function Get-DatabaseConnection {
    param([string]$ServerInstance, [string]$Database)
    
    try {
        $connection = New-Object System.Data.SqlClient.SqlConnection
        $connection.ConnectionString = "Server=$ServerInstance;Database=$Database;Integrated Security=true;"
        $connection.Open()
        return $connection
    }
    catch {
        Write-ColorOutput "Error connecting to database $Database on $ServerInstance" "Red"
        Write-ColorOutput $_.Exception.Message "Red"
        return $null
    }
}

# Function to monitor ETL performance
function Monitor-ETLPerformance {
    param([System.Data.SqlClient.SqlConnection]$Connection)
    
    Write-ColorOutput "=== ETL Performance Monitoring ===" "Cyan"
    
    try {
        # Recent execution summary
        $query = @"
        SELECT 
            COUNT(*) as TotalExecutions,
            SUM(CASE WHEN LogLevel = 'ERROR' THEN 1 ELSE 0 END) as ErrorCount,
            SUM(CASE WHEN LogLevel = 'INFO' AND ErrorCode IS NULL THEN 1 ELSE 0 END) as SuccessCount,
            AVG(CASE WHEN LogLevel = 'INFO' AND ErrorCode IS NULL THEN Duration ELSE NULL END) as AvgDurationSeconds,
            MAX(LogDate) as LastExecution
        FROM $MetadataDatabase.dbo.Logs 
        WHERE LogDate >= DATEADD(HOUR, -24, GETDATE())
"@
        
        $command = New-Object System.Data.SqlClient.SqlCommand($query, $Connection)
        $reader = $command.ExecuteReader()
        
        if ($reader.Read()) {
            Write-ColorOutput "Last 24 Hours Summary:" "Yellow"
            Write-ColorOutput "  Total Executions: $($reader['TotalExecutions'])" "White"
            Write-ColorOutput "  Successful: $($reader['SuccessCount'])" "Green"
            Write-ColorOutput "  Errors: $($reader['ErrorCount'])" "Red"
            Write-ColorOutput "  Average Duration: $([math]::Round($reader['AvgDurationSeconds'], 2)) seconds" "White"
            Write-ColorOutput "  Last Execution: $($reader['LastExecution'])" "White"
        }
        $reader.Close()
        
        # Top performing tables
        $query = @"
        SELECT TOP 10
            Source,
            COUNT(*) as ExecutionCount,
            AVG(RowsProcessed) as AvgRowsProcessed,
            AVG(Duration) as AvgDurationSeconds,
            CAST(AVG(RowsProcessed) AS FLOAT) / NULLIF(AVG(Duration), 0) as RowsPerSecond,
            MAX(LogDate) as LastExecution
        FROM $MetadataDatabase.dbo.Logs 
        WHERE LogLevel = 'INFO' AND ErrorCode IS NULL
        GROUP BY Source
        ORDER BY RowsPerSecond DESC
"@
        
        $command = New-Object System.Data.SqlClient.SqlCommand($query, $Connection)
        $reader = $command.ExecuteReader()
        
        Write-ColorOutput "`nTop Performing Tables (Rows/Second):" "Yellow"
        while ($reader.Read()) {
            $rowsPerSecond = [math]::Round($reader['RowsPerSecond'], 2)
            Write-ColorOutput "  $($reader['Source']): $rowsPerSecond rows/sec" "White"
        }
        $reader.Close()
        
        # Recent errors
        $query = @"
        SELECT TOP 10
            Source,
            LogDate,
            Message,
            ErrorCode,
            ErrorDescription
        FROM $MetadataDatabase.dbo.Logs 
        WHERE LogLevel = 'ERROR'
        ORDER BY LogDate DESC
"@
        
        $command = New-Object System.Data.SqlClient.SqlCommand($query, $Connection)
        $reader = $command.ExecuteReader()
        
        if ($reader.HasRows) {
            Write-ColorOutput "`nRecent Errors:" "Red"
            while ($reader.Read()) {
                Write-ColorOutput "  $($reader['Source']) - $($reader['LogDate']): $($reader['Message'])" "Red"
            }
        }
        $reader.Close()
        
    }
    catch {
        Write-ColorOutput "Error monitoring ETL performance:" "Red"
        Write-ColorOutput $_.Exception.Message "Red"
    }
}

# Function to monitor staging database
function Monitor-StagingDatabase {
    param([System.Data.SqlClient.SqlConnection]$Connection)
    
    Write-ColorOutput "`n=== Staging Database Monitoring ===" "Cyan"
    
    try {
        # Staging table sizes
        $query = @"
        SELECT 
            t.name as TableName,
            p.rows as RowCount,
            p.data_pages * 8 as DataSizeKB,
            p.data_pages * 8 / 1024.0 as DataSizeMB,
            p.data_pages * 8 / 1024.0 / 1024.0 as DataSizeGB
        FROM sys.tables t
        INNER JOIN sys.indexes i ON t.object_id = i.object_id
        INNER JOIN sys.partitions p ON i.object_id = p.object_id AND i.index_id = p.index_id
        WHERE t.name LIKE 'stg_%' AND i.index_id <= 1
        ORDER BY p.data_pages DESC
"@
        
        $command = New-Object System.Data.SqlClient.SqlCommand($query, $Connection)
        $reader = $command.ExecuteReader()
        
        Write-ColorOutput "Staging Table Sizes:" "Yellow"
        while ($reader.Read()) {
            $sizeMB = [math]::Round($reader['DataSizeMB'], 2)
            Write-ColorOutput "  $($reader['TableName']): $($reader['RowCount']) rows, $sizeMB MB" "White"
        }
        $reader.Close()
        
        # Old staging data
        $query = @"
        SELECT 
            t.name as TableName,
            COUNT(*) as OldRowCount,
            MIN(CAST(c.value AS datetime)) as OldestData,
            MAX(CAST(c.value AS datetime)) as NewestData
        FROM sys.tables t
        INNER JOIN sys.columns c ON t.object_id = c.object_id
        WHERE t.name LIKE 'stg_%' AND c.name = 'ExtractionDate'
        GROUP BY t.name
        HAVING MIN(CAST(c.value AS datetime)) < DATEADD(DAY, -$StagingRetentionDays, GETDATE())
        ORDER BY OldestData
"@
        
        $command = New-Object System.Data.SqlClient.SqlCommand($query, $Connection)
        $reader = $command.ExecuteReader()
        
        if ($reader.HasRows) {
            Write-ColorOutput "`nStaging Data Requiring Cleanup:" "Yellow"
            while ($reader.Read()) {
                Write-ColorOutput "  $($reader['TableName']): $($reader['OldRowCount']) rows older than $StagingRetentionDays days" "White"
            }
        }
        $reader.Close()
        
    }
    catch {
        Write-ColorOutput "Error monitoring staging database:" "Red"
        Write-ColorOutput $_.Exception.Message "Red"
    }
}

# Function to archive old logs
function Archive-Logs {
    param([System.Data.SqlClient.SqlConnection]$Connection)
    
    Write-ColorOutput "=== Archiving Old Logs ===" "Cyan"
    
    try {
        # Create archive table if it doesn't exist
        $createArchiveTable = @"
        IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[Logs_Archive]') AND type in (N'U'))
        BEGIN
            SELECT * INTO [dbo].[Logs_Archive] FROM [dbo].[Logs] WHERE 1=0
        END
"@
        
        $command = New-Object System.Data.SqlClient.SqlCommand($createArchiveTable, $Connection)
        $command.ExecuteNonQuery()
        
        # Archive old logs
        $archiveQuery = @"
        INSERT INTO [dbo].[Logs_Archive]
        SELECT * FROM [dbo].[Logs]
        WHERE LogDate < DATEADD(DAY, -$LogRetentionDays, GETDATE())
        
        DELETE FROM [dbo].[Logs]
        WHERE LogDate < DATEADD(DAY, -$LogRetentionDays, GETDATE())
"@
        
        $command = New-Object System.Data.SqlClient.SqlCommand($archiveQuery, $Connection)
        $rowsAffected = $command.ExecuteNonQuery()
        
        Write-ColorOutput "Archived $rowsAffected log records older than $LogRetentionDays days" "Green"
        
    }
    catch {
        Write-ColorOutput "Error archiving logs:" "Red"
        Write-ColorOutput $_.Exception.Message "Red"
    }
}

# Function to cleanup staging data
function Cleanup-StagingData {
    param([System.Data.SqlClient.SqlConnection]$Connection)
    
    Write-ColorOutput "=== Cleaning Up Staging Data ===" "Cyan"
    
    try {
        # Get staging tables with ExtractionDate column
        $query = @"
        SELECT DISTINCT t.name as TableName
        FROM sys.tables t
        INNER JOIN sys.columns c ON t.object_id = c.object_id
        WHERE t.name LIKE 'stg_%' AND c.name = 'ExtractionDate'
"@
        
        $command = New-Object System.Data.SqlClient.SqlCommand($query, $Connection)
        $reader = $command.ExecuteReader()
        
        $tables = @()
        while ($reader.Read()) {
            $tables += $reader['TableName']
        }
        $reader.Close()
        
        foreach ($table in $tables) {
            $cleanupQuery = @"
            DELETE FROM [dbo].[$table]
            WHERE ExtractionDate < DATEADD(DAY, -$StagingRetentionDays, GETDATE())
"@
            
            $command = New-Object System.Data.SqlClient.SqlCommand($cleanupQuery, $Connection)
            $rowsAffected = $command.ExecuteNonQuery()
            
            if ($rowsAffected -gt 0) {
                Write-ColorOutput "Cleaned up $rowsAffected rows from $table" "Green"
            }
        }
        
    }
    catch {
        Write-ColorOutput "Error cleaning up staging data:" "Red"
        Write-ColorOutput $_.Exception.Message "Red"
    }
}

# Function to cleanup Parquet files
function Cleanup-ParquetFiles {
    param([string]$ParquetPath = "C:\ETL\Parquet")
    
    Write-ColorOutput "=== Cleaning Up Parquet Files ===" "Cyan"
    
    try {
        if (Test-Path $ParquetPath) {
            $cutoffDate = (Get-Date).AddDays(-$ParquetRetentionDays)
            $oldFiles = Get-ChildItem $ParquetPath -Recurse -File | Where-Object { $_.LastWriteTime -lt $cutoffDate }
            
            if ($oldFiles.Count -gt 0) {
                $totalSize = ($oldFiles | Measure-Object -Property Length -Sum).Sum / 1MB
                $oldFiles | Remove-Item -Force
                Write-ColorOutput "Removed $($oldFiles.Count) Parquet files ($([math]::Round($totalSize, 2)) MB) older than $ParquetRetentionDays days" "Green"
            }
            else {
                Write-ColorOutput "No old Parquet files found" "Yellow"
            }
        }
        else {
            Write-ColorOutput "Parquet directory not found: $ParquetPath" "Yellow"
        }
    }
    catch {
        Write-ColorOutput "Error cleaning up Parquet files:" "Red"
        Write-ColorOutput $_.Exception.Message "Red"
    }
}

# Function to check system resources
function Check-SystemResources {
    Write-ColorOutput "`n=== System Resources ===" "Cyan"
    
    try {
        # Disk space
        $drives = Get-WmiObject -Class Win32_LogicalDisk | Where-Object { $_.DriveType -eq 3 }
        foreach ($drive in $drives) {
            $freeGB = [math]::Round($drive.FreeSpace / 1GB, 2)
            $totalGB = [math]::Round($drive.Size / 1GB, 2)
            $usedPercent = [math]::Round((($drive.Size - $drive.FreeSpace) / $drive.Size) * 100, 2)
            
            $color = if ($usedPercent -gt 90) { "Red" } elseif ($usedPercent -gt 80) { "Yellow" } else { "Green" }
            Write-ColorOutput "  Drive $($drive.DeviceID): $freeGB GB free of $totalGB GB ($usedPercent% used)" $color
        }
        
        # Memory usage
        $memory = Get-WmiObject -Class Win32_OperatingSystem
        $freeMemoryGB = [math]::Round($memory.FreePhysicalMemory / 1MB, 2)
        $totalMemoryGB = [math]::Round($memory.TotalVisibleMemorySize / 1MB, 2)
        $usedMemoryPercent = [math]::Round((($memory.TotalVisibleMemorySize - $memory.FreePhysicalMemory) / $memory.TotalVisibleMemorySize) * 100, 2)
        
        $color = if ($usedMemoryPercent -gt 90) { "Red" } elseif ($usedMemoryPercent -gt 80) { "Yellow" } else { "Green" }
        Write-ColorOutput "  Memory: $freeMemoryGB GB free of $totalMemoryGB GB ($usedMemoryPercent% used)" $color
        
    }
    catch {
        Write-ColorOutput "Error checking system resources:" "Red"
        Write-ColorOutput $_.Exception.Message "Red"
    }
}

# Main execution
Write-ColorOutput "=== ETL Monitoring and Maintenance Script ===" "Cyan"
Write-ColorOutput "Server Instance: $ServerInstance" "White"
Write-ColorOutput "Metadata Database: $MetadataDatabase" "White"
Write-ColorOutput "Staging Database: $StagingDatabase" "White"
Write-ColorOutput ""

# Get database connections
$metadataConn = Get-DatabaseConnection -ServerInstance $ServerInstance -Database $MetadataDatabase
$stagingConn = Get-DatabaseConnection -ServerInstance $ServerInstance -Database $StagingDatabase

if (-not $metadataConn -or -not $stagingConn) {
    Write-ColorOutput "Failed to connect to databases. Exiting." "Red"
    exit 1
}

try {
    # Monitoring tasks
    if (-not $MaintenanceOnly) {
        Monitor-ETLPerformance -Connection $metadataConn
        Monitor-StagingDatabase -Connection $stagingConn
        Check-SystemResources
    }
    
    # Maintenance tasks
    if (-not $MonitorOnly) {
        if ($ArchiveLogs -or $MaintenanceOnly) {
            Archive-Logs -Connection $metadataConn
        }
        
        if ($CleanupStaging -or $MaintenanceOnly) {
            Cleanup-StagingData -Connection $stagingConn
        }
        
        if ($CleanupParquet -or $MaintenanceOnly) {
            Cleanup-ParquetFiles
        }
    }
    
    Write-ColorOutput "`nScript completed successfully!" "Green"
}
finally {
    if ($metadataConn) { $metadataConn.Close() }
    if ($stagingConn) { $stagingConn.Close() }
}
