Enabling **Change Data Capture (CDC)** in SQL Server is a powerful way to track delta rows (inserts, updates, deletes) for tables, which is ideal for the ETL system processing 1 TB/day from N databases. Below, I explain how to enable CDC in SQL Server, how to handle cases where CDC is not currently enabled, and the potential side effects of enabling it. I'll also integrate this into the context of the .NET-based ETL framework using SSIS, .NET Spark, and C#.

### Enabling CDC in SQL Server

CDC is a feature in SQL Server (available in Enterprise, Developer, and some Standard editions) that captures insert, update, and delete operations on a table and stores them in change tables for ETL processing. Below are the steps to enable CDC.

#### Prerequisites
- **SQL Server Edition**: CDC is available in SQL Server Enterprise, Developer, and Standard editions (since SQL Server 2016 SP1). Check the edition with:
  ```sql
  SELECT SERVERPROPERTY('Edition');
  ```
- **Permissions**: You need `sysadmin` role to enable CDC at the database level and `db_owner` role for table-level enabling.
- **SQL Server Agent**: Must be running for CDC cleanup jobs.

#### Steps to Enable CDC

1. **Enable CDC at the Database Level**
   - Use the `sys.sp_cdc_enable_db` stored procedure to enable CDC for a database.
   ```sql
   USE YourDatabaseName;
   EXEC sys.sp_cdc_enable_db;
   ```
   - Verify CDC is enabled:
   ```sql
   SELECT is_cdc_enabled FROM sys.databases WHERE name = 'YourDatabaseName';
   ```
   - This creates system tables (e.g., `cdc.change_tables`, `cdc.captured_columns`) in the database.

2. **Enable CDC at the Table Level**
   - Enable CDC for specific tables using `sys.sp_cdc_enable_table`.
   - Example for a table named `Sales`:
   ```sql
   USE YourDatabaseName;
   EXEC sys.sp_cdc_enable_table
       @source_schema = N'dbo',
       @source_name = N'Sales',
       @role_name = NULL, -- Optional: Role for access control
       @supports_net_changes = 1; -- 1 for net changes, 0 for all changes
   ```
   - Parameters:
     - `@source_schema`: Schema of the table (e.g., `dbo`).
     - `@source_name`: Table name.
     - `@role_name`: Optional role to restrict access to CDC data (set to `NULL` for no restriction).
     - `@supports_net_changes`: Set to `1` to track net changes (latest state) or `0` for all changes (every operation).
   - This creates a change table (e.g., `cdc.dbo_Sales_CT`) and a capture job (`cdc.YourDatabaseName_capture`).

3. **Verify CDC Configuration**
   - Check enabled tables:
   ```sql
   SELECT * FROM cdc.change_tables;
   ```
   - Check capture and cleanup jobs:
   ```sql
   SELECT * FROM msdb.dbo.sysjobs WHERE name LIKE 'cdc.YourDatabaseName%';
   ```

4. **Query Change Data**
   - CDC stores changes in change tables (e.g., `cdc.dbo_Sales_CT`) with columns like:
     - `__${start_lsn}`: Log Sequence Number of the change.
     - `__${operation}`: 1 = delete, 2 = insert, 3 = update (old values), 4 = update (new values).
     - Tracked columns from the source table.
   - Use CDC functions to query changes:
   ```sql
   DECLARE @from_lsn BINARY(10) = sys.fn_cdc_get_min_lsn('dbo_Sales');
   DECLARE @to_lsn BINARY(10) = sys.fn_cdc_get_max_lsn();
   SELECT * FROM cdc.fn_cdc_get_all_changes_dbo_Sales(@from_lsn, @to_lsn, 'all');
   ```

5. **Configure Cleanup Job**
   - CDC creates a cleanup job to remove old change data (default retention: 3 days).
   - Modify retention period if needed:
   ```sql
   EXEC sys.sp_cdc_change_job
       @job_type = 'cleanup',
       @retention = 10080; -- Retention in minutes (e.g., 7 days)
   ```

#### Enabling CDC When Not Currently Enabled
If CDC is not enabled in the source databases (e.g., for the N databases in your ETL system), you can enable it by following these steps, but there are considerations:

1. **Check Compatibility**:
   - Verify the SQL Server edition supports CDC.
   - If using Standard Edition (pre-2016 SP1) or non-supported editions, CDC is unavailable. Fall back to timestamp-based delta tracking (as described in the previous response).

2. **Schema Changes**:
   - No schema changes are needed for source tables, but CDC creates system tables and jobs in the database.
   - Ensure sufficient disk space for CDC tables, especially for high-transaction tables.

3. **Enable CDC**:
   - Run the database-level and table-level enablement scripts (as above) for each of the N databases.
   - Update the ETL metadata database (`Connections` table) to set `IsCDCEnabled = 1` for these databases:
   ```sql
   UPDATE Connections
   SET IsCDCEnabled = 1
   WHERE DatabaseID IN (/* List of DatabaseIDs */);
   ```

4. **Update ETL Pipeline**:
   - Modify the SSIS package (`Extract_Delta.dtsx`) to check `IsCDCEnabled` and query CDC tables for delta rows.
   - Update the .NET Spark or C# transformation logic to process CDC data (e.g., filter by `__${start_lsn}`).
   - Example SSIS query for CDC:
   ```sql
   SELECT * FROM cdc.fn_cdc_get_all_changes_dbo_Sales(@LastProcessedLSN, sys.fn_cdc_get_max_lsn(), 'all');
   ```
   - Store the latest processed LSN in the `Connections` table after each run.

5. **Fallback for Non-CDC Databases**:
   - For databases where CDC cannot be enabled (e.g., MySQL, non-supported SQL Server editions), use the timestamp-based approach:
     - Ensure tables have a `LastModified` column.
     - Query rows where `LastModified > LastRunTimestamp` (stored in metadata).
   - Update the metadata database to reflect the delta tracking method:
   ```sql
   UPDATE Connections
   SET IsCDCEnabled = 0, DeltaCondition = 'WHERE LastModified > @LastRunTimestamp'
   WHERE DatabaseID IN (/* Non-CDC DatabaseIDs */);
   ```

### Potential Side Effects of Enabling CDC

Enabling CDC is generally low-impact, but there are potential side effects to consider, especially for a high-throughput ETL system processing 1 TB/day across N databases.

1. **Performance Overhead**:
   - **Impact**: CDC uses the SQL Server transaction log to capture changes, which adds minor overhead to write operations (inserts, updates, deletes).
   - **Mitigation**: Ensure the transaction log is on fast storage (e.g., SSD). Monitor log growth and configure appropriate log file sizes.
   - **ETL Context**: For 1 TB/day, the overhead is typically negligible with modern hardware, but test with high-transaction tables.

2. **Disk Space Usage**:
   - **Impact**: CDC tables (e.g., `cdc.dbo_Sales_CT`) store change data, which can grow significantly for high-transaction tables.
   - **Mitigation**: Configure the cleanup job to remove old data (e.g., retain 1-7 days of changes). Monitor disk usage and allocate sufficient storage.
   - **ETL Context**: For 1 TB/day, estimate CDC table size based on change rate (e.g., 10% daily changes = 100 GB/day additional storage).

3. **SQL Server Agent Dependency**:
   - **Impact**: CDC requires SQL Server Agent for capture and cleanup jobs. If the Agent is stopped, change data may not be captured or cleaned up.
   - **Mitigation**: Ensure SQL Server Agent is running and monitored. Set up alerts for job failures.
   - **ETL Context**: Integrate job status checks into the SSIS package (`Extract_Delta.dtsx`) to validate CDC jobs before extraction.

4. **Database Permissions**:
   - **Impact**: Enabling CDC requires `sysadmin` and `db_owner` permissions, and CDC tables may be accessible to users with `SELECT` permissions unless restricted.
   - **Mitigation**: Use `@role_name` in `sys.sp_cdc_enable_table` to limit access to CDC data. Encrypt sensitive data in source tables.
   - **ETL Context**: Secure connection strings in the ETL metadata database and use least-privilege accounts for SSIS and Spark.

5. **Compatibility Issues**:
   - **Impact**: CDC is not available in all SQL Server editions (e.g., Express) or older versions (pre-2008). Non-SQL Server databases (e.g., MySQL) require alternative delta tracking.
   - **Mitigation**: Use the hybrid approach (CDC + timestamp) as described. Check edition compatibility before enabling.
   - **ETL Context**: The ETL framework’s metadata-driven design supports both CDC and timestamp-based tracking, ensuring compatibility across N databases.

6. **Increased Complexity**:
   - **Impact**: CDC adds complexity to ETL logic (e.g., handling LSNs, processing insert/update/delete operations).
   - **Mitigation**: Encapsulate CDC logic in reusable SSIS tasks or Spark/C# functions. Document CDC workflows in `docs/Architecture.md`.
   - **ETL Context**: The provided SSIS and Spark/C# artifacts already handle CDC complexity via dynamic queries and metadata.

7. **Cleanup Job Overload**:
   - **Impact**: The cleanup job may struggle with very high change rates, causing delays or increased resource usage.
   - **Mitigation**: Adjust cleanup retention period (e.g., 1 day for high-throughput tables) and schedule during low-activity periods.
   - **ETL Context**: For 1 TB/day, ensure cleanup jobs are monitored via SQL Server Agent alerts integrated with SSIS logging.

### Integration with the ETL Framework

To incorporate CDC into the .NET-based ETL framework:

1. **Metadata Database Updates**:
   - Add `IsCDCEnabled`, `LastProcessedLSN`, and `LastRunTimestamp` to the `Connections` table (see updated `Metadata_Schema_Delta.sql`).
   - Store CDC-specific queries in the `Queries` table (e.g., `SELECT * FROM cdc.fn_cdc_get_all_changes_dbo_Sales`).

2. **SSIS Package Updates**:
   - Modify `Extract_Delta.dtsx` to:
     - Check `IsCDCEnabled` to determine whether to query CDC tables or use timestamp-based queries.
     - Use CDC Source components for CDC-enabled tables or OLE DB Source for timestamp-based tables.
     - Update `LastProcessedLSN` or `LastRunTimestamp` after extraction.
   - Example SSIS expression for dynamic query:
   ```plaintext
   User::IsCDCEnabled ? "SELECT * FROM cdc.fn_cdc_get_all_changes_dbo_" + User::TableName + "(@LastProcessedLSN, sys.fn_cdc_get_max_lsn(), 'all')"
                      : User::QueryText + " WHERE LastModified > @LastRunTimestamp"
   ```

3. **.NET Spark Updates**:
   - Update `Spark_MultiDB_Delta.cs` to query CDC tables via JDBC for CDC-enabled databases.
   - Process CDC operations (`__${operation}`) to handle inserts, updates, and deletes appropriately.
   - Example Spark SQL for CDC:
   ```sql
   SELECT id, column1, __$operation
   FROM table0
   WHERE __$start_lsn > 'LastProcessedLSN'
   ```

4. **C# Fallback Updates**:
   - Update `SSIS_Delta_Transform.cs` to filter rows based on `LastModified` or process CDC data if `IsCDCEnabled = 1`.
   - Handle CDC operation types (e.g., `__${operation}` = 2 for inserts, 4 for updates) in transformations.

5. **Monitoring and Logging**:
   - Log CDC-specific metrics (e.g., number of inserts, updates, deletes) to the `Logs` table.
   - Monitor CDC capture and cleanup jobs via SQL Server Agent alerts integrated with SSIS.

### Cursor AI Prompts for CDC Integration

Below are updated prompts for Cursor AI to generate components for CDC-enabled delta tracking in the ETL framework.

1. **SQL Script to Enable CDC**
   ```plaintext
   Generate a SQL script (Enable_CDC.sql) to enable Change Data Capture (CDC) in SQL Server for a database and its tables. The script should:
   - Enable CDC at the database level using sys.sp_cdc_enable_db.
   - Enable CDC for a sample table (e.g., dbo.Sales) using sys.sp_cdc_enable_table with supports_net_changes = 1.
   - Configure the CDC cleanup job to retain data for 1 day (1440 minutes).
   - Include checks to verify if CDC is already enabled (e.g., check sys.databases.is_cdc_enabled).
   - Add error handling for unsupported editions or permissions issues.
   - Include comments explaining each step.
   ```

2. **SSIS Package for CDC Extraction**
   ```plaintext
   Update the SSIS package (Extract_Delta.dtsx) to extract delta rows using CDC for SQL Server databases. The package should:
   - Retrieve connection strings, IsCDCEnabled, LastProcessedLSN, and DeltaCondition from a SQL Server metadata database (Connections and Queries tables).
   - Use a Foreach Loop Container to iterate over N databases.
   - For CDC-enabled databases, use CDC Source components to query change tables (e.g., cdc.fn_cdc_get_all_changes_dbo_Sales).
   - For non-CDC databases, use OLE DB Source with timestamp-based delta conditions (e.g., LastModified > @LastRunTimestamp).
   - Stage delta rows to Parquet files (for Spark) or SQL Server staging tables (for C#).
   - Update LastProcessedLSN or LastRunTimestamp in the metadata database.
   - Include error handling with event handlers to log failures to a Logs table.
   - Optimize for parallelism to handle 1 TB/day across N databases.
   - Add comments explaining each task.
   ```

3. **.NET Spark CDC Processing**
   ```plaintext
   Update the .NET Spark application (Spark_MultiDB_Delta.cs) in C# using Microsoft.Spark to process delta rows from CDC-enabled SQL Server databases. The application should:
   - Retrieve connection strings, queries, IsCDCEnabled, and LastProcessedLSN from a SQL Server metadata database.
   - For CDC-enabled databases, load delta rows from CDC tables via JDBC (e.g., cdc.dbo_Sales_CT WHERE __$start_lsn > LastProcessedLSN).
   - For non-CDC databases, apply timestamp-based delta conditions.
   - Perform a multi-database join using Spark SQL, handling CDC operation types (__$operation: 1=delete, 2=insert, 4=update).
   - Write results to a target SQL Server database via JDBC.
   - Update LastProcessedLSN in the metadata database.
   - Include error handling with try-catch and logging to console and metadata database.
   - Optimize for 1 TB/day with 10-20 Spark nodes.
   - Add detailed comments.
   ```

### Notes
- **Testing CDC**: Enable CDC on a test database with a small table to validate ETL integration before scaling to N databases.
- **Fallback for Non-CDC**: Ensure the ETL framework’s metadata-driven design supports timestamp-based tracking for non-CDC databases (e.g., MySQL).
- **Performance Monitoring**: Monitor CDC table growth and cleanup job performance using SQL Server Agent and SSIS logs.
- **Security**: Restrict access to CDC tables using `@role_name` and secure metadata database connections.
- **Documentation**: Update `docs/Architecture.md` to include CDC setup and delta tracking workflows.

By enabling CDC where supported and using timestamps as a fallback, the ETL framework efficiently tracks delta rows while minimizing side effects. The provided scripts and prompts ensure seamless integration into the existing system.