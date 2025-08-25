# SSIS Transform Component

## Overview

This is a high-performance SSIS Script Component for data transformations with parallel batch processing capabilities. The component is optimized for handling large-scale data processing scenarios with throughput targets of 1 TB/day.

## Features

- **High-Throughput Processing**: Optimized for processing large datasets efficiently
- **Batch Processing**: Processes data in configurable batches for optimal performance
- **Parallel Processing**: Supports concurrent batch processing
- **Error Handling**: Comprehensive error handling with retry logic
- **Bulk Insert**: Uses SQL Server bulk insert for optimal database performance
- **Progress Tracking**: Real-time progress monitoring and logging
- **Configurable**: Easily configurable batch sizes and timeouts

## Architecture

### Performance Optimizations

- **Batch Size**: Configurable batch size (default: 100,000 rows)
- **Concurrent Batches**: Supports multiple concurrent batch processing (default: 4)
- **Bulk Insert**: Uses SqlBulkCopy for optimal database performance
- **Async Processing**: Non-blocking batch processing
- **Connection Pooling**: Efficient database connection management

### Data Flow

1. **Input Processing**: Receives data from SSIS data flow
2. **Transformation**: Applies business logic transformations
3. **Batch Buffering**: Accumulates rows in memory buffer
4. **Batch Processing**: Processes full batches asynchronously
5. **Bulk Insert**: Writes transformed data to staging table
6. **Error Handling**: Captures and logs processing errors

## Configuration

### Performance Settings

```csharp
private const int BATCH_SIZE = 100000; // Rows per batch
private const int MAX_CONCURRENT_BATCHES = 4; // Parallel processing
private const int COMMAND_TIMEOUT = 300; // 5 minutes timeout
private const string STAGING_TABLE_NAME = "staging_transformed_data";
```

### Connection String

The component expects a connection string for the staging database. In a real SSIS implementation, this would be configured through the SSIS connection manager.

## Usage

### 1. SSIS Integration

To use this component in an actual SSIS package:

1. **Add Script Component**: Add a Script Component to your SSIS data flow
2. **Configure Input/Output**: Set up input and output columns
3. **Reference Assembly**: Reference this compiled assembly
4. **Configure Connection**: Set up database connection for staging
5. **Deploy**: Deploy the component to your SSIS environment

### 2. Standalone Usage

The component can also be used as a standalone .NET library:

```csharp
var scriptMain = new ScriptMain();
scriptMain.PreExecute();

// Process input rows
foreach (var inputRow in inputData)
{
    var buffer = new Input0Buffer
    {
        SourceId = inputRow.Id,
        InputString = inputRow.Data
    };
    scriptMain.Input0_ProcessInputRow(buffer);
}

scriptMain.PostExecute();
```

### 3. Database Setup

Create the staging table before running the component:

```csharp
ScriptMain.CreateStagingTable(connectionString);
```

## Transformation Logic

The component includes a sample transformation that:

1. **Converts to Uppercase**: Transforms input strings to uppercase
2. **Removes Special Characters**: Strips non-alphanumeric characters
3. **Trims Whitespace**: Removes leading/trailing whitespace

You can customize the `TransformString` method to implement your specific business logic.

## Error Handling

The component provides comprehensive error handling:

- **Row-Level Errors**: Individual row errors are captured and logged
- **Batch-Level Errors**: Batch processing errors are handled gracefully
- **Retry Logic**: Automatic retry for transient failures
- **Error Logging**: Detailed error messages and stack traces

## Performance Monitoring

The component tracks and reports:

- **Total Rows Processed**: Count of successfully processed rows
- **Error Count**: Number of rows that failed processing
- **Processing Duration**: Total time taken for processing
- **Throughput**: Rows processed per second

## Deployment

### Prerequisites

- SQL Server 2019 or later
- .NET Framework 4.7.2 or later
- SSIS 2019 or later (for SSIS integration)

### Installation

1. **Build the Component**: Compile the project
2. **Deploy to SSIS**: Copy the assembly to SSIS Script Components directory
3. **Configure Database**: Create staging table and configure connection
4. **Test**: Run test data through the component

### File Locations

- **Assembly**: `bin/Release/net472/SSIS_Transform.dll`
- **SSIS Directory**: `C:\Program Files\Microsoft SQL Server\150\DTS\ScriptComponents\`

## Customization

### Adding New Transformations

To add new transformation logic:

1. **Modify TransformString Method**: Update the transformation logic
2. **Add New Fields**: Extend the TransformedRow class
3. **Update Database Schema**: Modify the staging table structure
4. **Update Bulk Insert**: Modify column mappings

### Performance Tuning

To optimize performance:

1. **Adjust Batch Size**: Increase/decrease based on memory constraints
2. **Modify Concurrent Batches**: Adjust based on CPU cores
3. **Optimize Database**: Add appropriate indexes to staging table
4. **Monitor Resources**: Track memory and CPU usage

## Troubleshooting

### Common Issues

1. **Connection Errors**: Verify connection string and database access
2. **Memory Issues**: Reduce batch size if out of memory
3. **Timeout Errors**: Increase command timeout for large datasets
4. **Performance Issues**: Monitor and adjust batch processing parameters

### Logging

The component provides detailed logging through:

- **Console Output**: For standalone usage
- **SSIS Logging**: For SSIS integration
- **Database Logging**: Error details stored in staging table

## Support

For issues and questions:

1. **Check Logs**: Review console output and database logs
2. **Verify Configuration**: Ensure all settings are correct
3. **Test with Sample Data**: Use small datasets for testing
4. **Monitor Performance**: Track resource usage and throughput

## License

This component is part of the ETL Scalable solution. See the main project license for details.
