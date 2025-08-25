# ETL Scalable - Standalone Template

## Overview

This is a simplified, cross-platform ETL template that provides a basic structure for Extract, Transform, Load operations. The template is designed to be easily extensible and can be adapted for various data processing scenarios.

## Features

- **Cross-Platform Compatibility**: Works on Windows, macOS, and Linux
- **Database Connectivity**: Supports SQL Server, MySQL, Oracle, and PostgreSQL
- **Async Operations**: Full async/await support for better performance
- **Logging**: Integrated logging with Microsoft.Extensions.Logging
- **Configuration**: JSON-based configuration with environment variable support
- **Bulk Operations**: Efficient bulk data loading using SqlBulkCopy
- **Error Handling**: Comprehensive error handling and logging

## Project Structure

```
src/standalone/
├── ETL.Scalable.csproj          # Project file with dependencies
├── SimpleETLTemplate.cs         # Main ETL template implementation
└── README.md                    # This documentation file
```

## Dependencies

- **Microsoft.Extensions.Hosting**: Application hosting and lifecycle management
- **Microsoft.Extensions.Configuration**: Configuration management
- **Microsoft.Extensions.Logging**: Logging infrastructure
- **Microsoft.Data.SqlClient**: SQL Server connectivity
- **MySql.Data**: MySQL connectivity
- **Oracle.ManagedDataAccess.Core**: Oracle connectivity
- **Npgsql**: PostgreSQL connectivity
- **Newtonsoft.Json**: JSON processing
- **System.Diagnostics.PerformanceCounter**: Performance monitoring

## Usage

### 1. Configuration

Create an `appsettings.json` file in the project directory:

```json
{
  "ConnectionStrings": {
    "SourceDB": "Server=source-server;Database=source-db;Trusted_Connection=true;",
    "TargetDB": "Server=target-server;Database=target-db;Trusted_Connection=true;"
  },
  "Logging": {
    "LogLevel": {
      "Default": "Information",
      "Microsoft": "Warning",
      "Microsoft.Hosting.Lifetime": "Information"
    }
  }
}
```

### 2. Running the Application

```bash
# Build the project
dotnet build src/standalone/ETL.Scalable.csproj

# Run the application
dotnet run --project src/standalone/ETL.Scalable.csproj
```

### 3. Customizing the ETL Process

The `SimpleETLTemplate` class provides three main methods that you can customize:

#### ExtractDataAsync()
- Retrieves data from the source database
- Modify the SQL query to match your source table structure
- Add filtering, joins, or other data extraction logic

#### TransformData(DataTable sourceData)
- Applies transformations to the extracted data
- Current implementation converts string columns to uppercase
- Add your custom transformation logic here

#### LoadDataAsync(DataTable data)
- Loads transformed data to the target database
- Uses bulk copy for efficient data loading
- Modify the target table name and column mappings as needed

## Extending the Template

### Adding Spark Support

To add Apache Spark support, you can:

1. Add the Microsoft.Spark package to the project
2. Create a new ETL engine that implements the Spark DataFrame API
3. Replace the DataTable-based transformations with Spark operations

### Adding More Database Support

The template already includes packages for multiple databases. To use them:

1. Add connection strings for the additional databases
2. Create database-specific connection classes
3. Implement database-specific data extraction and loading logic

### Adding Data Validation

1. Create validation rules in the TransformData method
2. Add data quality checks before loading
3. Implement error handling for invalid data

## Performance Considerations

- **Bulk Operations**: Use SqlBulkCopy for efficient data loading
- **Async Operations**: All database operations are async for better performance
- **Connection Pooling**: Connection strings should include pooling settings
- **Batch Processing**: For large datasets, consider processing in batches

## Error Handling

The template includes comprehensive error handling:

- Database connection errors
- Data transformation errors
- Configuration errors
- All errors are logged with appropriate detail

## Logging

The application uses structured logging with the following log levels:

- **Information**: General process information
- **Warning**: Non-critical issues
- **Error**: Critical errors that may affect the process

## Security Considerations

- Store sensitive connection strings in environment variables
- Use Windows Authentication when possible
- Implement proper access controls for database connections
- Consider encrypting configuration files in production

## Deployment

### Docker Support

The template can be containerized using Docker:

```dockerfile
FROM mcr.microsoft.com/dotnet/runtime:6.0
COPY bin/Release/net6.0/ /app/
WORKDIR /app
ENTRYPOINT ["dotnet", "ETL.Scalable.dll"]
```

### Production Deployment

1. Build the application in Release mode
2. Deploy the compiled binaries to your target environment
3. Configure connection strings and logging
4. Set up monitoring and alerting
5. Implement proper backup and recovery procedures

## Troubleshooting

### Common Issues

1. **Connection String Errors**: Verify connection strings and network connectivity
2. **Permission Errors**: Ensure database user has appropriate permissions
3. **Configuration Errors**: Check appsettings.json format and file location
4. **Performance Issues**: Monitor database performance and adjust batch sizes

### Debugging

Enable debug logging by setting the log level to "Debug" in appsettings.json:

```json
{
  "Logging": {
    "LogLevel": {
      "Default": "Debug"
    }
  }
}
```

## Contributing

When extending this template:

1. Follow the existing code structure and patterns
2. Add appropriate logging for new operations
3. Include error handling for new functionality
4. Update this documentation for new features
5. Test thoroughly before deployment

## License

This template is part of the ETL Scalable solution. See the main project license for details.
