using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using Microsoft.Extensions.Logging;

namespace ETL.Tests.Unit
{
    /// <summary>
    /// Utility class for loading and parsing SQL queries from external files
    /// Supports both individual SQL files and SQL files with multiple queries
    /// </summary>
    public class SqlFileLoader
    {
        private readonly ILogger<SqlFileLoader> _logger;
        private readonly string _sqlFilesDirectory;

        public SqlFileLoader(ILogger<SqlFileLoader> logger, string sqlFilesDirectory = null)
        {
            _logger = logger;
            _sqlFilesDirectory = sqlFilesDirectory ?? Path.Combine(AppDomain.CurrentDomain.BaseDirectory, "SqlFiles");
        }

        /// <summary>
        /// Loads a single SQL query from a file
        /// </summary>
        /// <param name="fileName">Name of the SQL file</param>
        /// <returns>SQL query string</returns>
        public string LoadSqlQuery(string fileName)
        {
            try
            {
                var filePath = GetFilePath(fileName);
                if (!File.Exists(filePath))
                {
                    throw new FileNotFoundException($"SQL file not found: {filePath}");
                }

                var sqlContent = File.ReadAllText(filePath, Encoding.UTF8);
                var cleanedSql = CleanSqlQuery(sqlContent);
                
                _logger.LogDebug("Loaded SQL query from file: {FileName}", fileName);
                return cleanedSql;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error loading SQL query from file: {FileName}", fileName);
                throw;
            }
        }

        /// <summary>
        /// Loads multiple SQL queries from a single file
        /// Queries are separated by GO statements or custom delimiters
        /// </summary>
        /// <param name="fileName">Name of the SQL file</param>
        /// <param name="delimiter">Query delimiter (default: "GO")</param>
        /// <returns>List of SQL queries</returns>
        public List<SqlQueryInfo> LoadMultipleSqlQueries(string fileName, string delimiter = "GO")
        {
            try
            {
                var filePath = GetFilePath(fileName);
                if (!File.Exists(filePath))
                {
                    throw new FileNotFoundException($"SQL file not found: {filePath}");
                }

                var sqlContent = File.ReadAllText(filePath, Encoding.UTF8);
                var queries = ParseMultipleQueries(sqlContent, delimiter);
                
                _logger.LogDebug("Loaded {Count} SQL queries from file: {FileName}", queries.Count, fileName);
                return queries;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error loading multiple SQL queries from file: {FileName}", fileName);
                throw;
            }
        }

        /// <summary>
        /// Loads SQL queries from a directory
        /// </summary>
        /// <param name="directoryName">Name of the directory (relative to SqlFiles root)</param>
        /// <param name="filePattern">File pattern to match (default: "*.sql")</param>
        /// <returns>Dictionary of file names and their SQL queries</returns>
        public Dictionary<string, string> LoadSqlQueriesFromDirectory(string directoryName = null, string filePattern = "*.sql")
        {
            try
            {
                var directoryPath = string.IsNullOrEmpty(directoryName) 
                    ? _sqlFilesDirectory 
                    : Path.Combine(_sqlFilesDirectory, directoryName);

                if (!Directory.Exists(directoryPath))
                {
                    throw new DirectoryNotFoundException($"SQL directory not found: {directoryPath}");
                }

                var queries = new Dictionary<string, string>();
                var files = Directory.GetFiles(directoryPath, filePattern, SearchOption.AllDirectories);

                foreach (var file in files)
                {
                    var fileName = Path.GetFileNameWithoutExtension(file);
                    var sqlContent = File.ReadAllText(file, Encoding.UTF8);
                    var cleanedSql = CleanSqlQuery(sqlContent);
                    queries[fileName] = cleanedSql;
                }

                _logger.LogDebug("Loaded {Count} SQL queries from directory: {Directory}", queries.Count, directoryPath);
                return queries;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error loading SQL queries from directory: {Directory}", directoryName);
                throw;
            }
        }

        /// <summary>
        /// Loads SQL query with parameters from a file
        /// Supports parameterized queries with @ParameterName syntax
        /// </summary>
        /// <param name="fileName">Name of the SQL file</param>
        /// <returns>SQL query with parameter information</returns>
        public ParameterizedSqlQuery LoadParameterizedSqlQuery(string fileName)
        {
            try
            {
                var sqlQuery = LoadSqlQuery(fileName);
                var parameters = ExtractParameters(sqlQuery);
                
                return new ParameterizedSqlQuery
                {
                    Query = sqlQuery,
                    Parameters = parameters,
                    FileName = fileName
                };
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error loading parameterized SQL query from file: {FileName}", fileName);
                throw;
            }
        }

        /// <summary>
        /// Loads SQL query with metadata from a file
        /// Supports SQL files with metadata comments
        /// </summary>
        /// <param name="fileName">Name of the SQL file</param>
        /// <returns>SQL query with metadata</returns>
        public SqlQueryWithMetadata LoadSqlQueryWithMetadata(string fileName)
        {
            try
            {
                var filePath = GetFilePath(fileName);
                if (!File.Exists(filePath))
                {
                    throw new FileNotFoundException($"SQL file not found: {filePath}");
                }

                var sqlContent = File.ReadAllText(filePath, Encoding.UTF8);
                var metadata = ExtractMetadata(sqlContent);
                var cleanedSql = CleanSqlQuery(sqlContent);
                var parameters = ExtractParameters(cleanedSql);

                return new SqlQueryWithMetadata
                {
                    Query = cleanedSql,
                    Parameters = parameters,
                    FileName = fileName,
                    Metadata = metadata
                };
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error loading SQL query with metadata from file: {FileName}", fileName);
                throw;
            }
        }

        /// <summary>
        /// Validates SQL file syntax (basic validation)
        /// </summary>
        /// <param name="fileName">Name of the SQL file</param>
        /// <returns>Validation result</returns>
        public SqlValidationResult ValidateSqlFile(string fileName)
        {
            try
            {
                var sqlQuery = LoadSqlQuery(fileName);
                return ValidateSqlSyntax(sqlQuery);
            }
            catch (Exception ex)
            {
                return new SqlValidationResult
                {
                    IsValid = false,
                    Errors = new List<string> { ex.Message }
                };
            }
        }

        #region Private Helper Methods

        private string GetFilePath(string fileName)
        {
            if (Path.IsPathRooted(fileName))
            {
                return fileName;
            }

            // Ensure .sql extension
            if (!fileName.EndsWith(".sql", StringComparison.OrdinalIgnoreCase))
            {
                fileName += ".sql";
            }

            return Path.Combine(_sqlFilesDirectory, fileName);
        }

        private string CleanSqlQuery(string sqlContent)
        {
            if (string.IsNullOrWhiteSpace(sqlContent))
            {
                return string.Empty;
            }

            // Remove metadata comments (lines starting with --@)
            var lines = sqlContent.Split('\n');
            var cleanedLines = lines.Where(line => !line.TrimStart().StartsWith("--@")).ToArray();
            
            var cleanedSql = string.Join("\n", cleanedLines);
            
            // Remove excessive whitespace
            cleanedSql = Regex.Replace(cleanedSql, @"\s+", " ");
            
            // Remove leading/trailing whitespace
            cleanedSql = cleanedSql.Trim();
            
            return cleanedSql;
        }

        private List<SqlQueryInfo> ParseMultipleQueries(string sqlContent, string delimiter)
        {
            var queries = new List<SqlQueryInfo>();
            var delimiterRegex = new Regex($@"^\s*{Regex.Escape(delimiter)}\s*$", RegexOptions.Multiline | RegexOptions.IgnoreCase);
            
            var parts = delimiterRegex.Split(sqlContent);
            
            for (int i = 0; i < parts.Length; i++)
            {
                var query = parts[i].Trim();
                if (!string.IsNullOrWhiteSpace(query))
                {
                    queries.Add(new SqlQueryInfo
                    {
                        Query = CleanSqlQuery(query),
                        Index = i,
                        Delimiter = delimiter
                    });
                }
            }
            
            return queries;
        }

        private List<string> ExtractParameters(string sqlQuery)
        {
            var parameterRegex = new Regex(@"@(\w+)", RegexOptions.IgnoreCase);
            var matches = parameterRegex.Matches(sqlQuery);
            
            return matches.Cast<Match>()
                .Select(match => match.Groups[1].Value)
                .Distinct()
                .ToList();
        }

        private SqlQueryMetadata ExtractMetadata(string sqlContent)
        {
            var metadata = new SqlQueryMetadata();
            var lines = sqlContent.Split('\n');
            
            foreach (var line in lines)
            {
                var trimmedLine = line.Trim();
                if (trimmedLine.StartsWith("--@"))
                {
                    var metadataLine = trimmedLine.Substring(3).Trim();
                    var parts = metadataLine.Split(':', 2);
                    
                    if (parts.Length == 2)
                    {
                        var key = parts[0].Trim().ToLower();
                        var value = parts[1].Trim();
                        
                        switch (key)
                        {
                            case "name":
                                metadata.Name = value;
                                break;
                            case "description":
                                metadata.Description = value;
                                break;
                            case "type":
                                metadata.QueryType = value;
                                break;
                            case "timeout":
                                if (int.TryParse(value, out int timeout))
                                {
                                    metadata.TimeoutSeconds = timeout;
                                }
                                break;
                            case "database":
                                metadata.DatabaseType = value;
                                break;
                            case "author":
                                metadata.Author = value;
                                break;
                            case "version":
                                metadata.Version = value;
                                break;
                            case "tags":
                                metadata.Tags = value.Split(',').Select(t => t.Trim()).ToList();
                                break;
                        }
                    }
                }
            }
            
            return metadata;
        }

        private SqlValidationResult ValidateSqlSyntax(string sqlQuery)
        {
            var result = new SqlValidationResult { IsValid = true, Errors = new List<string>() };
            
            // Basic syntax validation
            if (string.IsNullOrWhiteSpace(sqlQuery))
            {
                result.IsValid = false;
                result.Errors.Add("SQL query is empty");
                return result;
            }
            
            // Check for balanced parentheses
            var openParens = sqlQuery.Count(c => c == '(');
            var closeParens = sqlQuery.Count(c => c == ')');
            if (openParens != closeParens)
            {
                result.IsValid = false;
                result.Errors.Add("Unbalanced parentheses in SQL query");
            }
            
            // Check for balanced quotes
            var singleQuotes = sqlQuery.Count(c => c == '\'');
            if (singleQuotes % 2 != 0)
            {
                result.IsValid = false;
                result.Errors.Add("Unbalanced single quotes in SQL query");
            }
            
            // Check for basic SQL keywords
            var upperSql = sqlQuery.ToUpper();
            var hasSelect = upperSql.Contains("SELECT");
            var hasInsert = upperSql.Contains("INSERT");
            var hasUpdate = upperSql.Contains("UPDATE");
            var hasDelete = upperSql.Contains("DELETE");
            var hasFrom = upperSql.Contains("FROM");
            
            if (!hasSelect && !hasInsert && !hasUpdate && !hasDelete)
            {
                result.IsValid = false;
                result.Errors.Add("SQL query does not contain any valid SQL keywords (SELECT, INSERT, UPDATE, DELETE)");
            }
            
            if ((hasSelect || hasUpdate || hasDelete) && !hasFrom)
            {
                result.IsValid = false;
                result.Errors.Add("SQL query appears to be missing FROM clause");
            }
            
            return result;
        }

        #endregion
    }

    #region Supporting Classes

    public class SqlQueryInfo
    {
        public string Query { get; set; }
        public int Index { get; set; }
        public string Delimiter { get; set; }
    }

    public class ParameterizedSqlQuery
    {
        public string Query { get; set; }
        public List<string> Parameters { get; set; }
        public string FileName { get; set; }
    }

    public class SqlQueryWithMetadata
    {
        public string Query { get; set; }
        public List<string> Parameters { get; set; }
        public string FileName { get; set; }
        public SqlQueryMetadata Metadata { get; set; }
    }

    public class SqlQueryMetadata
    {
        public string Name { get; set; }
        public string Description { get; set; }
        public string QueryType { get; set; }
        public int? TimeoutSeconds { get; set; }
        public string DatabaseType { get; set; }
        public string Author { get; set; }
        public string Version { get; set; }
        public List<string> Tags { get; set; } = new List<string>();
    }

    public class SqlValidationResult
    {
        public bool IsValid { get; set; }
        public List<string> Errors { get; set; } = new List<string>();
    }

    #endregion
}
