using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using System.Text.Json;
using System.Data;
using System.Globalization;

namespace ETL.Tests.Integration
{
    /// <summary>
    /// Advanced data validation utilities for ETL testing
    /// </summary>
    public static class DataValidationUtilities
    {
        #region File Comparison Methods

        /// <summary>
        /// Compares two CSV files with detailed row-by-row analysis
        /// </summary>
        public static async Task<DetailedComparisonResult> CompareCsvFilesAsync(
            string actualFile, 
            string expectedFile, 
            ComparisonOptions options = null)
        {
            options ??= new ComparisonOptions();
            var result = new DetailedComparisonResult();

            try
            {
                var actualData = await ReadCsvFileAsync(actualFile);
                var expectedData = await ReadCsvFileAsync(expectedFile);

                result.ActualRowCount = actualData.Count;
                result.ExpectedRowCount = expectedData.Count;

                // Compare row counts
                if (actualData.Count != expectedData.Count && !options.AllowRowCountDifferences)
                {
                    result.IsValid = false;
                    result.ErrorMessage = $"Row count mismatch: Actual={actualData.Count}, Expected={expectedData.Count}";
                    return result;
                }

                // Compare headers
                if (actualData.Count > 0 && expectedData.Count > 0)
                {
                    var actualHeaders = actualData[0].Keys.ToList();
                    var expectedHeaders = expectedData[0].Keys.ToList();
                    
                    var headerDifferences = CompareHeaders(actualHeaders, expectedHeaders, options);
                    result.HeaderDifferences.AddRange(headerDifferences);
                }

                // Compare data rows
                var maxRows = Math.Max(actualData.Count, expectedData.Count);
                for (int i = 0; i < maxRows; i++)
                {
                    var actualRow = i < actualData.Count ? actualData[i] : null;
                    var expectedRow = i < expectedData.Count ? expectedData[i] : null;

                    var rowComparison = CompareRows(actualRow, expectedRow, i, options);
                    result.RowComparisons.Add(rowComparison);
                }

                // Calculate summary statistics
                result.MatchingRows = result.RowComparisons.Count(r => r.IsMatch);
                result.DifferentRows = result.RowComparisons.Count(r => !r.IsMatch);
                result.MissingRows = result.RowComparisons.Count(r => r.ActualRow == null);
                result.ExtraRows = result.RowComparisons.Count(r => r.ExpectedRow == null);

                result.IsValid = result.DifferentRows == 0 && result.MissingRows == 0 && result.ExtraRows == 0;
            }
            catch (Exception ex)
            {
                result.IsValid = false;
                result.ErrorMessage = $"Comparison error: {ex.Message}";
            }

            return result;
        }

        /// <summary>
        /// Compares two JSON files with detailed analysis
        /// </summary>
        public static async Task<JsonComparisonResult> CompareJsonFilesAsync(
            string actualFile, 
            string expectedFile, 
            JsonComparisonOptions options = null)
        {
            options ??= new JsonComparisonOptions();
            var result = new JsonComparisonResult();

            try
            {
                var actualJson = await File.ReadAllTextAsync(actualFile);
                var expectedJson = await File.ReadAllTextAsync(expectedFile);

                var actualObject = JsonSerializer.Deserialize<JsonElement>(actualJson);
                var expectedObject = JsonSerializer.Deserialize<JsonElement>(expectedJson);

                result = CompareJsonElements(actualObject, expectedObject, "", options);
            }
            catch (Exception ex)
            {
                result.IsValid = false;
                result.ErrorMessage = $"JSON comparison error: {ex.Message}";
            }

            return result;
        }

        /// <summary>
        /// Validates data against a schema definition
        /// </summary>
        public static async Task<SchemaValidationResult> ValidateDataSchemaAsync(
            string dataFile, 
            string schemaFile, 
            SchemaValidationOptions options = null)
        {
            options ??= new SchemaValidationOptions();
            var result = new SchemaValidationResult();

            try
            {
                var data = await ReadCsvFileAsync(dataFile);
                var schema = await ReadSchemaAsync(schemaFile);

                foreach (var row in data)
                {
                    var rowValidation = ValidateRowAgainstSchema(row, schema, options);
                    result.RowValidations.Add(rowValidation);
                }

                result.IsValid = result.RowValidations.All(r => r.IsValid);
                result.ValidRows = result.RowValidations.Count(r => r.IsValid);
                result.InvalidRows = result.RowValidations.Count(r => !r.IsValid);
            }
            catch (Exception ex)
            {
                result.IsValid = false;
                result.ErrorMessage = $"Schema validation error: {ex.Message}";
            }

            return result;
        }

        #endregion

        #region Data Quality Validation Methods

        /// <summary>
        /// Validates data completeness (no null values in required fields)
        /// </summary>
        public static async Task<CompletenessValidationResult> ValidateDataCompletenessAsync(
            string dataFile, 
            string[] requiredFields)
        {
            var result = new CompletenessValidationResult();
            result.RequiredFields = requiredFields;

            try
            {
                var data = await ReadCsvFileAsync(dataFile);

                foreach (var row in data)
                {
                    var rowValidation = new RowCompletenessValidation
                    {
                        RowIndex = data.IndexOf(row)
                    };

                    foreach (var field in requiredFields)
                    {
                        if (!row.ContainsKey(field) || string.IsNullOrWhiteSpace(row[field]))
                        {
                            rowValidation.MissingFields.Add(field);
                        }
                    }

                    rowValidation.IsValid = rowValidation.MissingFields.Count == 0;
                    result.RowValidations.Add(rowValidation);
                }

                result.IsValid = result.RowValidations.All(r => r.IsValid);
                result.CompleteRows = result.RowValidations.Count(r => r.IsValid);
                result.IncompleteRows = result.RowValidations.Count(r => !r.IsValid);
            }
            catch (Exception ex)
            {
                result.IsValid = false;
                result.ErrorMessage = $"Completeness validation error: {ex.Message}";
            }

            return result;
        }

        /// <summary>
        /// Validates data accuracy against reference data
        /// </summary>
        public static async Task<AccuracyValidationResult> ValidateDataAccuracyAsync(
            string dataFile, 
            string referenceFile, 
            string[] keyFields)
        {
            var result = new AccuracyValidationResult();
            result.KeyFields = keyFields;

            try
            {
                var data = await ReadCsvFileAsync(dataFile);
                var referenceData = await ReadCsvFileAsync(referenceFile);

                // Create reference lookup
                var referenceLookup = referenceData.ToDictionary(
                    r => string.Join("|", keyFields.Select(k => r.ContainsKey(k) ? r[k] : "")),
                    r => r);

                foreach (var row in data)
                {
                    var rowValidation = new RowAccuracyValidation
                    {
                        RowIndex = data.IndexOf(row)
                    };

                    var key = string.Join("|", keyFields.Select(k => row.ContainsKey(k) ? row[k] : ""));
                    
                    if (referenceLookup.ContainsKey(key))
                    {
                        var referenceRow = referenceLookup[key];
                        var rowComparison = CompareRows(row, referenceRow, data.IndexOf(row), new ComparisonOptions());
                        
                        if (rowComparison.Differences.Any())
                        {
                            rowValidation.FieldDifferences.AddRange(rowComparison.Differences);
                        }
                    }
                    else
                    {
                        rowValidation.IsValid = false;
                        rowValidation.ErrorMessage = "No matching reference record found";
                    }

                    rowValidation.IsValid = rowValidation.FieldDifferences.Count == 0;
                    result.RowValidations.Add(rowValidation);
                }

                result.IsValid = result.RowValidations.All(r => r.IsValid);
                result.AccurateRows = result.RowValidations.Count(r => r.IsValid);
                result.InaccurateRows = result.RowValidations.Count(r => !r.IsValid);
            }
            catch (Exception ex)
            {
                result.IsValid = false;
                result.ErrorMessage = $"Accuracy validation error: {ex.Message}";
            }

            return result;
        }

        /// <summary>
        /// Validates data consistency across related datasets
        /// </summary>
        public static async Task<ConsistencyValidationResult> ValidateDataConsistencyAsync(
            string[] dataFiles, 
            string[] keyFields, 
            string[] consistencyRules)
        {
            var result = new ConsistencyValidationResult();
            result.KeyFields = keyFields;
            result.ConsistencyRules = consistencyRules;

            try
            {
                var datasets = new List<List<Dictionary<string, string>>>();
                
                foreach (var file in dataFiles)
                {
                    var data = await ReadCsvFileAsync(file);
                    datasets.Add(data);
                }

                // Implement consistency validation logic
                // This is a simplified example
                for (int i = 0; i < datasets.Count; i++)
                {
                    for (int j = i + 1; j < datasets.Count; j++)
                    {
                        var consistencyCheck = CheckConsistencyBetweenDatasets(
                            datasets[i], datasets[j], keyFields, consistencyRules);
                        result.ConsistencyChecks.Add(consistencyCheck);
                    }
                }

                result.IsValid = result.ConsistencyChecks.All(c => c.IsConsistent);
            }
            catch (Exception ex)
            {
                result.IsValid = false;
                result.ErrorMessage = $"Consistency validation error: {ex.Message}";
            }

            return result;
        }

        #endregion

        #region Statistical Validation Methods

        /// <summary>
        /// Performs statistical validation of data distributions
        /// </summary>
        public static async Task<StatisticalValidationResult> ValidateDataStatisticsAsync(
            string dataFile, 
            string[] numericFields, 
            StatisticalValidationOptions options = null)
        {
            options ??= new StatisticalValidationOptions();
            var result = new StatisticalValidationResult();
            result.NumericFields = numericFields;

            try
            {
                var data = await ReadCsvFileAsync(dataFile);

                foreach (var field in numericFields)
                {
                    var fieldStats = CalculateFieldStatistics(data, field);
                    result.FieldStatistics[field] = fieldStats;

                    // Check for outliers
                    var outliers = DetectOutliers(data, field, options.OutlierThreshold);
                    result.Outliers[field] = outliers;

                    // Check for distribution
                    var distributionCheck = CheckDistribution(data, field, options.ExpectedDistribution);
                    result.DistributionChecks[field] = distributionCheck;
                }

                result.IsValid = result.Outliers.Values.All(o => o.Count == 0) && 
                               result.DistributionChecks.Values.All(d => d.IsValid);
            }
            catch (Exception ex)
            {
                result.IsValid = false;
                result.ErrorMessage = $"Statistical validation error: {ex.Message}";
            }

            return result;
        }

        /// <summary>
        /// Validates data freshness (timestamps within expected range)
        /// </summary>
        public static async Task<FreshnessValidationResult> ValidateDataFreshnessAsync(
            string dataFile, 
            string timestampField, 
            TimeSpan maxAge)
        {
            var result = new FreshnessValidationResult();
            result.TimestampField = timestampField;
            result.MaxAge = maxAge;

            try
            {
                var data = await ReadCsvFileAsync(dataFile);
                var now = DateTime.UtcNow;

                foreach (var row in data)
                {
                    var rowValidation = new RowFreshnessValidation
                    {
                        RowIndex = data.IndexOf(row)
                    };

                    if (row.ContainsKey(timestampField))
                    {
                        if (DateTime.TryParse(row[timestampField], out DateTime timestamp))
                        {
                            var age = now - timestamp;
                            rowValidation.Age = age;
                            rowValidation.IsValid = age <= maxAge;
                            
                            if (!rowValidation.IsValid)
                            {
                                rowValidation.ErrorMessage = $"Data is {age.TotalHours:F2} hours old, exceeds maximum of {maxAge.TotalHours:F2} hours";
                            }
                        }
                        else
                        {
                            rowValidation.IsValid = false;
                            rowValidation.ErrorMessage = "Invalid timestamp format";
                        }
                    }
                    else
                    {
                        rowValidation.IsValid = false;
                        rowValidation.ErrorMessage = "Timestamp field not found";
                    }

                    result.RowValidations.Add(rowValidation);
                }

                result.IsValid = result.RowValidations.All(r => r.IsValid);
                result.FreshRows = result.RowValidations.Count(r => r.IsValid);
                result.StaleRows = result.RowValidations.Count(r => !r.IsValid);
            }
            catch (Exception ex)
            {
                result.IsValid = false;
                result.ErrorMessage = $"Freshness validation error: {ex.Message}";
            }

            return result;
        }

        #endregion

        #region Helper Methods

        /// <summary>
        /// Reads CSV file and returns list of dictionaries
        /// </summary>
        private static async Task<List<Dictionary<string, string>>> ReadCsvFileAsync(string filePath)
        {
            var data = new List<Dictionary<string, string>>();
            var lines = await File.ReadAllLinesAsync(filePath);
            
            if (lines.Length == 0) return data;

            var headers = ParseCsvLine(lines[0]);
            
            for (int i = 1; i < lines.Length; i++)
            {
                var values = ParseCsvLine(lines[i]);
                var row = new Dictionary<string, string>();
                
                for (int j = 0; j < headers.Count && j < values.Count; j++)
                {
                    row[headers[j]] = values[j];
                }
                
                data.Add(row);
            }
            
            return data;
        }

        /// <summary>
        /// Parses a CSV line handling quoted values
        /// </summary>
        private static List<string> ParseCsvLine(string line)
        {
            var result = new List<string>();
            var current = "";
            var inQuotes = false;
            
            for (int i = 0; i < line.Length; i++)
            {
                var c = line[i];
                
                if (c == '"')
                {
                    inQuotes = !inQuotes;
                }
                else if (c == ',' && !inQuotes)
                {
                    result.Add(current.Trim());
                    current = "";
                }
                else
                {
                    current += c;
                }
            }
            
            result.Add(current.Trim());
            return result;
        }

        /// <summary>
        /// Compares headers between two datasets
        /// </summary>
        private static List<HeaderDifference> CompareHeaders(
            List<string> actualHeaders, 
            List<string> expectedHeaders, 
            ComparisonOptions options)
        {
            var differences = new List<HeaderDifference>();
            
            var actualSet = actualHeaders.ToHashSet();
            var expectedSet = expectedHeaders.ToHashSet();
            
            var missingHeaders = expectedSet.Except(actualSet).ToList();
            var extraHeaders = actualSet.Except(expectedSet).ToList();
            
            foreach (var header in missingHeaders)
            {
                differences.Add(new HeaderDifference
                {
                    Header = header,
                    Type = HeaderDifferenceType.Missing,
                    Message = $"Header '{header}' is missing in actual data"
                });
            }
            
            foreach (var header in extraHeaders)
            {
                differences.Add(new HeaderDifference
                {
                    Header = header,
                    Type = HeaderDifferenceType.Extra,
                    Message = $"Header '{header}' is extra in actual data"
                });
            }
            
            return differences;
        }

        /// <summary>
        /// Compares two rows and returns detailed comparison
        /// </summary>
        private static RowComparison CompareRows(
            Dictionary<string, string> actualRow, 
            Dictionary<string, string> expectedRow, 
            int rowIndex, 
            ComparisonOptions options)
        {
            var comparison = new RowComparison
            {
                RowIndex = rowIndex,
                ActualRow = actualRow,
                ExpectedRow = expectedRow
            };
            
            if (actualRow == null)
            {
                comparison.IsMatch = false;
                comparison.Differences.Add(new FieldDifference
                {
                    Field = "Row",
                    ActualValue = "null",
                    ExpectedValue = "exists",
                    Message = "Row is missing in actual data"
                });
                return comparison;
            }
            
            if (expectedRow == null)
            {
                comparison.IsMatch = false;
                comparison.Differences.Add(new FieldDifference
                {
                    Field = "Row",
                    ActualValue = "exists",
                    ExpectedValue = "null",
                    Message = "Row is extra in actual data"
                });
                return comparison;
            }
            
            var allKeys = actualRow.Keys.Union(expectedRow.Keys).Distinct();
            
            foreach (var key in allKeys)
            {
                var actualValue = actualRow.ContainsKey(key) ? actualRow[key] : null;
                var expectedValue = expectedRow.ContainsKey(key) ? expectedRow[key] : null;
                
                if (actualValue != expectedValue)
                {
                    // Check for floating-point tolerance
                    if (options.FloatingPointTolerance > 0 && 
                        double.TryParse(actualValue, out double actualNum) && 
                        double.TryParse(expectedValue, out double expectedNum))
                    {
                        if (Math.Abs(actualNum - expectedNum) <= options.FloatingPointTolerance)
                        {
                            continue; // Values are within tolerance
                        }
                    }
                    
                    comparison.Differences.Add(new FieldDifference
                    {
                        Field = key,
                        ActualValue = actualValue ?? "null",
                        ExpectedValue = expectedValue ?? "null",
                        Message = $"Field '{key}' mismatch"
                    });
                }
            }
            
            comparison.IsMatch = comparison.Differences.Count == 0;
            return comparison;
        }

        /// <summary>
        /// Compares JSON elements recursively
        /// </summary>
        private static JsonComparisonResult CompareJsonElements(
            JsonElement actual, 
            JsonElement expected, 
            string path, 
            JsonComparisonOptions options)
        {
            var result = new JsonComparisonResult();
            
            if (actual.ValueKind != expected.ValueKind)
            {
                result.IsValid = false;
                result.Differences.Add(new JsonDifference
                {
                    Path = path,
                    ActualType = actual.ValueKind.ToString(),
                    ExpectedType = expected.ValueKind.ToString(),
                    Message = $"Type mismatch at {path}"
                });
                return result;
            }
            
            switch (actual.ValueKind)
            {
                case JsonValueKind.Object:
                    var actualProps = actual.EnumerateObject().ToDictionary(p => p.Name, p => p.Value);
                    var expectedProps = expected.EnumerateObject().ToDictionary(p => p.Name, p => p.Value);
                    
                    var allPropNames = actualProps.Keys.Union(expectedProps.Keys).Distinct();
                    
                    foreach (var propName in allPropNames)
                    {
                        var propPath = string.IsNullOrEmpty(path) ? propName : $"{path}.{propName}";
                        var propResult = CompareJsonElements(
                            actualProps.ContainsKey(propName) ? actualProps[propName] : default,
                            expectedProps.ContainsKey(propName) ? expectedProps[propName] : default,
                            propPath, options);
                        
                        result.Differences.AddRange(propResult.Differences);
                    }
                    break;
                    
                case JsonValueKind.Array:
                    var actualArray = actual.EnumerateArray().ToList();
                    var expectedArray = expected.EnumerateArray().ToList();
                    
                    if (actualArray.Count != expectedArray.Count)
                    {
                        result.Differences.Add(new JsonDifference
                        {
                            Path = path,
                            ActualValue = actualArray.Count.ToString(),
                            ExpectedValue = expectedArray.Count.ToString(),
                            Message = $"Array length mismatch at {path}"
                        });
                    }
                    else
                    {
                        for (int i = 0; i < actualArray.Count; i++)
                        {
                            var itemPath = $"{path}[{i}]";
                            var itemResult = CompareJsonElements(actualArray[i], expectedArray[i], itemPath, options);
                            result.Differences.AddRange(itemResult.Differences);
                        }
                    }
                    break;
                    
                default:
                    var actualValue = actual.ToString();
                    var expectedValue = expected.ToString();
                    
                    if (actualValue != expectedValue)
                    {
                        result.Differences.Add(new JsonDifference
                        {
                            Path = path,
                            ActualValue = actualValue,
                            ExpectedValue = expectedValue,
                            Message = $"Value mismatch at {path}"
                        });
                    }
                    break;
            }
            
            result.IsValid = result.Differences.Count == 0;
            return result;
        }

        /// <summary>
        /// Calculates statistics for a numeric field
        /// </summary>
        private static FieldStatistics CalculateFieldStatistics(
            List<Dictionary<string, string>> data, 
            string field)
        {
            var values = data
                .Where(r => r.ContainsKey(field) && double.TryParse(r[field], out _))
                .Select(r => double.Parse(r[field]))
                .ToList();
            
            if (values.Count == 0)
            {
                return new FieldStatistics { Field = field };
            }
            
            return new FieldStatistics
            {
                Field = field,
                Count = values.Count,
                Min = values.Min(),
                Max = values.Max(),
                Mean = values.Average(),
                Median = CalculateMedian(values),
                StandardDeviation = CalculateStandardDeviation(values)
            };
        }

        /// <summary>
        /// Calculates median value
        /// </summary>
        private static double CalculateMedian(List<double> values)
        {
            var sorted = values.OrderBy(x => x).ToList();
            var count = sorted.Count;
            
            if (count % 2 == 0)
            {
                return (sorted[count / 2 - 1] + sorted[count / 2]) / 2.0;
            }
            else
            {
                return sorted[count / 2];
            }
        }

        /// <summary>
        /// Calculates standard deviation
        /// </summary>
        private static double CalculateStandardDeviation(List<double> values)
        {
            var mean = values.Average();
            var variance = values.Select(x => Math.Pow(x - mean, 2)).Average();
            return Math.Sqrt(variance);
        }

        /// <summary>
        /// Detects outliers using IQR method
        /// </summary>
        private static List<Outlier> DetectOutliers(
            List<Dictionary<string, string>> data, 
            string field, 
            double threshold)
        {
            var outliers = new List<Outlier>();
            var values = data
                .Where(r => r.ContainsKey(field) && double.TryParse(r[field], out _))
                .Select((r, i) => (Value: double.Parse(r[field]), Index: i))
                .ToList();
            
            if (values.Count < 4) return outliers; // Need at least 4 values for IQR
            
            var sorted = values.OrderBy(x => x.Value).ToList();
            var q1 = CalculatePercentile(sorted, 25);
            var q3 = CalculatePercentile(sorted, 75);
            var iqr = q3 - q1;
            var lowerBound = q1 - threshold * iqr;
            var upperBound = q3 + threshold * iqr;
            
            foreach (var item in values)
            {
                if (item.Value < lowerBound || item.Value > upperBound)
                {
                    outliers.Add(new Outlier
                    {
                        RowIndex = item.Index,
                        Value = item.Value,
                        LowerBound = lowerBound,
                        UpperBound = upperBound
                    });
                }
            }
            
            return outliers;
        }

        /// <summary>
        /// Calculates percentile value
        /// </summary>
        private static double CalculatePercentile(List<(double Value, int Index)> sortedValues, double percentile)
        {
            var index = (percentile / 100.0) * (sortedValues.Count - 1);
            var lower = (int)Math.Floor(index);
            var upper = (int)Math.Ceiling(index);
            
            if (lower == upper)
            {
                return sortedValues[lower].Value;
            }
            
            var weight = index - lower;
            return sortedValues[lower].Value * (1 - weight) + sortedValues[upper].Value * weight;
        }

        /// <summary>
        /// Checks data distribution
        /// </summary>
        private static DistributionCheck CheckDistribution(
            List<Dictionary<string, string>> data, 
            string field, 
            string expectedDistribution)
        {
            // Simplified distribution check
            return new DistributionCheck
            {
                Field = field,
                ExpectedDistribution = expectedDistribution,
                IsValid = true // Placeholder implementation
            };
        }

        /// <summary>
        /// Validates row against schema
        /// </summary>
        private static RowSchemaValidation ValidateRowAgainstSchema(
            Dictionary<string, string> row, 
            DataSchema schema, 
            SchemaValidationOptions options)
        {
            var validation = new RowSchemaValidation();
            
            foreach (var field in schema.Fields)
            {
                var fieldValidation = ValidateFieldAgainstSchema(row, field);
                validation.FieldValidations.Add(fieldValidation);
            }
            
            validation.IsValid = validation.FieldValidations.All(f => f.IsValid);
            return validation;
        }

        /// <summary>
        /// Validates field against schema definition
        /// </summary>
        private static FieldSchemaValidation ValidateFieldAgainstSchema(
            Dictionary<string, string> row, 
            SchemaField field)
        {
            var validation = new FieldSchemaValidation
            {
                FieldName = field.Name
            };
            
            if (!row.ContainsKey(field.Name))
            {
                if (field.IsRequired)
                {
                    validation.IsValid = false;
                    validation.ErrorMessage = "Required field is missing";
                }
                return validation;
            }
            
            var value = row[field.Name];
            
            // Check data type
            switch (field.DataType.ToLower())
            {
                case "integer":
                    if (!int.TryParse(value, out _))
                    {
                        validation.IsValid = false;
                        validation.ErrorMessage = "Invalid integer format";
                    }
                    break;
                case "decimal":
                    if (!decimal.TryParse(value, out _))
                    {
                        validation.IsValid = false;
                        validation.ErrorMessage = "Invalid decimal format";
                    }
                    break;
                case "date":
                    if (!DateTime.TryParse(value, out _))
                    {
                        validation.IsValid = false;
                        validation.ErrorMessage = "Invalid date format";
                    }
                    break;
            }
            
            // Check constraints
            if (field.Constraints != null)
            {
                foreach (var constraint in field.Constraints)
                {
                    if (!ValidateConstraint(value, constraint))
                    {
                        validation.IsValid = false;
                        validation.ErrorMessage = $"Constraint violation: {constraint}";
                        break;
                    }
                }
            }
            
            return validation;
        }

        /// <summary>
        /// Validates field value against constraint
        /// </summary>
        private static bool ValidateConstraint(string value, string constraint)
        {
            // Simplified constraint validation
            return true; // Placeholder implementation
        }

        /// <summary>
        /// Checks consistency between two datasets
        /// </summary>
        private static ConsistencyCheck CheckConsistencyBetweenDatasets(
            List<Dictionary<string, string>> dataset1, 
            List<Dictionary<string, string>> dataset2, 
            string[] keyFields, 
            string[] rules)
        {
            // Simplified consistency check
            return new ConsistencyCheck
            {
                IsConsistent = true // Placeholder implementation
            };
        }

        /// <summary>
        /// Reads schema from file
        /// </summary>
        private static async Task<DataSchema> ReadSchemaAsync(string schemaFile)
        {
            var json = await File.ReadAllTextAsync(schemaFile);
            return JsonSerializer.Deserialize<DataSchema>(json) ?? new DataSchema();
        }

        #endregion
    }

    #region Configuration Classes

    /// <summary>
    /// Options for data comparison
    /// </summary>
    public class ComparisonOptions
    {
        public bool AllowRowCountDifferences { get; set; } = false;
        public double FloatingPointTolerance { get; set; } = 0.0;
        public bool CaseSensitive { get; set; } = true;
        public bool TrimWhitespace { get; set; } = true;
    }

    /// <summary>
    /// Options for JSON comparison
    /// </summary>
    public class JsonComparisonOptions
    {
        public bool IgnoreArrayOrder { get; set; } = false;
        public string[] IgnoreFields { get; set; } = Array.Empty<string>();
    }

    /// <summary>
    /// Options for schema validation
    /// </summary>
    public class SchemaValidationOptions
    {
        public bool StrictMode { get; set; } = true;
        public bool AllowExtraFields { get; set; } = false;
    }

    /// <summary>
    /// Options for statistical validation
    /// </summary>
    public class StatisticalValidationOptions
    {
        public double OutlierThreshold { get; set; } = 1.5;
        public string ExpectedDistribution { get; set; } = "normal";
    }

    #endregion

    #region Result Classes

    /// <summary>
    /// Result of detailed CSV comparison
    /// </summary>
    public class DetailedComparisonResult
    {
        public bool IsValid { get; set; }
        public string ErrorMessage { get; set; } = string.Empty;
        public int ActualRowCount { get; set; }
        public int ExpectedRowCount { get; set; }
        public int MatchingRows { get; set; }
        public int DifferentRows { get; set; }
        public int MissingRows { get; set; }
        public int ExtraRows { get; set; }
        public List<HeaderDifference> HeaderDifferences { get; set; } = new List<HeaderDifference>();
        public List<RowComparison> RowComparisons { get; set; } = new List<RowComparison>();
    }

    /// <summary>
    /// Result of JSON comparison
    /// </summary>
    public class JsonComparisonResult
    {
        public bool IsValid { get; set; }
        public string ErrorMessage { get; set; } = string.Empty;
        public List<JsonDifference> Differences { get; set; } = new List<JsonDifference>();
    }

    /// <summary>
    /// Result of schema validation
    /// </summary>
    public class SchemaValidationResult
    {
        public bool IsValid { get; set; }
        public string ErrorMessage { get; set; } = string.Empty;
        public int ValidRows { get; set; }
        public int InvalidRows { get; set; }
        public List<RowSchemaValidation> RowValidations { get; set; } = new List<RowSchemaValidation>();
    }

    /// <summary>
    /// Result of completeness validation
    /// </summary>
    public class CompletenessValidationResult
    {
        public bool IsValid { get; set; }
        public string ErrorMessage { get; set; } = string.Empty;
        public string[] RequiredFields { get; set; } = Array.Empty<string>();
        public int CompleteRows { get; set; }
        public int IncompleteRows { get; set; }
        public List<RowCompletenessValidation> RowValidations { get; set; } = new List<RowCompletenessValidation>();
    }

    /// <summary>
    /// Result of accuracy validation
    /// </summary>
    public class AccuracyValidationResult
    {
        public bool IsValid { get; set; }
        public string ErrorMessage { get; set; } = string.Empty;
        public string[] KeyFields { get; set; } = Array.Empty<string>();
        public int AccurateRows { get; set; }
        public int InaccurateRows { get; set; }
        public List<RowAccuracyValidation> RowValidations { get; set; } = new List<RowAccuracyValidation>();
    }

    /// <summary>
    /// Result of consistency validation
    /// </summary>
    public class ConsistencyValidationResult
    {
        public bool IsValid { get; set; }
        public string ErrorMessage { get; set; } = string.Empty;
        public string[] KeyFields { get; set; } = Array.Empty<string>();
        public string[] ConsistencyRules { get; set; } = Array.Empty<string>();
        public List<ConsistencyCheck> ConsistencyChecks { get; set; } = new List<ConsistencyCheck>();
    }

    /// <summary>
    /// Result of statistical validation
    /// </summary>
    public class StatisticalValidationResult
    {
        public bool IsValid { get; set; }
        public string ErrorMessage { get; set; } = string.Empty;
        public string[] NumericFields { get; set; } = Array.Empty<string>();
        public Dictionary<string, FieldStatistics> FieldStatistics { get; set; } = new Dictionary<string, FieldStatistics>();
        public Dictionary<string, List<Outlier>> Outliers { get; set; } = new Dictionary<string, List<Outlier>>();
        public Dictionary<string, DistributionCheck> DistributionChecks { get; set; } = new Dictionary<string, DistributionCheck>();
    }

    /// <summary>
    /// Result of freshness validation
    /// </summary>
    public class FreshnessValidationResult
    {
        public bool IsValid { get; set; }
        public string ErrorMessage { get; set; } = string.Empty;
        public string TimestampField { get; set; } = string.Empty;
        public TimeSpan MaxAge { get; set; }
        public int FreshRows { get; set; }
        public int StaleRows { get; set; }
        public List<RowFreshnessValidation> RowValidations { get; set; } = new List<RowFreshnessValidation>();
    }

    #endregion

    #region Data Classes

    /// <summary>
    /// Represents a header difference
    /// </summary>
    public class HeaderDifference
    {
        public string Header { get; set; } = string.Empty;
        public HeaderDifferenceType Type { get; set; }
        public string Message { get; set; } = string.Empty;
    }

    /// <summary>
    /// Types of header differences
    /// </summary>
    public enum HeaderDifferenceType
    {
        Missing,
        Extra,
        Mismatch
    }

    /// <summary>
    /// Represents a row comparison
    /// </summary>
    public class RowComparison
    {
        public int RowIndex { get; set; }
        public Dictionary<string, string> ActualRow { get; set; } = new Dictionary<string, string>();
        public Dictionary<string, string> ExpectedRow { get; set; } = new Dictionary<string, string>();
        public bool IsMatch { get; set; }
        public List<FieldDifference> Differences { get; set; } = new List<FieldDifference>();
    }

    /// <summary>
    /// Represents a field difference
    /// </summary>
    public class FieldDifference
    {
        public string Field { get; set; } = string.Empty;
        public string ActualValue { get; set; } = string.Empty;
        public string ExpectedValue { get; set; } = string.Empty;
        public string Message { get; set; } = string.Empty;
    }

    /// <summary>
    /// Represents a JSON difference
    /// </summary>
    public class JsonDifference
    {
        public string Path { get; set; } = string.Empty;
        public string ActualValue { get; set; } = string.Empty;
        public string ExpectedValue { get; set; } = string.Empty;
        public string ActualType { get; set; } = string.Empty;
        public string ExpectedType { get; set; } = string.Empty;
        public string Message { get; set; } = string.Empty;
    }

    /// <summary>
    /// Represents field statistics
    /// </summary>
    public class FieldStatistics
    {
        public string Field { get; set; } = string.Empty;
        public int Count { get; set; }
        public double Min { get; set; }
        public double Max { get; set; }
        public double Mean { get; set; }
        public double Median { get; set; }
        public double StandardDeviation { get; set; }
    }

    /// <summary>
    /// Represents an outlier
    /// </summary>
    public class Outlier
    {
        public int RowIndex { get; set; }
        public double Value { get; set; }
        public double LowerBound { get; set; }
        public double UpperBound { get; set; }
    }

    /// <summary>
    /// Represents a distribution check
    /// </summary>
    public class DistributionCheck
    {
        public string Field { get; set; } = string.Empty;
        public string ExpectedDistribution { get; set; } = string.Empty;
        public bool IsValid { get; set; }
    }

    /// <summary>
    /// Represents a consistency check
    /// </summary>
    public class ConsistencyCheck
    {
        public bool IsConsistent { get; set; }
        public string Message { get; set; } = string.Empty;
    }

    /// <summary>
    /// Represents row schema validation
    /// </summary>
    public class RowSchemaValidation
    {
        public bool IsValid { get; set; }
        public List<FieldSchemaValidation> FieldValidations { get; set; } = new List<FieldSchemaValidation>();
    }

    /// <summary>
    /// Represents field schema validation
    /// </summary>
    public class FieldSchemaValidation
    {
        public string FieldName { get; set; } = string.Empty;
        public bool IsValid { get; set; }
        public string ErrorMessage { get; set; } = string.Empty;
    }

    /// <summary>
    /// Represents row completeness validation
    /// </summary>
    public class RowCompletenessValidation
    {
        public int RowIndex { get; set; }
        public bool IsValid { get; set; }
        public List<string> MissingFields { get; set; } = new List<string>();
    }

    /// <summary>
    /// Represents row accuracy validation
    /// </summary>
    public class RowAccuracyValidation
    {
        public int RowIndex { get; set; }
        public bool IsValid { get; set; }
        public string ErrorMessage { get; set; } = string.Empty;
        public List<FieldDifference> FieldDifferences { get; set; } = new List<FieldDifference>();
    }

    /// <summary>
    /// Represents row freshness validation
    /// </summary>
    public class RowFreshnessValidation
    {
        public int RowIndex { get; set; }
        public bool IsValid { get; set; }
        public string ErrorMessage { get; set; } = string.Empty;
        public TimeSpan Age { get; set; }
    }

    /// <summary>
    /// Represents a data schema
    /// </summary>
    public class DataSchema
    {
        public List<SchemaField> Fields { get; set; } = new List<SchemaField>();
    }

    /// <summary>
    /// Represents a schema field
    /// </summary>
    public class SchemaField
    {
        public string Name { get; set; } = string.Empty;
        public string DataType { get; set; } = string.Empty;
        public bool IsRequired { get; set; }
        public List<string> Constraints { get; set; } = new List<string>();
    }

    #endregion
}
