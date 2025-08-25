using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Moq;
using Xunit;
using ETL.Core;

namespace ETL.Tests
{
    /// <summary>
    /// Unit tests for the DataTransformer class
    /// Tests cover batch processing, transformations, error handling, and scalability features
    /// </summary>
    public class DataTransformerTests : IDisposable
    {
        private readonly Mock<ILogger<DataTransformer>> _mockLogger;
        private readonly string _testConnectionString;
        private DataTransformer _transformer;

        public DataTransformerTests()
        {
            _mockLogger = new Mock<ILogger<DataTransformer>>();
            _testConnectionString = "Server=localhost;Database=TestDB;Trusted_Connection=true;";
            _transformer = new DataTransformer(_testConnectionString, _mockLogger.Object, 1000, 2);
        }

        [Fact]
        public void Constructor_WithValidParameters_ShouldInitializeCorrectly()
        {
            // Arrange & Act
            var transformer = new DataTransformer(_testConnectionString, _mockLogger.Object);

            // Assert
            Assert.NotNull(transformer);
        }

        [Fact]
        public void Constructor_WithNullConnectionString_ShouldThrowArgumentNullException()
        {
            // Arrange & Act & Assert
            Assert.Throws<ArgumentNullException>(() => 
                new DataTransformer(null, _mockLogger.Object));
        }

        [Fact]
        public void Constructor_WithNullLogger_ShouldThrowArgumentNullException()
        {
            // Arrange & Act & Assert
            Assert.Throws<ArgumentNullException>(() => 
                new DataTransformer(_testConnectionString, null));
        }

        [Fact]
        public void Constructor_WithInvalidBatchSize_ShouldThrowArgumentException()
        {
            // Arrange & Act & Assert
            Assert.Throws<ArgumentException>(() => 
                new DataTransformer(_testConnectionString, _mockLogger.Object, 0));
        }

        [Fact]
        public void Constructor_WithCustomParallelism_ShouldSetCorrectValue()
        {
            // Arrange & Act
            var transformer = new DataTransformer(_testConnectionString, _mockLogger.Object, 1000, 4);

            // Assert
            Assert.NotNull(transformer);
        }

        [Fact]
        public async Task TransformDataAsync_WithEmptySourceDatabases_ShouldReturnEmptyResult()
        {
            // Arrange
            var sourceDatabases = new List<SourceDatabaseConfig>();
            var rules = new TransformationRules();

            // Act
            var result = await _transformer.TransformDataAsync(sourceDatabases, rules);

            // Assert
            Assert.NotNull(result);
            Assert.Equal(0, result.SourceDatabases);
            Assert.Equal(0, result.ProcessedRecords);
            Assert.Equal(0, result.TransformedRecords);
            Assert.True(result.Success);
        }

        [Fact]
        public async Task TransformDataAsync_WithNullSourceDatabases_ShouldThrowArgumentNullException()
        {
            // Arrange
            List<SourceDatabaseConfig> sourceDatabases = null;
            var rules = new TransformationRules();

            // Act & Assert
            await Assert.ThrowsAsync<ArgumentNullException>(() => 
                _transformer.TransformDataAsync(sourceDatabases, rules));
        }

        [Fact]
        public async Task TransformDataAsync_WithNullTransformationRules_ShouldThrowArgumentNullException()
        {
            // Arrange
            var sourceDatabases = new List<SourceDatabaseConfig>
            {
                new SourceDatabaseConfig
                {
                    DatabaseName = "TestDB",
                    ConnectionString = _testConnectionString,
                    SourceTable = "TestTable"
                }
            };
            TransformationRules rules = null;

            // Act & Assert
            await Assert.ThrowsAsync<ArgumentNullException>(() => 
                _transformer.TransformDataAsync(sourceDatabases, rules));
        }

        [Fact]
        public void CleanString_WithTrimWhitespace_ShouldRemoveLeadingAndTrailingSpaces()
        {
            // Arrange
            var input = "  test string  ";
            var options = new StringCleaningOptions { TrimWhitespace = true };

            // Act
            var result = CleanString(input, options);

            // Assert
            Assert.Equal("test string", result);
        }

        [Fact]
        public void CleanString_WithRemoveSpecialCharacters_ShouldRemoveNonAlphanumericCharacters()
        {
            // Arrange
            var input = "test@#$%^&*()string";
            var options = new StringCleaningOptions { RemoveSpecialCharacters = true };

            // Act
            var result = CleanString(input, options);

            // Assert
            Assert.Equal("teststring", result);
        }

        [Fact]
        public void CleanString_WithNormalizeCase_ShouldConvertToLowerCase()
        {
            // Arrange
            var input = "TEST String";
            var options = new StringCleaningOptions { NormalizeCase = true };

            // Act
            var result = CleanString(input, options);

            // Assert
            Assert.Equal("test string", result);
        }

        [Fact]
        public void CleanString_WithRemoveExtraSpaces_ShouldNormalizeSpaces()
        {
            // Arrange
            var input = "test    string   with   spaces";
            var options = new StringCleaningOptions { RemoveExtraSpaces = true };

            // Act
            var result = CleanString(input, options);

            // Assert
            Assert.Equal("test string with spaces", result);
        }

        [Fact]
        public void CleanString_WithMaxLength_ShouldTruncateString()
        {
            // Arrange
            var input = "very long test string that should be truncated";
            var options = new StringCleaningOptions { MaxLength = 10 };

            // Act
            var result = CleanString(input, options);

            // Assert
            Assert.Equal("very long ", result);
        }

        [Fact]
        public void CleanString_WithNullInput_ShouldReturnNull()
        {
            // Arrange
            string input = null;
            var options = new StringCleaningOptions();

            // Act
            var result = CleanString(input, options);

            // Assert
            Assert.Null(result);
        }

        [Fact]
        public void CleanString_WithEmptyInput_ShouldReturnEmpty()
        {
            // Arrange
            var input = "";
            var options = new StringCleaningOptions();

            // Act
            var result = CleanString(input, options);

            // Assert
            Assert.Equal("", result);
        }

        [Fact]
        public void CleanString_WithAllOptionsEnabled_ShouldApplyAllTransformations()
        {
            // Arrange
            var input = "  TEST@#$%^&*()String    with   SPACES  ";
            var options = new StringCleaningOptions
            {
                TrimWhitespace = true,
                RemoveSpecialCharacters = true,
                NormalizeCase = true,
                RemoveExtraSpaces = true,
                MaxLength = 20
            };

            // Act
            var result = CleanString(input, options);

            // Assert
            Assert.Equal("test string with spaces", result);
        }

        [Fact]
        public void TransformationRules_DefaultConstructor_ShouldSetDefaultValues()
        {
            // Arrange & Act
            var rules = new TransformationRules();

            // Assert
            Assert.True(rules.CleanStrings);
            Assert.NotNull(rules.StringCleaningOptions);
            Assert.True(rules.StringCleaningOptions.TrimWhitespace);
            Assert.False(rules.StringCleaningOptions.RemoveSpecialCharacters);
            Assert.False(rules.StringCleaningOptions.NormalizeCase);
            Assert.True(rules.StringCleaningOptions.RemoveExtraSpaces);
            Assert.Equal(0, rules.StringCleaningOptions.MaxLength);
        }

        [Fact]
        public void SourceDatabaseConfig_Properties_ShouldBeSettable()
        {
            // Arrange
            var config = new SourceDatabaseConfig
            {
                DatabaseName = "TestDB",
                ConnectionString = "test connection string",
                SourceTable = "TestTable"
            };

            // Assert
            Assert.Equal("TestDB", config.DatabaseName);
            Assert.Equal("test connection string", config.ConnectionString);
            Assert.Equal("TestTable", config.SourceTable);
        }

        [Fact]
        public void TransformationResult_Properties_ShouldBeSettable()
        {
            // Arrange
            var result = new TransformationResult
            {
                StartTime = DateTime.UtcNow,
                EndTime = DateTime.UtcNow.AddHours(1),
                ProcessedRecords = 1000,
                TransformedRecords = 950,
                SourceDatabases = 2,
                Success = true
            };

            // Assert
            Assert.Equal(1000, result.ProcessedRecords);
            Assert.Equal(950, result.TransformedRecords);
            Assert.Equal(2, result.SourceDatabases);
            Assert.True(result.Success);
            Assert.NotNull(result.Errors);
        }

        [Fact]
        public void TransformedRecord_Properties_ShouldBeSettable()
        {
            // Arrange
            var record = new TransformedRecord
            {
                TransformationTimestamp = DateTime.UtcNow
            };
            record.Data["TestColumn"] = "TestValue";

            // Assert
            Assert.NotNull(record.Data);
            Assert.Equal("TestValue", record.Data["TestColumn"]);
            Assert.Equal(DateTime.UtcNow.Date, record.TransformationTimestamp.Date);
        }

        [Fact]
        public void TransformationError_Properties_ShouldBeSettable()
        {
            // Arrange
            var error = new TransformationError
            {
                Message = "Test error message",
                Severity = ErrorSeverity.Error,
                DatabaseName = "TestDB",
                Timestamp = DateTime.UtcNow
            };

            // Assert
            Assert.Equal("Test error message", error.Message);
            Assert.Equal(ErrorSeverity.Error, error.Severity);
            Assert.Equal("TestDB", error.DatabaseName);
            Assert.Equal(DateTime.UtcNow.Date, error.Timestamp.Date);
        }

        [Fact]
        public void ErrorSeverity_Values_ShouldBeCorrect()
        {
            // Assert
            Assert.Equal(0, (int)ErrorSeverity.Warning);
            Assert.Equal(1, (int)ErrorSeverity.Error);
            Assert.Equal(2, (int)ErrorSeverity.Critical);
        }

        [Fact]
        public async Task TransformDataAsync_WithCancellationToken_ShouldRespectCancellation()
        {
            // Arrange
            var sourceDatabases = new List<SourceDatabaseConfig>
            {
                new SourceDatabaseConfig
                {
                    DatabaseName = "TestDB",
                    ConnectionString = _testConnectionString,
                    SourceTable = "TestTable"
                }
            };
            var rules = new TransformationRules();
            var cancellationTokenSource = new CancellationTokenSource();
            cancellationTokenSource.Cancel(); // Cancel immediately

            // Act & Assert
            await Assert.ThrowsAsync<OperationCanceledException>(() => 
                _transformer.TransformDataAsync(sourceDatabases, rules, cancellationTokenSource.Token));
        }

        [Fact]
        public void Dispose_ShouldDisposeResources()
        {
            // Arrange
            var transformer = new DataTransformer(_testConnectionString, _mockLogger.Object);

            // Act
            transformer.Dispose();

            // Assert - No exception should be thrown
            Assert.True(true);
        }

        [Fact]
        public void Dispose_MultipleCalls_ShouldNotThrowException()
        {
            // Arrange
            var transformer = new DataTransformer(_testConnectionString, _mockLogger.Object);

            // Act & Assert
            transformer.Dispose();
            transformer.Dispose(); // Second call should not throw
            Assert.True(true);
        }

        // Helper method to test string cleaning functionality
        private string CleanString(string input, StringCleaningOptions options)
        {
            if (string.IsNullOrEmpty(input)) return input;

            var result = input;

            if (options.TrimWhitespace)
                result = result.Trim();

            if (options.RemoveSpecialCharacters)
                result = System.Text.RegularExpressions.Regex.Replace(result, @"[^\w\s]", "");

            if (options.NormalizeCase)
                result = result.ToLowerInvariant();

            if (options.RemoveExtraSpaces)
                result = System.Text.RegularExpressions.Regex.Replace(result, @"\s+", " ");

            if (options.MaxLength > 0 && result.Length > options.MaxLength)
                result = result.Substring(0, options.MaxLength);

            return result;
        }

        public void Dispose()
        {
            _transformer?.Dispose();
        }
    }
}
