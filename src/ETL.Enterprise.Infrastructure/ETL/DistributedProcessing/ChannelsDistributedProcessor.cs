using Microsoft.Extensions.Logging;
using System.Threading.Channels;

namespace ETL.Enterprise.Infrastructure.ETL.DistributedProcessing;

/// <summary>
/// Distributed processor using .NET Channels for producer-consumer pattern
/// </summary>
public class ChannelsDistributedProcessor
{
    private readonly ILogger<ChannelsDistributedProcessor> _logger;

    public ChannelsDistributedProcessor(ILogger<ChannelsDistributedProcessor> logger)
    {
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));
    }

    /// <summary>
    /// Process data using .NET Channels for producer-consumer pattern
    /// </summary>
    public async Task<ProcessingResult> ProcessWithChannelsAsync(
        IEnumerable<IDictionary<string, object>> data,
        int producerCount,
        int consumerCount,
        CancellationToken cancellationToken = default)
    {
        _logger.LogInformation("Starting Channels distributed processing with {ProducerCount} producers and {ConsumerCount} consumers", 
            producerCount, consumerCount);

        var startTime = DateTime.UtcNow;
        var recordsProcessed = 0;
        var recordsFailed = 0;

        try
        {
            // Create bounded channel for data flow
            var channel = Channel.CreateBounded<IDictionary<string, object>>(
                new BoundedChannelOptions(1000)
                {
                    FullMode = BoundedChannelFullMode.Wait,
                    SingleReader = false,
                    SingleWriter = false
                });

            // Start producers
            var producerTasks = StartProducersAsync(data, channel.Writer, producerCount, cancellationToken);

            // Start consumers
            var consumerTasks = StartConsumersAsync(channel.Reader, consumerCount, cancellationToken);

            // Wait for all producers to complete
            await Task.WhenAll(producerTasks);
            channel.Writer.Complete();

            // Wait for all consumers to complete
            var consumerResults = await Task.WhenAll(consumerTasks);

            // Aggregate results
            recordsProcessed = consumerResults.Sum(r => r.RecordsProcessed);
            recordsFailed = consumerResults.Sum(r => r.RecordsFailed);

            var duration = DateTime.UtcNow - startTime;

            return new ProcessingResult
            {
                IsSuccess = true,
                RecordsProcessed = recordsProcessed,
                RecordsFailed = recordsFailed,
                Duration = duration
            };
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error in Channels distributed processing");
            return new ProcessingResult
            {
                IsSuccess = false,
                RecordsProcessed = recordsProcessed,
                RecordsFailed = recordsFailed,
                Duration = DateTime.UtcNow - startTime,
                ErrorMessage = ex.Message
            };
        }
    }

    private async Task StartProducersAsync(
        IEnumerable<IDictionary<string, object>> data,
        ChannelWriter<IDictionary<string, object>> writer,
        int producerCount,
        CancellationToken cancellationToken)
    {
        var dataList = data.ToList();
        var chunkSize = Math.Max(1, dataList.Count / producerCount);

        var producerTasks = new List<Task>();

        for (int i = 0; i < producerCount; i++)
        {
            var producerId = i;
            var chunk = dataList.Skip(i * chunkSize).Take(chunkSize);
            
            var task = Task.Run(async () =>
            {
                try
                {
                    _logger.LogDebug("Producer {ProducerId} starting with {RecordCount} records", producerId, chunk.Count());

                    foreach (var record in chunk)
                    {
                        cancellationToken.ThrowIfCancellationRequested();
                        await writer.WriteAsync(record, cancellationToken);
                    }

                    _logger.LogDebug("Producer {ProducerId} completed", producerId);
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "Error in producer {ProducerId}", producerId);
                    throw;
                }
            }, cancellationToken);

            producerTasks.Add(task);
        }

        await Task.WhenAll(producerTasks);
    }

    private async Task<ProcessingResult> StartConsumersAsync(
        ChannelReader<IDictionary<string, object>> reader,
        int consumerCount,
        CancellationToken cancellationToken)
    {
        var consumerTasks = new List<Task<ProcessingResult>>();

        for (int i = 0; i < consumerCount; i++)
        {
            var consumerId = i;
            var task = Task.Run(async () =>
            {
                var recordsProcessed = 0;
                var recordsFailed = 0;

                try
                {
                    _logger.LogDebug("Consumer {ConsumerId} starting", consumerId);

                    await foreach (var record in reader.ReadAllAsync(cancellationToken))
                    {
                        try
                        {
                            // Process the record
                            await ProcessRecordAsync(record, cancellationToken);
                            recordsProcessed++;
                        }
                        catch (Exception ex)
                        {
                            _logger.LogError(ex, "Error processing record in consumer {ConsumerId}", consumerId);
                            recordsFailed++;
                        }
                    }

                    _logger.LogDebug("Consumer {ConsumerId} completed. Processed: {Processed}, Failed: {Failed}", 
                        consumerId, recordsProcessed, recordsFailed);

                    return new ProcessingResult
                    {
                        IsSuccess = true,
                        RecordsProcessed = recordsProcessed,
                        RecordsFailed = recordsFailed,
                        Duration = TimeSpan.Zero
                    };
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "Error in consumer {ConsumerId}", consumerId);
                    return new ProcessingResult
                    {
                        IsSuccess = false,
                        RecordsProcessed = recordsProcessed,
                        RecordsFailed = recordsFailed,
                        Duration = TimeSpan.Zero,
                        ErrorMessage = ex.Message
                    };
                }
            }, cancellationToken);

            consumerTasks.Add(task);
        }

        var results = await Task.WhenAll(consumerTasks);

        // Aggregate results
        var totalProcessed = results.Sum(r => r.RecordsProcessed);
        var totalFailed = results.Sum(r => r.RecordsFailed);

        return new ProcessingResult
        {
            IsSuccess = true,
            RecordsProcessed = totalProcessed,
            RecordsFailed = totalFailed,
            Duration = TimeSpan.Zero
        };
    }

    private async Task ProcessRecordAsync(IDictionary<string, object> record, CancellationToken cancellationToken)
    {
        // Simulate record processing
        await Task.Delay(10, cancellationToken);
    }

    public class ProcessingResult
    {
        public bool IsSuccess { get; set; }
        public int RecordsProcessed { get; set; }
        public int RecordsFailed { get; set; }
        public TimeSpan Duration { get; set; }
        public string? ErrorMessage { get; set; }
    }
}
