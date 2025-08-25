# Distributed Processing in Custom C# - Complete Guide

## Overview

Custom C# can achieve distributed processing using various .NET technologies and patterns. This guide covers multiple approaches for implementing distributed processing in the ETL Enterprise application.

## 1. Parallel Processing with PLINQ

### Basic Parallel Processing
```csharp
// Simple parallel processing
var results = data.AsParallel()
    .WithDegreeOfParallelism(Environment.ProcessorCount)
    .Select(record => ProcessRecord(record))
    .ToList();
```

### Advanced Parallel Processing
```csharp
// With cancellation and error handling
var results = data.AsParallel()
    .WithDegreeOfParallelism(maxDegreeOfParallelism)
    .WithCancellation(cancellationToken)
    .WithExecutionMode(ParallelExecutionMode.ForceParallelism)
    .Select(record =>
    {
        try
        {
            return ProcessRecord(record);
        }
        catch (Exception ex)
        {
            // Handle errors
            return null;
        }
    })
    .Where(result => result != null)
    .ToList();
```

## 2. Task-Based Parallel Processing

### Task Creation and Management
```csharp
public async Task<ProcessingResult> ProcessWithTasksAsync(
    IEnumerable<IDictionary<string, object>> data,
    int maxDegreeOfParallelism,
    CancellationToken cancellationToken)
{
    var dataList = data.ToList();
    var batchSize = Math.Max(1, dataList.Count / maxDegreeOfParallelism);
    var semaphore = new SemaphoreSlim(maxDegreeOfParallelism);

    var tasks = dataList.Chunk(batchSize).Select(async batch =>
    {
        await semaphore.WaitAsync(cancellationToken);
        try
        {
            return await ProcessBatchAsync(batch, cancellationToken);
        }
        finally
        {
            semaphore.Release();
        }
    });

    var results = await Task.WhenAll(tasks);
    return AggregateResults(results);
}
```

### Batch Processing with Tasks
```csharp
private async Task<BatchResult> ProcessBatchAsync(
    IEnumerable<IDictionary<string, object>> batch,
    CancellationToken cancellationToken)
{
    var processed = 0;
    var failed = 0;

    foreach (var record in batch)
    {
        cancellationToken.ThrowIfCancellationRequested();
        
        try
        {
            await ProcessRecordAsync(record, cancellationToken);
            processed++;
        }
        catch (Exception ex)
        {
            failed++;
            _logger.LogError(ex, "Error processing record");
        }
    }

    return new BatchResult { Processed = processed, Failed = failed };
}
```

## 3. Dataflow Pipeline Processing

### Creating Dataflow Pipeline
```csharp
public async Task<ProcessingResult> ProcessWithDataflowAsync(
    IEnumerable<IDictionary<string, object>> data,
    int maxDegreeOfParallelism,
    CancellationToken cancellationToken)
{
    // Create dataflow blocks
    var bufferBlock = new BufferBlock<IDictionary<string, object>>(
        new DataflowBlockOptions { BoundedCapacity = 1000 });

    var transformBlock = new TransformBlock<IDictionary<string, object>, IDictionary<string, object>>(
        async record => await TransformRecordAsync(record, cancellationToken),
        new ExecutionDataflowBlockOptions
        {
            MaxDegreeOfParallelism = maxDegreeOfParallelism,
            CancellationToken = cancellationToken
        });

    var actionBlock = new ActionBlock<IDictionary<string, object>>(
        async record => await LoadRecordAsync(record, cancellationToken),
        new ExecutionDataflowBlockOptions
        {
            MaxDegreeOfParallelism = maxDegreeOfParallelism,
            CancellationToken = cancellationToken
        });

    // Link the pipeline
    bufferBlock.LinkTo(transformBlock, new DataflowLinkOptions { PropagateCompletion = true });
    transformBlock.LinkTo(actionBlock, new DataflowLinkOptions { PropagateCompletion = true });

    // Feed data and wait for completion
    foreach (var record in data)
    {
        await bufferBlock.SendAsync(record, cancellationToken);
    }

    bufferBlock.Complete();
    await actionBlock.Completion;

    return new ProcessingResult { IsSuccess = true };
}
```

### Advanced Dataflow with Multiple Stages
```csharp
public async Task<ProcessingResult> ProcessWithMultiStageDataflowAsync(
    IEnumerable<IDictionary<string, object>> data,
    CancellationToken cancellationToken)
{
    // Stage 1: Buffer
    var bufferBlock = new BufferBlock<IDictionary<string, object>>(
        new DataflowBlockOptions { BoundedCapacity = 1000 });

    // Stage 2: Validate
    var validateBlock = new TransformBlock<IDictionary<string, object>, IDictionary<string, object>>(
        async record => await ValidateRecordAsync(record, cancellationToken),
        new ExecutionDataflowBlockOptions { MaxDegreeOfParallelism = 4 });

    // Stage 3: Transform
    var transformBlock = new TransformBlock<IDictionary<string, object>, IDictionary<string, object>>(
        async record => await TransformRecordAsync(record, cancellationToken),
        new ExecutionDataflowBlockOptions { MaxDegreeOfParallelism = 8 });

    // Stage 4: Load
    var loadBlock = new ActionBlock<IDictionary<string, object>>(
        async record => await LoadRecordAsync(record, cancellationToken),
        new ExecutionDataflowBlockOptions { MaxDegreeOfParallelism = 4 });

    // Link pipeline
    bufferBlock.LinkTo(validateBlock);
    validateBlock.LinkTo(transformBlock);
    transformBlock.LinkTo(loadBlock);

    // Feed data
    foreach (var record in data)
    {
        await bufferBlock.SendAsync(record, cancellationToken);
    }

    bufferBlock.Complete();
    await loadBlock.Completion;

    return new ProcessingResult { IsSuccess = true };
}
```

## 4. Channels for Producer-Consumer Pattern

### Basic Producer-Consumer with Channels
```csharp
public async Task<ProcessingResult> ProcessWithChannelsAsync(
    IEnumerable<IDictionary<string, object>> data,
    int producerCount,
    int consumerCount,
    CancellationToken cancellationToken)
{
    // Create channel
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

    // Wait for completion
    await Task.WhenAll(producerTasks);
    channel.Writer.Complete();
    var results = await Task.WhenAll(consumerTasks);

    return AggregateResults(results);
}
```

### Producer Implementation
```csharp
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
            foreach (var record in chunk)
            {
                cancellationToken.ThrowIfCancellationRequested();
                await writer.WriteAsync(record, cancellationToken);
            }
        }, cancellationToken);

        producerTasks.Add(task);
    }

    await Task.WhenAll(producerTasks);
}
```

### Consumer Implementation
```csharp
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
            var processed = 0;
            var failed = 0;

            await foreach (var record in reader.ReadAllAsync(cancellationToken))
            {
                try
                {
                    await ProcessRecordAsync(record, cancellationToken);
                    processed++;
                }
                catch (Exception ex)
                {
                    failed++;
                }
            }

            return new ProcessingResult
            {
                IsSuccess = true,
                RecordsProcessed = processed,
                RecordsFailed = failed
            };
        }, cancellationToken);

        consumerTasks.Add(task);
    }

    var results = await Task.WhenAll(consumerTasks);
    return AggregateResults(results);
}
```

## 5. Memory-Mapped Files for Large Datasets

### Memory-Mapped File Processing
```csharp
public async Task<ProcessingResult> ProcessWithMemoryMappedFilesAsync(
    string filePath,
    int maxDegreeOfParallelism,
    CancellationToken cancellationToken)
{
    using var mmf = MemoryMappedFile.CreateFromFile(filePath, FileMode.Open);
    using var accessor = mmf.CreateViewAccessor();

    var fileSize = accessor.Capacity;
    var chunkSize = fileSize / maxDegreeOfParallelism;

    var tasks = new List<Task<ChunkResult>>();

    for (int i = 0; i < maxDegreeOfParallelism; i++)
    {
        var startPosition = i * chunkSize;
        var endPosition = (i == maxDegreeOfParallelism - 1) ? fileSize : (i + 1) * chunkSize;
        
        var task = Task.Run(() => ProcessChunkAsync(accessor, startPosition, endPosition, cancellationToken));
        tasks.Add(task);
    }

    var results = await Task.WhenAll(tasks);
    return AggregateChunkResults(results);
}
```

### Chunk Processing
```csharp
private async Task<ChunkResult> ProcessChunkAsync(
    MemoryMappedViewAccessor accessor,
    long startPosition,
    long endPosition,
    CancellationToken cancellationToken)
{
    var processed = 0;
    var failed = 0;

    // Process data in the chunk
    for (long pos = startPosition; pos < endPosition; pos += RecordSize)
    {
        cancellationToken.ThrowIfCancellationRequested();

        try
        {
            var record = ReadRecordFromMemoryMappedFile(accessor, pos);
            await ProcessRecordAsync(record, cancellationToken);
            processed++;
        }
        catch (Exception ex)
        {
            failed++;
        }
    }

    return new ChunkResult { Processed = processed, Failed = failed };
}
```

## 6. Microsoft Orleans for Actor-Based Processing

### Orleans Grain Implementation
```csharp
// Grain interface
public interface IETLProcessingGrain : IGrainWithStringKey
{
    Task<ProcessingResult> ProcessChunkAsync(List<IDictionary<string, object>> chunk);
}

// Grain implementation
public class ETLProcessingGrain : Grain, IETLProcessingGrain
{
    public async Task<ProcessingResult> ProcessChunkAsync(List<IDictionary<string, object>> chunk)
    {
        var processed = 0;
        var failed = 0;

        foreach (var record in chunk)
        {
            try
            {
                await ProcessRecordAsync(record);
                processed++;
            }
            catch (Exception ex)
            {
                failed++;
            }
        }

        return new ProcessingResult
        {
            IsSuccess = true,
            RecordsProcessed = processed,
            RecordsFailed = failed
        };
    }
}
```

### Orleans Client Usage
```csharp
public async Task<ProcessingResult> ProcessWithOrleansAsync(
    IEnumerable<IDictionary<string, object>> data,
    int grainCount,
    CancellationToken cancellationToken)
{
    var dataList = data.ToList();
    var chunkSize = Math.Max(1, dataList.Count / grainCount);

    var grainTasks = new List<Task<ProcessingResult>>();

    for (int i = 0; i < grainCount; i++)
    {
        var chunk = dataList.Skip(i * chunkSize).Take(chunkSize).ToList();
        var grain = GrainFactory.GetGrain<IETLProcessingGrain>($"grain_{i}");
        var task = grain.ProcessChunkAsync(chunk);
        grainTasks.Add(task);
    }

    var results = await Task.WhenAll(grainTasks);
    return AggregateResults(results);
}
```

## 7. Reactive Extensions (Rx) for Stream Processing

### Rx Stream Processing
```csharp
public async Task<ProcessingResult> ProcessWithRxAsync(
    IEnumerable<IDictionary<string, object>> data,
    CancellationToken cancellationToken)
{
    var processed = 0;
    var failed = 0;

    var observable = data.ToObservable()
        .Buffer(100) // Buffer records
        .SelectMany(batch => batch.ToObservable())
        .Select(record => Observable.FromAsync(async () =>
        {
            try
            {
                await ProcessRecordAsync(record, cancellationToken);
                return new { Success = true, Error = (string?)null };
            }
            catch (Exception ex)
            {
                return new { Success = false, Error = ex.Message };
            }
        }))
        .Merge(maxDegreeOfParallelism);

    await observable.ForEachAsync(result =>
    {
        if (result.Success)
            processed++;
        else
            failed++;
    }, cancellationToken);

    return new ProcessingResult
    {
        IsSuccess = true,
        RecordsProcessed = processed,
        RecordsFailed = failed
    };
}
```

## 8. Performance Optimization Techniques

### Memory Management
```csharp
// Use object pooling for frequently allocated objects
private readonly ObjectPool<IDictionary<string, object>> _recordPool = 
    new DefaultObjectPool<IDictionary<string, object>>(new RecordPooledObjectPolicy());

// Use Span<T> for high-performance operations
public void ProcessSpan(Span<byte> data)
{
    // Process data without allocations
    for (int i = 0; i < data.Length; i += RecordSize)
    {
        var recordSpan = data.Slice(i, RecordSize);
        ProcessRecordSpan(recordSpan);
    }
}
```

### CPU Optimization
```csharp
// Use SIMD operations where possible
public unsafe void ProcessVectorized(float[] data)
{
    fixed (float* ptr = data)
    {
        var vectorPtr = (Vector<float>*)ptr;
        var vectorCount = data.Length / Vector<float>.Count;

        for (int i = 0; i < vectorCount; i++)
        {
            var vector = vectorPtr[i];
            var result = Vector.Multiply(vector, 2.0f);
            vectorPtr[i] = result;
        }
    }
}
```

## 9. Configuration and Monitoring

### Performance Configuration
```csharp
public class DistributedProcessingConfig
{
    public int MaxDegreeOfParallelism { get; set; } = Environment.ProcessorCount;
    public int BufferSize { get; set; } = 1000;
    public int BatchSize { get; set; } = 100;
    public TimeSpan Timeout { get; set; } = TimeSpan.FromMinutes(30);
    public bool EnableMonitoring { get; set; } = true;
    public string ProcessingStrategy { get; set; } = "Auto";
}
```

### Monitoring and Metrics
```csharp
public class ProcessingMetrics
{
    public int RecordsProcessed { get; set; }
    public int RecordsFailed { get; set; }
    public TimeSpan Duration { get; set; }
    public double Throughput => RecordsProcessed / Duration.TotalSeconds;
    public double ErrorRate => (double)RecordsFailed / (RecordsProcessed + RecordsFailed);
    public Dictionary<string, object> CustomMetrics { get; set; } = new();
}
```

## 10. Best Practices

### 1. **Choose the Right Strategy**
- **Small datasets (< 1GB)**: Parallel LINQ or Task-based
- **Medium datasets (1-10GB)**: Dataflow or Channels
- **Large datasets (> 10GB)**: Memory-mapped files or Orleans
- **Streaming data**: Rx or Dataflow

### 2. **Resource Management**
- Use `using` statements for disposable resources
- Implement proper cancellation token support
- Monitor memory usage and implement backpressure

### 3. **Error Handling**
- Implement retry policies with exponential backoff
- Log errors with sufficient context
- Graceful degradation when possible

### 4. **Performance Tuning**
- Profile and measure performance
- Adjust buffer sizes and parallelism levels
- Use appropriate data structures

### 5. **Monitoring**
- Track processing metrics
- Monitor resource usage
- Set up alerts for failures

## Conclusion

Custom C# provides multiple powerful options for distributed processing, from simple parallel processing to complex actor-based systems. The choice depends on your specific requirements, data size, and performance needs. The ETL Enterprise application supports all these approaches and can automatically select the most appropriate strategy based on your configuration.
