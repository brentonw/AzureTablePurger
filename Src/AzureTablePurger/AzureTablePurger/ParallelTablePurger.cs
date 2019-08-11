using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.WindowsAzure.Storage.Table;
using Serilog;

namespace AzureTablePurger
{
    /// <summary>
    /// Purges entities from an Azure table older than a specified number of days.
    ///
    /// Executes in a parallel using a producer/consumer pattern.
    ///
    /// One task is querying Azure Table Storage, downloading data and caching it locally on disk (since there is
    /// potentially too much data to deal with in memory, and depending on the partitioning of the data returned,
    /// we could produce a lot faster than we can consume).
    ///
    /// Another task is picking up items from the in-memory queue in parallel and processing them. Processing
    /// involves reading the cached data from disk, creating batch delete commands and dispatching them to
    /// Azure Table Storage.
    /// </summary>
    public class ParallelTablePurger : TablePurgerBase
    {
        private const int MaxParallelOperations = 32;
        public const int ConnectionLimit = 32;

        private const string TempDataFileDirectory = "AzureTablePurgerTempData";

        private readonly BlockingCollection<string> _partitionKeyQueue = new BlockingCollection<string>();

        private int _globalEntityCounter = 0;
        private int _globalPartitionCounter = 0;

        private CancellationTokenSource _cancellationTokenSource;

        public ParallelTablePurger(ILogger logger)
            : base(logger)
        {
            ServicePointManager.DefaultConnectionLimit = ConnectionLimit;
        }

        public override void PurgeEntities(out int numEntitiesProcessed, out int numPartitionsProcessed)
        {
            VerifyIsInitialized();

            void CollectData()
            {
                CollectDataToProcess(PurgeEntitiesOlderThanDays);
            }

            void ProcessData()
            {
                Parallel.ForEach(_partitionKeyQueue.GetConsumingPartitioner(), new ParallelOptions { MaxDegreeOfParallelism = MaxParallelOperations }, ProcessPartition);
            }

            _cancellationTokenSource = new CancellationTokenSource();

            Parallel.Invoke(new ParallelOptions { CancellationToken = _cancellationTokenSource.Token }, CollectData, ProcessData);

            numPartitionsProcessed = _globalPartitionCounter;
            numEntitiesProcessed = _globalEntityCounter;
        }
        
        /// <summary>
        /// Data collection step (ie producer).
        ///
        /// Collects data to process, caches it locally on disk and adds to the _partitionKeyQueue collection for the consumer
        /// </summary>
        private void CollectDataToProcess(int purgeRecordsOlderThanDays)
        {
            var query = PartitionKeyHandler.GetTableQuery(purgeRecordsOlderThanDays);
            var continuationToken = new TableContinuationToken();
            string previouslyCachedPartitionKey = null;

            try
            {
                do
                {
                    var page = TableReference.ExecuteQuerySegmented(query, continuationToken);

                    if (page.Results.Count == 0)
                    {
                        Logger.Information("No results available");
                        break;
                    }

                    var firstResultTimestamp = PartitionKeyHandler.ConvertPartitionKeyToDateTime(page.Results.First().PartitionKey);
                    LogStartingToProcessPage(page, firstResultTimestamp);

                    _cancellationTokenSource.Token.ThrowIfCancellationRequested();

                    var partitionsFromPage = GetPartitionsFromPage(page.Results);

                    foreach (var partition in partitionsFromPage)
                    {
                        var partitionKey = partition.First().PartitionKey;

                        if (!string.IsNullOrEmpty(previouslyCachedPartitionKey) && partitionKey != previouslyCachedPartitionKey)
                        {
                            // we've moved onto a new partitionKey, queue the one we previously cached
                            QueuePartitionKeyForProcessing(previouslyCachedPartitionKey);
                        }

                        using (var streamWriter = GetStreamWriterForPartitionTempFile(partitionKey))
                        {
                            foreach (var entity in partition)
                            {
                                var lineToWrite = $"{entity.PartitionKey},{entity.RowKey}";

                                streamWriter.WriteLine(lineToWrite);
                                Interlocked.Increment(ref _globalEntityCounter);
                            }
                        }

                        previouslyCachedPartitionKey = partitionKey;
                    }

                    continuationToken = page.ContinuationToken;

                } while (continuationToken != null);

                // queue the last partition we just processed
                if (previouslyCachedPartitionKey != null)
                {
                    QueuePartitionKeyForProcessing(previouslyCachedPartitionKey);
                }
            }
            finally
            {
                _partitionKeyQueue.CompleteAdding();
            }
        }

        private void QueuePartitionKeyForProcessing(string partitionKey)
        {
            _partitionKeyQueue.Add(partitionKey);
            Interlocked.Increment(ref _globalPartitionCounter);
            ConsoleLogProgressItemQueued();
        }

        /// <summary>
        /// Consumer: process a specific partition.
        /// </summary>
        private void ProcessPartition(string partitionKeyForPartition)
        {
            try
            {
                var fileContents = GetDataFromPartitionTempFile(partitionKeyForPartition);

                var chunkedData = Chunk(fileContents, MaxBatchSize);

                foreach (var chunk in chunkedData)
                {
                    var batchOperation = new TableBatchOperation();

                    foreach (var line in chunk)
                    {
                        string partitionKey = null;
                        string rowKey = null;

                        try
                        {
                            ExtractRowAndPartitionKey(line, out partitionKey, out rowKey);
                        }
                        catch (InvalidOperationException e)
                        {
                            Logger.Error(e, e.Message);
                            continue;
                        }

                        if (partitionKey != partitionKeyForPartition)
                        {
                            Logger.Error("PartitionKey doesn't match current partition we are processing, skipping. PartitionKey={partitionKey}, RowKey={rowKey}", partitionKey, rowKey);
                            continue;
                        }

                        var entity = new DynamicTableEntity(partitionKey, rowKey) { ETag = "*" };

                        batchOperation.Delete(entity);
                    }

                    TableReference.ExecuteBatch(batchOperation);
                }

                DeletePartitionTempFile(partitionKeyForPartition);

                ConsoleLogProgressItemProcessed();
            }
            catch (Exception e)
            {
                Logger.Fatal(e, "Error processing partition {partitionKeyForPartition}", partitionKeyForPartition);
                _cancellationTokenSource.Cancel();
                throw;
            }
        }

        private void ExtractRowAndPartitionKey(string line, out string partitionKey, out string rowKey)
        {
            var message = $"Line not in expected format: {line}";

            if (string.IsNullOrEmpty(line))
            {
                throw new InvalidOperationException(message);
            }

            var results = line.Split(new []{','}, StringSplitOptions.RemoveEmptyEntries);

            if (results.Length != 2)
            {
                throw new InvalidOperationException(message);
            }

            partitionKey = results[0];
            rowKey = results[1];
        }

        private StreamWriter GetStreamWriterForPartitionTempFile(string entityPartitionKey)
        {
            Directory.CreateDirectory(TempDataFileDirectory);

            return new StreamWriter(GetPartitionTempFileFullPath(entityPartitionKey), true);
        }

        private IEnumerable<string> GetDataFromPartitionTempFile(string entityPartitionKey)
        {
            return File.ReadLines(GetPartitionTempFileFullPath(entityPartitionKey));
        }

        private string GetPartitionTempFileFullPath(string entityPartitionKey)
        {
            return Path.Combine(TempDataFileDirectory, $"{entityPartitionKey}.txt");
        }

        private void DeletePartitionTempFile(string entityPartitionKey)
        {
            File.Delete(GetPartitionTempFileFullPath(entityPartitionKey));
        }
    }
}