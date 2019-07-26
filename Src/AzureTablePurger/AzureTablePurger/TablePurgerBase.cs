using System;
using System.Collections.Generic;
using System.Linq;
using AzureTablePurger.Enums;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Table;

namespace AzureTablePurger
{
    abstract class TablePurgerBase : ITablePurger
    {
        public const int MaxBatchSize = 100;

        public bool IsInitialized { get; private set; }

        public string StorageAccountConnectionString { get; set; }

        public string TableName { get; set; }

        public IPartitionKeyHandler PartitionKeyHandler { get; set; }

        public PartitionKeyFormat PartitionKeyFormat { get; set; }

        public int PurgeEntitiesOlderThanDays { get; set; }

        public CloudTable TableReference { get; set; }

        public void Initialize(string storageAccountConnectionString, string tableName, IPartitionKeyHandler partitionKeyHandler, PartitionKeyFormat partitionKeyFormat,
            int purgeEntitiesOlderThanDays)
        {
            StorageAccountConnectionString = storageAccountConnectionString;
            TableName = tableName;
            PartitionKeyHandler = partitionKeyHandler;
            PartitionKeyFormat = partitionKeyFormat;
            PurgeEntitiesOlderThanDays = purgeEntitiesOlderThanDays;

            TableReference = GetTableReference(StorageAccountConnectionString, TableName);

            IsInitialized = true;
        }

        public abstract void PurgeEntities(out int numEntitiesProcessed, out int numPartitionsProcessed);

        protected CloudTable GetTableReference(string connectionString, string tableName)
        {
            var storageAccount = CloudStorageAccount.Parse(connectionString);
            var client = storageAccount.CreateCloudTableClient();
            var tableReference = client.GetTableReference(tableName);

            var doesTableExist = tableReference.Exists();

            if (!doesTableExist)
            {
                throw new InvalidOperationException($"The table '{tableName}' does not exist on the provided storage account");
            }

            return tableReference;
        }

        protected IList<IList<DynamicTableEntity>> GetPartitionsFromPage(IList<DynamicTableEntity> page)
        {
            var result = new List<IList<DynamicTableEntity>>();

            var groupByResult = page.GroupBy(x => x.PartitionKey);

            foreach (var partition in groupByResult.ToList())
            {
                var partitionAsList = partition.ToList();
                if (partitionAsList.Count > MaxBatchSize)
                {
                    var chunkedPartitions = Chunk(partition, MaxBatchSize);
                    foreach (var smallerPartition in chunkedPartitions)
                    {
                        result.Add(smallerPartition.ToList());
                    }
                }
                else
                {
                    result.Add(partitionAsList);
                }
            }

            return result;
        }

        protected IEnumerable<IEnumerable<DynamicTableEntity>> Chunk(IEnumerable<DynamicTableEntity> listOfItems, int chunkSize)
        {
            while (listOfItems.Any())
            {
                yield return listOfItems.Take(chunkSize);
                listOfItems = listOfItems.Skip(chunkSize);
            }
        }

        protected void WriteStartingToProcessPage(TableQuerySegment<DynamicTableEntity> page, DateTime firstResultTimestamp)
        {
            Console.WriteLine();
            ConsoleHelper.WriteLineWithColor($"Got {page.Count()} results starting at timestamp {firstResultTimestamp}", ConsoleColor.Gray);
        }

        protected void WriteProgressItemQueued()
        {
            ConsoleHelper.WriteWithColor(".", ConsoleColor.Gray);
        }

        protected void WriteProgressItemProcessed()
        {
            ConsoleHelper.WriteWithColor("o", ConsoleColor.DarkGreen);
        }

        protected void VerifyIsInitialized()
        {
            if (!IsInitialized)
            {
                throw new InvalidOperationException("Must call Initialize() before using this method");
            }
        }
    }
}
