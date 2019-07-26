using System;
using System.Configuration;
using System.Diagnostics;
using AzureTablePurger.Enums;

namespace AzureTablePurger
{
    /// <summary>
    /// Command line utility to purge data from an Azure table.
    ///
    /// 2 implementations provided:
    /// - simple, synchronous implementation
    /// - asynchronous and parallel processing implementation (currently active)
    ///
    /// The asynchronous implementation is a produce/consumer pattern: one task is querying Azure Table Storage, downloading
    /// data and caching it locally on disk (potentially too much data to deal with in memory - we produce a lot faster than
    /// we're able to consume). Another task is picking up items from the in-memory queue in parallel and processing them.
    /// Processing involves reading the cached data from disk, creating batch operations and dispatching them to Azure Table
    /// Storage.
    ///
    /// Some references:
    /// https://stackoverflow.com/questions/12712117/how-to-post-results-of-parallel-foreach-to-a-queue-which-is-continually-read-in
    /// https://blogs.msdn.microsoft.com/pfxteam/2010/04/06/parallelextensionsextras-tour-4-blockingcollectionextensions/
    /// 
    /// https://stackoverflow.com/questions/154551/volatile-vs-interlocked-vs-lock
    /// https://stackoverflow.com/questions/16170915/best-practice-in-deleting-azure-table-entities-in-foreach-loop
    /// https://stackoverflow.com/questions/3724232/parallel-invoke-exception-handling
    /// </summary>
    class Program
    {
        public const string StorageConnectionConfigKey = "StorageConnectionString";
        public const string TableNameConfigKey = "TableName";
        public const string PurgeRecordsOlderThanDaysConfigKey = "PurgeRecordsOlderThanDays";
        public const string PartitionKeyFormatConfigKey = "PartitionKeyFormat";
        public const string OperationModeConfigKey = "OperationMode";

        static void Main(string[] args)
        {
            var connectionString = GetRequiredConfigSetting(StorageConnectionConfigKey);
            var tableName = GetRequiredConfigSetting(TableNameConfigKey);
            var purgeRecordsOlderThanDays = GetRequiredConfigSettingAsInt(PurgeRecordsOlderThanDaysConfigKey);
            var partitionKeyFormat = GetRequiredConfigSettingAsEnum<PartitionKeyFormat>(PartitionKeyFormatConfigKey);
            var operationMode = GetRequiredConfigSettingAsEnum<OperationMode>(OperationModeConfigKey);

            var partitionKeyHandler = CreatePartitionKeyHandler(partitionKeyFormat);
            var tablePurger = CreateTablePurger(operationMode);
            tablePurger.Initialize(connectionString, tableName, partitionKeyHandler, partitionKeyFormat, purgeRecordsOlderThanDays);

            var sw = new Stopwatch();
            sw.Start();
            ConsoleHelper.WriteLineWithColor("Starting...", ConsoleColor.Green);

            int numEntitiesProcessed = 0;
            int numPartitionsProcessed = 0;

            tablePurger.PurgeEntities(out numEntitiesProcessed, out numPartitionsProcessed);

            sw.Stop();
            var averageTimePerEntityInMs = sw.ElapsedMilliseconds / numEntitiesProcessed;

            Console.WriteLine();
            ConsoleHelper.WriteLineWithColor($"Finished processing {numEntitiesProcessed} entities across {numPartitionsProcessed} partitions in {sw.Elapsed}. Average time per entity {averageTimePerEntityInMs}ms", ConsoleColor.Green);
        }

        private static IPartitionKeyHandler CreatePartitionKeyHandler(PartitionKeyFormat partitionKeyFormat)
        {
            switch (partitionKeyFormat)
            {
                case PartitionKeyFormat.TicksAscendingWithLeadingZero:
                    return new TicksAscendingWithLeadingZeroPartitionKeyHandler();
                default:
                    throw new ArgumentOutOfRangeException(nameof(partitionKeyFormat), partitionKeyFormat, null);
            }
        }

        private static ITablePurger CreateTablePurger(OperationMode operationMode)
        {
            switch (operationMode)
            {
                case OperationMode.Simple:
                    return new SimpleTablePurger();
                case OperationMode.Parallel:
                    return new ParallelTablePurger();
                default:
                    throw new ArgumentOutOfRangeException(nameof(operationMode), operationMode, null);
            }
        }

        private static string GetRequiredConfigSetting(string configKey)
        {
            var setting = ConfigurationManager.AppSettings[configKey];

            if (string.IsNullOrEmpty(setting))
            {
                throw new ConfigurationErrorsException(
                    $"The required configuration setting '{configKey}' is not present in the configuration file");
            }

            return setting;
        }

        private static int GetRequiredConfigSettingAsInt(string configKey)
        {
            var settingAsString = GetRequiredConfigSetting(configKey);

            int result = 0;

            if (!int.TryParse(settingAsString, out result))
            {
                throw new ConfigurationErrorsException($"The required configuration setting '{configKey}' is not a valid int");
            }

            return result;
        }

        private static T GetRequiredConfigSettingAsEnum<T>(string configKey) where T : struct, Enum
        {
            var settingAsString = GetRequiredConfigSetting(configKey);

            T result;

            if (!Enum.TryParse(settingAsString, out result))
            {
                throw new ConfigurationErrorsException($"The required configuration setting '{configKey}' can not be converted to an enum of type '{typeof(T).FullName}'");
            }

            return result;
        }
    }
}