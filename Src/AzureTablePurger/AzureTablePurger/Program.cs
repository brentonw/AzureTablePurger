using System;
using System.Configuration;
using System.Diagnostics;
using AzureTablePurger.Enums;
using Serilog;

namespace AzureTablePurger
{
    /// <summary>
    /// Command line utility to purge data from an Azure table.
    ///
    /// Two implementations provided:
    /// - simple, synchronous implementation
    /// - parallel processing implementation (default)
    /// </summary>
    class Program
    {
        public const string StorageConnectionConfigKey = "StorageConnectionString";
        public const string TableNameConfigKey = "TableName";
        public const string PurgeRecordsOlderThanDaysConfigKey = "PurgeRecordsOlderThanDays";
        public const string PartitionKeyFormatConfigKey = "PartitionKeyFormat";
        public const string OperationModeConfigKey = "OperationMode";

        private static ILogger _logger;

        static void Main(string[] args)
        {
            InitializeLogger();

            var connectionString = GetRequiredConfigSetting(StorageConnectionConfigKey);
            var tableName = GetRequiredConfigSetting(TableNameConfigKey);
            var purgeRecordsOlderThanDays = GetRequiredConfigSettingAsInt(PurgeRecordsOlderThanDaysConfigKey);
            var partitionKeyFormat = GetRequiredConfigSettingAsEnum<PartitionKeyFormat>(PartitionKeyFormatConfigKey);
            var operationMode = GetRequiredConfigSettingAsEnum<OperationMode>(OperationModeConfigKey);

            var partitionKeyHandler = CreatePartitionKeyHandler(partitionKeyFormat);
            var tablePurger = CreateTablePurger(operationMode);
            tablePurger.Initialize(connectionString, tableName, partitionKeyHandler, purgeRecordsOlderThanDays);

            var sw = new Stopwatch();
            sw.Start();
            _logger.Information("Starting: tableName={tableName}, purgeRecordsOlderThanDays={purgeRecordsOlderThanDays}, operationMode={operationMode}...", tableName, purgeRecordsOlderThanDays, operationMode);

            int numEntitiesProcessed = 0;
            int numPartitionsProcessed = 0;

            tablePurger.PurgeEntities(out numEntitiesProcessed, out numPartitionsProcessed);

            sw.Stop();
            double averageTimePerEntityInMs = sw.ElapsedMilliseconds / (double)numEntitiesProcessed;

            _logger.Information($"Finished processing {numEntitiesProcessed} entities across {numPartitionsProcessed} partitions in {sw.Elapsed}. Average time per entity {averageTimePerEntityInMs:0.##}ms");
            Console.ResetColor();
            Console.WriteLine("Press any key to exit");
            Console.ReadKey();
        }

        private static void InitializeLogger()
        {
            Log.Logger = new LoggerConfiguration()
                .WriteTo.Console()
                .CreateLogger();

            _logger = Log.Logger;
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
                    return new SimpleTablePurger(_logger);
                case OperationMode.Parallel:
                    return new ParallelTablePurger(_logger);
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