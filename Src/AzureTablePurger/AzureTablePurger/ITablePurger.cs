using AzureTablePurger.Enums;

namespace AzureTablePurger
{
    interface ITablePurger
    {
        bool IsInitialized { get; }

        void Initialize(string storageAccountConnectionString, string tableName,
            IPartitionKeyHandler partitionKeyHandler, PartitionKeyFormat partitionKeyFormat,
            int purgeEntitiesOlderThanDays);

        void PurgeEntities(out int numEntitiesProcessed, out int numPartitionsProcessed);
    }
}
