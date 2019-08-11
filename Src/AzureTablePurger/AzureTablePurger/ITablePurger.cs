namespace AzureTablePurger
{
    public interface ITablePurger
    {
        void Initialize(string storageAccountConnectionString, string tableName, IPartitionKeyHandler partitionKeyHandler, int purgeEntitiesOlderThanDays);

        void PurgeEntities(out int numEntitiesProcessed, out int numPartitionsProcessed);
    }
}
