namespace AzureTablePurger.Services
{
    public class PurgeEntitiesOptions
    {
        public string TargetAccountConnectionString { get; set; }

        public string TargetTableName { get; set; }

        public int PurgeRecordsOlderThanDays { get; set; }

        public string PartitionKeyPrefix { get; set; }
    }
}