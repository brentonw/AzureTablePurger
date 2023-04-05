using System;

using Microsoft.Azure.Cosmos.Table;

namespace AzureTablePurger.Services
{
    public interface IEntityQueryHandler
    {
        TableQuery GetTableQuery(int purgeEntitiesOlderThanDays);

        TableQuery GetTableQuery(string lowerBoundKey, string upperBoundKey);

        DateTime ConvertKeyToDateTime(DynamicTableEntity entry);


    }
}
