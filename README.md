# AzureTablePurger
A command line utility to delete old data (eg logging data) from an Azure Storage Table.

## Note
This relies on the target table having its `PartitionKey` formatted in a specific way, namely ascending ticks with a leading 0, eg `0636738338530008778`. This is the default format for Serilog, Windows Azure Diagnostic Logs (eg `WADDiagnosticsInfrastructureLogsTable`, `WADLogsTable`, `WADPerformanceCountersTable`, `WADWindowsEventLogsTable`) and other logging packages.

## Background
For more info on the background of this tools, see: [How to Delete Data From An Old Azure Storage Table Part 1](https://brentonwebster.com/blog/deleting-stuff-from-an-azure-table-part-1) and [Part 2](https://brentonwebster.com/blog/how-to-delete-old-data-from-an-azure-storage-table-part-2).

## Usage
Example: `.\AzureTablePurger.App.exe -account "[YourStorageAccountConnectionString]" -table "[YourTableName]" -days 365`

This will delete all records in `[YourTableName]` under the storage account identified by `[YourStorageAccountConnectionString]` that are older than `365` days.

You can interrupt this process at any time and restart it, it will essentially just pick up where it left off and keep going.

### Configuration
You can specify configuration in 3 ways:

1. Via the command line:

`-account [storage account connection string]`: the connection string for the account you want to target
`-table [name of table]`: the name of the table inside the storage account
`-days [number of days]`: delete all records that are older than this amount of days. Defaults to 365

2. Via config in your `appsettings.json` file

3. Via config in your local user `secrets.json` file
If you're storing your storage account connection string, you're better off storing it in your user secrets file instead of your `appsettings.json` file.

## Performance
While you can run this from your local machine, for best performance, run this from a VM that is deployed in the same region as your Azure storage account.

As an example, I was getting deletion throughput of around 80-90 entities per second when running from my local machine, vs 2000+ entities per second when running from a VM deploying in the same region as my Azure Storage account.
