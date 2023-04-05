using System.Collections.Generic;
using System.IO;
using System.Threading;
using System.Threading.Tasks;

using AzureTablePurger.Services;

using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Console;

namespace AzureTablePurger.App
{
    class Program
    {
        private const string ConfigKeyTargetStorageAccountConnectionString = "TargetStorageAccountConnectionString";
        private const string ConfigKeyTargetTableName = "TargetTableName";
        private const string ConfigKeyPurgeRecordsOlderThanDays = "PurgeRecordsOlderThanDays";

        private static ServiceProvider _serviceProvider;
        private static IConfigurationRoot _config;

        static async Task Main(string[] args)
        {
            BuildConfig(args);
            var serviceCollection = RegisterServices();
            ConfigureLogging(serviceCollection);

            _serviceProvider = serviceCollection.BuildServiceProvider();

            await using (_serviceProvider)
            {
                var logger = _serviceProvider.GetService<ILogger<Program>>();
                logger.LogInformation("Starting...");

                var tablePurger = _serviceProvider.GetService<ITablePurger>();

                var options = new PurgeEntitiesOptions
                {
                    TargetAccountConnectionString = _config[ConfigKeyTargetStorageAccountConnectionString],
                    TargetTableName = _config[ConfigKeyTargetTableName],
                    PurgeRecordsOlderThanDays = int.Parse(_config[ConfigKeyPurgeRecordsOlderThanDays])
                };

                var cts = new CancellationTokenSource();

                await tablePurger.PurgeEntitiesAsync(options, cts.Token);

                logger.LogInformation($"Finished");
            }
        }

        private static void BuildConfig(string[] commandLineArgs)
        {
            var configBuilder = new ConfigurationBuilder()
                .SetBasePath(Directory.GetCurrentDirectory())
                .AddJsonFile("appsettings.json")
                .AddUserSecrets<Program>();

            // Command line config
            var switchMapping = new Dictionary<string, string>
            {
                { "-account", ConfigKeyTargetStorageAccountConnectionString },
                { "-table", ConfigKeyTargetTableName },
                { "-days", ConfigKeyPurgeRecordsOlderThanDays }
            };

            configBuilder.AddCommandLine(commandLineArgs, switchMapping);

            _config = configBuilder.Build();
        }

        private static ServiceCollection RegisterServices()
        {
            var serviceCollection = new ServiceCollection();

            // Core logic
            serviceCollection.AddScoped<ITablePurger, SimpleTablePurger>();
            serviceCollection.AddScoped<IAzureStorageClientFactory, AzureStorageClientFactory>();
            serviceCollection.AddScoped<IEntityQueryHandler, TicksAscendingWithLeadingZeroPartitionKeyHandler>();

            return serviceCollection;
        }

        private static void ConfigureLogging(ServiceCollection serviceCollection)
        {
            serviceCollection.AddLogging(configure =>
                    configure.AddSimpleConsole(options =>
                    {
                        options.SingleLine = true;
                        options.ColorBehavior = LoggerColorBehavior.Default;
                        options.IncludeScopes = false;
                        options.TimestampFormat = "[yyyy-MM-dd HH:mm:ss:fffff tt] ";
                    }))
                .Configure<LoggerFilterOptions>(options => options.MinLevel = LogLevel.Debug);
        }
    }
}