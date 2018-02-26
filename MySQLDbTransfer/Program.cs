using System;
using System.IO;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;

namespace MySQLDbTransfer
{
    class Program
    {
        static void Main(string[] args)
        {
            var services = new ServiceCollection();
            ConfigureServices(services);
            var provider = services.BuildServiceProvider();
            Console.WriteLine("** Welcome to Data Transfer tool **");
            Console.WriteLine("** Please select one of the following option **");
            Console.WriteLine("** [1] - Enter 1 for extracting data from SQL Server Data **");
            Console.WriteLine("** [2] - Enter 2 for inserting data in MySQL Database **");
            var input = Console.ReadLine();
            try
            {
                switch (input)
                {
                    case "1":
                        var deService = provider.GetRequiredService<DataExtractor>();
                        deService.ExtractData();
                        break;
                    case "2":
                        var mySQLService = provider.GetRequiredService<MySQLDataExport>();
                        mySQLService.ExportData();
                        break;
                    default:
                        break;
                }
                Console.WriteLine("** Process Completed.. Press any key to exit. **");
            }
            catch (Exception ex)
            {
                Console.WriteLine("** Exception Occured.. **");
                Console.WriteLine(ex.ToString());
            }
            Console.ReadLine();
        }

        private static void ConfigureServices(IServiceCollection serviceCollection)
        {
            serviceCollection.AddTransient<DataExtractor>();
            serviceCollection.AddTransient<MySQLDataExport>();
            var configuration = new ConfigurationBuilder()
                .SetBasePath(Directory.GetCurrentDirectory())
                .AddJsonFile("appsetting.json", false)
                .Build();

            serviceCollection.AddSingleton<IConfigurationRoot>(configuration);
        }
    }
}
