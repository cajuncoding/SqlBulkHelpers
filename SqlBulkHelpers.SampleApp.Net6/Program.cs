using System;
using System.Threading.Tasks;
using SqlBulkHelpersSample.ConsoleApp;

namespace SqlBulkHelpers.SampleApp.NetCore
{
    class Program
    {
        static async Task Main(string[] args)
        {
            try
            {
                Console.WriteLine("Starting Sample .NetCore Console App process...");

                var sqlConnectionString = Environment.GetEnvironmentVariable(SqlBulkHelpersConnectionProvider.SqlConnectionStringConfigKey);

                await SqlBulkHelpersSampleAsync.RunSampleAsync(sqlConnectionString).ConfigureAwait(false);

                Console.WriteLine("Process Finished Successfully (e.g. without Error)!");
                Console.ReadKey();
            }
            catch (Exception exc)
            {
                Console.WriteLine(exc.Message);
                Console.WriteLine(exc.StackTrace);
                Console.ReadKey();
            }
        }
    }
}
