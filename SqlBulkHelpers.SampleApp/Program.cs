using System;
using System.Configuration;
using System.Threading.Tasks;
using SqlBulkHelpers;

namespace SqlBulkHelpersSample.ConsoleApp
{
    class Program
    {
        static async Task Main(string[] args)
        {
            try
            {
                Console.WriteLine("Staring Sample .Net Console App process...");

                var sqlConnectionString = ConfigurationManager.AppSettings[SqlBulkHelpersConnectionProvider.SqlConnectionStringConfigKey];

                //await SqlBulkHelpersSampleAsync.RunBenchmarksAsync(sqlConnectionString);
                await SqlBulkHelpersSampleAsync.RunSampleAsync(sqlConnectionString);

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

        //static void Main(string[] args)
        //{
        //    try
        //    {
        //        SqlBulkHelpersSampleSynchronous.Run();
        //    }
        //    catch (Exception exc)
        //    {
        //        Console.WriteLine(exc.Message);
        //        Console.WriteLine(exc.StackTrace);
        //        Console.ReadKey();
        //    }
        //}
    }
}


