using System;
using System.Threading.Tasks;

namespace Debug.ConsoleApp
{
    class Program
    {
        static async Task Main(string[] args)
        {
            try
            {
                //await SqlBulkHelpersSampleAsync.RunBenchmarksAsync();
                await SqlBulkHelpersSampleAsync.RunSampleAsync();

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


