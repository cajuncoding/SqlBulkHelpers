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
                await SqlBulkHelpersSampleAsync.RunBenchmarks();
                //await SqlBulkHelpersSampleAsync.RunSample();
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


