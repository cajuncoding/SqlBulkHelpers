using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.IO;
using System.Text.RegularExpressions;

namespace Debug.ConsoleApp
{
    class Program
    {
        static async Task Main(string[] args)
        {
            try
            {
                await SqlBulkHelpersSample.Run();
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


