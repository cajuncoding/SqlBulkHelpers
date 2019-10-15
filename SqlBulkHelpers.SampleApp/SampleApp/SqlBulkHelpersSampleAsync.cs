using SqlBulkHelpers;
using System;
using System.Data.SqlClient;
using System.Diagnostics;
using System.Threading.Tasks;

namespace Debug.ConsoleApp
{
    public class SqlBulkHelpersSampleAsync
    {
        public static async Task Run()
        {
            ISqlBulkHelpersConnectionProvider sqlConnectionProvider = SqlBulkHelpersConnectionProvider.Default;

            using (var conn = await sqlConnectionProvider.NewConnectionAsync())
            using (SqlTransaction transaction = conn.BeginTransaction())
            {
                var tableName = "__SQL_BULK_HELPERS_TEST";
                ISqlBulkHelper<TestElement> sqlBulkIdentityHelper = new SqlBulkIdentityHelper<TestElement>();

                var timer = new Stopwatch();

                //WARM UP THE CODE and initialize all CACHES!
                timer.Start();
                var testData = SqlBulkHelpersSample.CreateTestData(1);

                await sqlBulkIdentityHelper.BulkInsertOrUpdateAsync(testData, tableName, transaction);
                await sqlBulkIdentityHelper.BulkInsertOrUpdateAsync(testData, tableName, transaction);

                timer.Stop();
                Console.WriteLine($"Warm Up ran in [{timer.ElapsedMilliseconds} ms]...");


                //NOW RUN BENCHMARK LOOPS
                int itemCounter = 0, batchCounter = 1, dataSize = 1000;
                timer.Reset();
                for (; batchCounter < 20; batchCounter++)
                {
                    testData = SqlBulkHelpersSample.CreateTestData(dataSize);

                    timer.Start();
                    await sqlBulkIdentityHelper.BulkInsertAsync(testData, tableName, transaction);
                    timer.Stop();

                    itemCounter += testData.Count;
                }

                transaction.Commit();
                Console.WriteLine($"[{batchCounter}] Bulk Uploads of [{dataSize}] items each, for total of [{itemCounter}], executed in [{timer.ElapsedMilliseconds} ms] at ~[{timer.ElapsedMilliseconds / batchCounter} ms] each!");

                var tableCount = 0;
                using (var sqlCmd = conn.CreateCommand())
                {
                    sqlCmd.CommandText = $"SELECT COUNT(*) FROM [{tableName}]";
                    tableCount = Convert.ToInt32(sqlCmd.ExecuteScalar());
                }
                Console.WriteLine($"[{tableCount}] Total Items in the Table Now!");
                Console.ReadKey();
            }
        }
    }
}
