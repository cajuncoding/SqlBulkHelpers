using SqlBulkHelpers;
using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Diagnostics;
using System.Threading.Tasks;

namespace Debug.ConsoleApp
{
    public class SqlBulkHelpersSampleAsync
    {
        public static async Task RunSample()
        {
            //Initialize the Sql Connection Provider (or manually create your own Sql DB Connection...)
            //NOTE: This interface provides a great abstraction that most projects don't take the time to do, 
            //          so it is provided here for convenience (e.g. extremely helpful with DI).
            ISqlBulkHelpersConnectionProvider sqlConnectionProvider = SqlBulkHelpersConnectionProvider.Default;

            //Initialize large list of Data to Insert or Update in a Table
            List<TestElement> testData = SqlBulkHelpersSample.CreateTestData(1000);

            //Bulk Inserting is now as easy as:
            //  1) Initialize the DB Connection & Transaction (IDisposable)
            //  2) Instantiate the SqlBulkIdentityHelper class with ORM Model Type...
            //  3) Execute the insert/update (e.g. Convenience method allows InsertOrUpdate in one execution!)
            using (var conn = await sqlConnectionProvider.NewConnectionAsync())
            using (SqlTransaction transaction = conn.BeginTransaction())
            {
                ISqlBulkHelper<TestElement> sqlBulkIdentityHelper = new SqlBulkIdentityHelper<TestElement>();

                await sqlBulkIdentityHelper.BulkInsertOrUpdateAsync(testData, "TestTableName", transaction);

                transaction.Commit();
            }
        }

        public static async Task RunBenchmarks()
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
