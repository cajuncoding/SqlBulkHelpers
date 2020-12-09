using SqlBulkHelpers;
using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Diagnostics;
using System.Linq;

namespace SqlBulkHelpersSample.ConsoleApp
{
    public class SqlBulkHelpersSampleSynchronous
    {
        public static void RunBenchmarks()
        {
            ISqlBulkHelpersConnectionProvider sqlConnectionProvider = SqlBulkHelpersConnectionProvider.Default;

            using (var conn = sqlConnectionProvider.NewConnection())
            using (SqlTransaction transaction = conn.BeginTransaction())
            {
                var tableName = SqlBulkHelpersSampleApp.TestTableName;

                ISqlBulkHelper<TestElement> sqlBulkIdentityHelper = new SqlBulkIdentityHelper<TestElement>(conn, transaction);

                var timer = new Stopwatch();

                //WARM UP THE CODE and initialize all CACHES!
                timer.Start();
                List<TestElement> testData = SqlBulkHelpersSample.CreateTestData(1);

                sqlBulkIdentityHelper.BulkInsertOrUpdate(testData, tableName, transaction);
                sqlBulkIdentityHelper.BulkInsertOrUpdate(testData, tableName, transaction);

                timer.Stop();
                Console.WriteLine($"Warm Up ran in [{timer.ElapsedMilliseconds} ms]...");


                //NOW RUN BENCHMARK LOOPS
                int itemCounter = 0, batchCounter = 1, dataSize = 1000;
                timer.Reset();
                for (; batchCounter < 20; batchCounter++)
                {
                    testData = SqlBulkHelpersSample.CreateTestData(dataSize);

                    timer.Start();
                    var results = sqlBulkIdentityHelper.BulkInsert(testData, tableName, transaction)?.ToList();
                    timer.Stop();

                    if(results.Count() != dataSize)
                    {
                        Console.WriteLine($"The results count of [{results.Count()}] does not match the expected count of [{dataSize}]!!!");
                    }

                    if (results.Any(t => t.Id <= 0))
                    {
                        Console.WriteLine($"Some items were returned with an invalid Identity Value (e.g. may still be initialized to default value [{default(int)})");
                    }

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
