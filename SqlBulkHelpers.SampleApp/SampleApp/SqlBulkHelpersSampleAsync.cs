using SqlBulkHelpers;
using System;
using System.Collections.Generic;
using System.Configuration;
using System.Data.SqlClient;
using System.Diagnostics;
using System.Threading.Tasks;

namespace SqlBulkHelpersSample.ConsoleApp
{
    public class SqlBulkHelpersSampleAsync
    {
        public static async Task RunSampleAsync()
        {
            //Initialize the Sql Connection Provider from Internal Default
            //NOTE: The Default relies on App.config (or Web.config) AppSettings with pre-defined connection string key;
            //          and is most useful for ConsoleApps, etc.
            //ISqlBulkHelpersConnectionProvider sqlConnectionProvider = SqlBulkHelpersConnectionProvider.Default;

            //OR Initialize with a Connection String (using our Config Key or your own, or any other initialization
            //  of the Connection String (e.g. perfect for DI initialization, etc.):
            //NOTE: The ISqlBulkHelpersConnectionProvider interface provides a great abstraction that most projects don't
            //          take the time to do, so it is provided here for convenience (e.g. extremely helpful with DI).
            var sqlConnectionString = ConfigurationManager.AppSettings[SqlBulkHelpersConnectionProvider.SqlConnectionStringConfigKey];
            ISqlBulkHelpersConnectionProvider sqlConnectionProvider = new SqlBulkHelpersConnectionProvider(sqlConnectionString);

            //Initialize large list of Data to Insert or Update in a Table
            List<TestElement> testData = SqlBulkHelpersSample.CreateTestData(1000);

            //Bulk Inserting is now as easy as:
            //  1) Initialize the DB Connection & Transaction (IDisposable)
            //  2) Instantiate the SqlBulkIdentityHelper class with ORM Model Type & Schema Loader instance...
            //  3) Execute the insert/update (e.g. Convenience method allows InsertOrUpdate in one execution!)
            using (SqlConnection conn = await sqlConnectionProvider.NewConnectionAsync())
            using (SqlTransaction transaction = conn.BeginTransaction())
            {
                ISqlBulkHelper<TestElement> sqlBulkIdentityHelper = new SqlBulkIdentityHelper<TestElement>(conn, transaction);

                await sqlBulkIdentityHelper.BulkInsertOrUpdateAsync(
                    testData,
                    SqlBulkHelpersSampleApp.TestTableName,
                    transaction);

                transaction.Commit();
            }
        }

        public static async Task RunBenchmarksAsync()
        {
            ISqlBulkHelpersConnectionProvider sqlConnectionProvider = SqlBulkHelpersConnectionProvider.Default;

            using (var conn = await sqlConnectionProvider.NewConnectionAsync())
            using (SqlTransaction transaction = conn.BeginTransaction())
            {
                var tableName = SqlBulkHelpersSampleApp.TestTableName;

                ISqlBulkHelper<TestElement> sqlBulkIdentityHelper = new SqlBulkIdentityHelper<TestElement>(conn, transaction);

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
                for (; batchCounter <= 20; batchCounter++)
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
