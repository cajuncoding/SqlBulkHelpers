using SqlBulkHelpers;
using System;
using System.Collections.Generic;
using System.Configuration;
using System.Data.SqlClient;
using System.Diagnostics;
using System.Threading.Tasks;

namespace Debug.ConsoleApp
{
    public class SqlBulkHelpersSampleAsync
    {
        public static async Task RunSampleAsync()
        {
            //Initialize the Sql Connection Provider (or manually create your own Sql DB Connection...)
            //NOTE: This interface provides a great abstraction that most projects don't take the time to do, 
            //          so it is provided here for convenience (e.g. extremely helpful with DI).
            ISqlBulkHelpersConnectionProvider sqlConnectionProvider = SqlBulkHelpersConnectionProvider.Default;

            //TODO: Create Unit test for Default vs Custom Sql Connection Providers
            //var sqlConnectionString = ConfigurationManager.AppSettings[SqlBulkHelpersConnectionProvider.SQL_CONNECTION_STRING_CONFIG_KEY];
            //ISqlBulkHelpersConnectionProvider sqlConnectionProvider = new SqlBulkHelpersConnectionProvider(sqlConnectionString);

            //TODO: Create Unit test for Custom DB Schema Loader!
            //var customDbSchemaLoader = new SqlBulkHelpersDBSchemaStaticLoader(new SqlBulkHelpersConnectionProvider("BRANDON FIXED IT!"));

            //Initialize large list of Data to Insert or Update in a Table
            List<TestElement> testData = SqlBulkHelpersSample.CreateTestData(1000);

            //Get the DB Schema Loader (will load from Cache if already initialized).
            //NOTE: This can also be initialized from an existing Connection, via overlaod, but that should be called before
            //      any transactions are opened on the connection.
            //NOTE: This often only has to be done once, and could be kept across many connections, or requests (e.g. static).
            //      or simply leverage this provided in-memory cache here to manage that internally.
            var sqlBulkHelpersSchemaLoader = SqlBulkHelpersSchemaLoaderCache.GetSchemaLoader(sqlConnectionProvider);

            //Bulk Inserting is now as easy as:
            //  1) Initialize the DB Connection & Transaction (IDisposable)
            //  2) Instantiate the SqlBulkIdentityHelper class with ORM Model Type & Schema Loader instance...
            //  3) Execute the insert/update (e.g. Convenience method allows InsertOrUpdate in one execution!)
            using (SqlConnection conn = await sqlConnectionProvider.NewConnectionAsync())
            using (SqlTransaction transaction = conn.BeginTransaction())
            {
                ISqlBulkHelper<TestElement> sqlBulkIdentityHelper = new SqlBulkIdentityHelper<TestElement>(sqlBulkHelpersSchemaLoader);

                await sqlBulkIdentityHelper.BulkInsertOrUpdateAsync(
                    testData, 
                    "SqlBulkHelpersTestElements",
                    transaction);

                transaction.Commit();
            }
        }

        public static async Task RunBenchmarksAsync()
        {
            ISqlBulkHelpersConnectionProvider sqlConnectionProvider = SqlBulkHelpersConnectionProvider.Default;

            var sqlBulkHelpersSchemaLoader = SqlBulkHelpersSchemaLoaderCache.GetSchemaLoader(sqlConnectionProvider);

            using (var conn = await sqlConnectionProvider.NewConnectionAsync())
            using (SqlTransaction transaction = conn.BeginTransaction())
            {
                var tableName = "SqlBulkHelpersTestElements";
                ISqlBulkHelper<TestElement> sqlBulkIdentityHelper = new SqlBulkIdentityHelper<TestElement>(sqlBulkHelpersSchemaLoader);

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
