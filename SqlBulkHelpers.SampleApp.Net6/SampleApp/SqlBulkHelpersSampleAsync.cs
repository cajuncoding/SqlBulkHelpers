using SqlBulkHelpers;
using System;
using System.Collections.Generic;
using Microsoft.Data.SqlClient;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;
using SqlBulkHelpers.SqlBulkHelpers;

namespace SqlBulkHelpersSample.ConsoleApp
{
    public class SqlBulkHelpersSampleAsync
    {
        public static async Task RunSampleAsync(string sqlConnectionString)
        {
            //Initialize Sql Bulk Helpers Configuration Defaults...
            SqlBulkHelpersConfig.ConfigureDefaults(config =>
            {
                config.SqlBulkPerBatchTimeoutSeconds = SqlBulkHelpersSampleApp.SqlTimeoutSeconds;
            });

            //Initialize with a Connection String (using our Config Key or your own, or any other initialization
            //  of the Connection String (e.g. perfect for DI initialization, etc.):
            //NOTE: The ISqlBulkHelpersConnectionProvider interface provides a great abstraction that most projects don't
            //          take the time to do, so it is provided here for convenience (e.g. extremely helpful with DI).
            ISqlBulkHelpersConnectionProvider sqlConnectionProvider = new SqlBulkHelpersConnectionProvider(sqlConnectionString);

            //Initialize large list of Data to Insert or Update in a Table
            var testData = SqlBulkHelpersSample.CreateTestData(1000);
            var timer = Stopwatch.StartNew();

            //Bulk Inserting is now as easy as:
            //  1) Initialize the DB Connection & Transaction (IDisposable)
            //  2) Execute the insert/update (e.g. new Extension Method API greatly simplifies this and allows InsertOrUpdate in one execution!)
            //  3) Map the results to Child Data and then repeat to create related Child data!
            await using SqlConnection sqlConn = await sqlConnectionProvider.NewConnectionAsync().ConfigureAwait(false);
            await using SqlTransaction sqlTrans = (SqlTransaction)await sqlConn.BeginTransactionAsync().ConfigureAwait(false);

            //Test using manual Table name provided...
            var results = (await sqlTrans.BulkInsertOrUpdateAsync(testData, tableName: SqlBulkHelpersSampleApp.TestTableName).ConfigureAwait(false)).ToList();

            //Test using Table Name derived from Model Annotation [SqlBulkTable(...)]
            var childTestData = SqlBulkHelpersSample.CreateChildTestData(results);
            var childResults = await sqlTrans.BulkInsertOrUpdateAsync(childTestData).ConfigureAwait(false);

            await sqlTrans.CommitAsync();

            timer.Stop();
            Console.WriteLine($"Successfully Inserted or Updated [{testData.Count}] items and [{childTestData.Count}] related child items in [{timer.ElapsedMilliseconds}] millis!");
        }
    }
}
