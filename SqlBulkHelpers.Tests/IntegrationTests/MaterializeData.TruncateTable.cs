using System;
using SqlBulkHelpers.Tests;
using SqlBulkHelpers.MaterializedData;
using Microsoft.Data.SqlClient;
using RepoDb;
using RepoDb.Extensions;
using SqlBulkHelpers.SqlBulkHelpers;

namespace SqlBulkHelpers.IntegrationTests
{
    [TestClass]
    public class MaterializeDataTruncateTableTests
    {
        [TestMethod]
        public async Task TestTruncateTableAsync()
        {
            var sqlConnectionProvider = SqlConnectionHelper.GetConnectionProvider();

            await using (var sqlConn = await sqlConnectionProvider.NewConnectionAsync().ConfigureAwait(false))
            await using (var sqlTrans = (SqlTransaction)await sqlConn.BeginTransactionAsync().ConfigureAwait(false))
            {
                //NOTE: the TestChildTableName does not have any FKey constraints so it can be safely truncated!
                var tableNameTerm = TestHelpers.TestChildTableNameFullyQualified.ParseAsTableNameTerm();

                //Ensure at least some data exists...
                await sqlTrans.BulkInsertAsync(TestHelpers.CreateTestData(10, "TEST_TRUNCATION_DotNet6"), tableNameTerm);

                //Get our count BEFORE and Validate we have some data...
                var initialTableCount = await sqlConn.CountAllAsync(tableNameTerm, transaction: sqlTrans).ConfigureAwait(false);
                Assert.IsTrue(initialTableCount > 0);

                //var initialTableCount = await sqlConn.CountAllAsync(tableNameTerm, transaction: sqlTrans).ConfigureAwait(false);
                var tableResults = await sqlTrans.TruncateTableAsync(tableNameTerm);

                //ASSERT Results are Valid...
                Assert.IsNotNull(tableResults);
                Assert.AreEqual(1, tableResults.Length);

                foreach (var resultTable in tableResults)
                {
                    //Get our count AFTER and assert they are valid...
                    var truncatedTableCount = await sqlConn.CountAllAsync(tableName: resultTable, transaction: sqlTrans).ConfigureAwait(false);
                    Assert.AreEqual(0, truncatedTableCount);
                }

                await sqlTrans.RollbackAsync().ConfigureAwait(false);
            }
        }

        [TestMethod]
        public async Task TestTruncateTableFailsConstraintsAsync()
        {
            var sqlConnectionProvider = SqlConnectionHelper.GetConnectionProvider();

            await using (var sqlConn = await sqlConnectionProvider.NewConnectionAsync().ConfigureAwait(false))
            await using (var sqlTrans = (SqlTransaction)await sqlConn.BeginTransactionAsync().ConfigureAwait(false))
            {
                var targetTableNameTerm = TableNameTerm.From(TestHelpers.TestTableNameFullyQualified);
                
                //FIRST Validate that the Truncation with Constrains fails as expected (e.g. has FKey constraints)...
                Exception? failedException = null;
                try
                {
                    //Second attempt the clone again but this time expecting it to now already exist and fail out!
                    await sqlTrans.TruncateTableAsync(targetTableNameTerm);
                }
                catch (Exception exc)
                {
                    failedException = exc;
                }

                Assert.IsNotNull(failedException);
            }
        }

        [TestMethod]
        public async Task TestTruncateTableForceWithConstraintsAsync()
        {
            var sqlConnectionProvider = SqlConnectionHelper.GetConnectionProvider();

            await using (var sqlConn = await sqlConnectionProvider.NewConnectionAsync().ConfigureAwait(false))
            await using (var sqlTrans = (SqlTransaction)await sqlConn.BeginTransactionAsync().ConfigureAwait(false))
            {
                var targetTableNameTerm = TableNameTerm.From(TestHelpers.TestTableNameFullyQualified);

                //Ensure at least some data exists...
                await sqlTrans.BulkInsertAsync(TestHelpers.CreateTestData(10, "TEST_TRUNCATION_DotNet6"), targetTableNameTerm);

                //Get our count BEFORE and Validate we have some data...
                var initialTableCount = await sqlConn.CountAllAsync(targetTableNameTerm, transaction: sqlTrans).ConfigureAwait(false);
                Assert.IsTrue(initialTableCount > 0);

                //NOW we can now test Truncation with support for overriding the constraints...
                var tableResults = await sqlTrans.TruncateTableAsync(targetTableNameTerm, forceOverrideOfConstraints: true);

                Assert.IsNotNull(tableResults);
                Assert.AreEqual(1, tableResults.Length);

                foreach (var resultTable in tableResults)
                {
                    //Get our count AFTER and assert they are valid...
                    var truncatedTableCount = await sqlConn.CountAllAsync(tableName: resultTable, transaction: sqlTrans).ConfigureAwait(false);
                    Assert.AreEqual(0, truncatedTableCount);
                }

                await sqlTrans.RollbackAsync().ConfigureAwait(false);
            }

        }
    }
}
