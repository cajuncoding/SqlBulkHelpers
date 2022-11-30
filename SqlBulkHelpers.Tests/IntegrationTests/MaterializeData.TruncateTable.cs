using System;
using SqlBulkHelpers.Tests;
using SqlBulkHelpers.MaterializedData;
using Microsoft.Data.SqlClient;

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
            await using (var sqlTransaction = (SqlTransaction)await sqlConn.BeginTransactionAsync().ConfigureAwait(false))
            {
                var tableResults = await sqlTransaction.TruncateTableAsync(TestHelpers.TestTableNameFullyQualified);

                await sqlTransaction.RollbackAsync().ConfigureAwait(false);

                //ASSERT Results are Valid...
                Assert.IsNotNull(tableResults);
                Assert.AreEqual(1, tableResults.Length);
            }
        }

        [TestMethod]
        public async Task TestTruncateTableFailsConstraintsAsync()
        {
            //var sqlConnectionProvider = SqlConnectionHelper.GetConnectionProvider();

            //using (var sqlConn = await sqlConnectionProvider.NewConnectionAsync().ConfigureAwait(false))
            //using (SqlTransaction sqlTransaction = sqlConn.BeginTransaction().ConfigureAwait(false))
            //{
            //    var targetTableNameTerm = TableNameTerm.From(TestHelpers.MaterializeDataLoadingSchema, TestHelpers.TestTableName);
            //    //First Clone and force re-creation to make this Test Idempotent!
            //    var successfulCloneInfo = await sqlTransaction.CloneTableAsync(TestHelpers.TestTableNameFullyQualified, targetTableNameTerm, recreateIfExists: true);

            //    CloneTableInfo? failedCloneInfo = null;
            //    Exception failedToCloneException = null;
            //    try
            //    {
            //        //Second attempt the clone again but this time expecting it to now already exist and fail out!
            //        failedCloneInfo = await sqlTransaction.CloneTableAsync(TestHelpers.TestTableNameFullyQualified, targetTableNameTerm, recreateIfExists: false);
            //    }
            //    catch (Exception cloneExc)
            //    {
            //        failedToCloneException = cloneExc;
            //    }

            //    //Now Clean up the Cloned Table...
            //    await sqlTransaction.DropTableAsync(successfulCloneInfo.TargetTable).ConfigureAwait(false);

            //    //ASSERT Results are Valid...
            //    Assert.IsNotNull(successfulCloneInfo);
            //    Assert.IsNotNull(failedToCloneException);
            //    Assert.IsNull(failedCloneInfo);
            //}
        }

        [TestMethod]
        public async Task TestTruncateTableForceWithConstraintsAsync()
        {
        }
    }
}
