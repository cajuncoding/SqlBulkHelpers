using System;
using SqlBulkHelpers.Tests;
using Microsoft.Data.SqlClient;
using SqlBulkHelpers.MaterializedData;

namespace SqlBulkHelpers.Tests.IntegrationTests.MaterializeDataTests
{
    [TestClass]
    public class BulkHelpersMetadataMethodTests : BaseTest
    {
        [TestMethod]
        public async Task TestGetTableCurrentIdentityValueFromSqlTransactionSyncAndAsync()
        {
            var sqlConnectionString = SqlConnectionHelper.GetSqlConnectionString();
            ISqlBulkHelpersConnectionProvider sqlConnectionProvider = new SqlBulkHelpersConnectionProvider(sqlConnectionString);

            long asyncCurrentIdentityValue = -1;
            long syncCurrentIdentityValue = -1;

            await using (var sqlConn = await sqlConnectionProvider.NewConnectionAsync().ConfigureAwait(false))
            await using (var sqlTrans = (SqlTransaction)await sqlConn.BeginTransactionAsync().ConfigureAwait(false))
            {
                asyncCurrentIdentityValue = await sqlTrans.GetTableCurrentIdentityValueAsync(TestHelpers.TestTableName).ConfigureAwait(false);
            }

            await using (var sqlConn = await sqlConnectionProvider.NewConnectionAsync().ConfigureAwait(false))
            await using (var sqlTrans = (SqlTransaction)await sqlConn.BeginTransactionAsync().ConfigureAwait(false))
            {
                syncCurrentIdentityValue = sqlTrans.GetTableCurrentIdentityValue(TestHelpers.TestTableName);
            }

            Assert.IsTrue(asyncCurrentIdentityValue > 0);
            Assert.IsTrue(syncCurrentIdentityValue > 0);
            Assert.AreEqual(asyncCurrentIdentityValue, syncCurrentIdentityValue);
        }

        [TestMethod]
        public async Task TestGetTableCurrentIdentityValueFromSqlConnectionSyncAndAsync()
        {
            var sqlConnectionString = SqlConnectionHelper.GetSqlConnectionString();
            ISqlBulkHelpersConnectionProvider sqlConnectionProvider = new SqlBulkHelpersConnectionProvider(sqlConnectionString);

            long asyncCurrentIdentityValueFromTrans = -1;
            long syncCurrentIdentityValueFromConn = -1;

            await using (var sqlConn = await sqlConnectionProvider.NewConnectionAsync().ConfigureAwait(false))
            {
                asyncCurrentIdentityValueFromTrans = await sqlConn.GetTableCurrentIdentityValueAsync(TestHelpers.TestTableName).ConfigureAwait(false);
            }

            await using (var sqlConn = await sqlConnectionProvider.NewConnectionAsync().ConfigureAwait(false))
            {
                syncCurrentIdentityValueFromConn = sqlConn.GetTableCurrentIdentityValue(TestHelpers.TestTableName);
            }

            Assert.IsTrue(asyncCurrentIdentityValueFromTrans > 0);
            Assert.IsTrue(syncCurrentIdentityValueFromConn > 0);
            Assert.AreEqual(asyncCurrentIdentityValueFromTrans, syncCurrentIdentityValueFromConn);
        }

        [TestMethod]
        public async Task TestReSeedTableIdentityValueFromSqlTransactionSyncAndAsync()
        {
            var sqlConnectionString = SqlConnectionHelper.GetSqlConnectionString();
            ISqlBulkHelpersConnectionProvider sqlConnectionProvider = new SqlBulkHelpersConnectionProvider(sqlConnectionString);

            long initialIdentitySeedValue = 0;
            var firstNewIdentitySeedValue = 777888;
            var secondNewIdentitySeedValue = 888999;

            await using (var sqlConn = await sqlConnectionProvider.NewConnectionAsync().ConfigureAwait(false))
            await using (var sqlTrans = (SqlTransaction)await sqlConn.BeginTransactionAsync().ConfigureAwait(false))
            {
                initialIdentitySeedValue = await sqlTrans.GetTableCurrentIdentityValueAsync(TestHelpers.TestTableName).ConfigureAwait(false);
                Assert.IsTrue(initialIdentitySeedValue > 0);
                Assert.AreNotEqual(initialIdentitySeedValue, firstNewIdentitySeedValue);

                await sqlTrans.ReSeedTableIdentityValueAsync(TestHelpers.TestTableName, firstNewIdentitySeedValue).ConfigureAwait(false);
                var asyncUpdatedIdentityValue = await sqlTrans.GetTableCurrentIdentityValueAsync(TestHelpers.TestTableName).ConfigureAwait(false);
                Assert.AreEqual(asyncUpdatedIdentityValue, firstNewIdentitySeedValue);
                Assert.AreNotEqual(initialIdentitySeedValue, asyncUpdatedIdentityValue);

                await sqlTrans.CommitAsync().ConfigureAwait(false);
            }

            await using (var sqlConn = await sqlConnectionProvider.NewConnectionAsync().ConfigureAwait(false))
            await using (var sqlTrans = (SqlTransaction)await sqlConn.BeginTransactionAsync().ConfigureAwait(false))
            {
                var syncCurrentIdentityValue = sqlTrans.GetTableCurrentIdentityValue(TestHelpers.TestTableName);
                Assert.AreEqual(firstNewIdentitySeedValue, syncCurrentIdentityValue);

                await sqlTrans.ReSeedTableIdentityValueAsync(TestHelpers.TestTableName, secondNewIdentitySeedValue).ConfigureAwait(false);
                var syncUpdatedIdentityValue = sqlTrans.GetTableCurrentIdentityValue(TestHelpers.TestTableName);
                Assert.AreEqual(syncUpdatedIdentityValue, secondNewIdentitySeedValue);
                Assert.AreNotEqual(syncUpdatedIdentityValue, initialIdentitySeedValue);

                await sqlTrans.ReSeedTableIdentityValueAsync(TestHelpers.TestTableName, initialIdentitySeedValue).ConfigureAwait(false);
                sqlTrans.Commit();
            }
        }
    }
}
