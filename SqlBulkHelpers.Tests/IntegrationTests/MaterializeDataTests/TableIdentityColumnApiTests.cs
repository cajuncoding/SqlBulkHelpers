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
                // ReSharper disable once MethodHasAsyncOverload
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
                // ReSharper disable once MethodHasAsyncOverload
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
                // ReSharper disable once MethodHasAsyncOverload
                var syncCurrentIdentityValue = sqlTrans.GetTableCurrentIdentityValue(TestHelpers.TestTableName);
                Assert.AreEqual(firstNewIdentitySeedValue, syncCurrentIdentityValue);

                await sqlTrans.ReSeedTableIdentityValueAsync(TestHelpers.TestTableName, secondNewIdentitySeedValue).ConfigureAwait(false);
                // ReSharper disable once MethodHasAsyncOverload
                var syncUpdatedIdentityValue = sqlTrans.GetTableCurrentIdentityValue(TestHelpers.TestTableName);
                Assert.AreEqual(syncUpdatedIdentityValue, secondNewIdentitySeedValue);
                Assert.AreNotEqual(syncUpdatedIdentityValue, initialIdentitySeedValue);

                await sqlTrans.ReSeedTableIdentityValueAsync(TestHelpers.TestTableName, initialIdentitySeedValue).ConfigureAwait(false);
                sqlTrans.Commit();
            }
        }

        [TestMethod]
        public async Task TestReSeedTableIdentityValueWithMaxIdFromSqlTransactionSyncAndAsync()
        {
            var sqlConnectionString = SqlConnectionHelper.GetSqlConnectionString();
            ISqlBulkHelpersConnectionProvider sqlConnectionProvider = new SqlBulkHelpersConnectionProvider(sqlConnectionString);

            var firstNewIdentitySeedValue = 555888;
            var secondNewIdentitySeedValue = 888444;

            await using (var sqlConn = await sqlConnectionProvider.NewConnectionAsync().ConfigureAwait(false))
            await using (var sqlTrans = (SqlTransaction)await sqlConn.BeginTransactionAsync().ConfigureAwait(false))
            {
                await sqlTrans.ReSeedTableIdentityValueAsync(TestHelpers.TestTableName, firstNewIdentitySeedValue).ConfigureAwait(false);
                var asyncInitialIdentityValue = await sqlTrans.GetTableCurrentIdentityValueAsync(TestHelpers.TestTableName).ConfigureAwait(false);
                Assert.AreEqual(firstNewIdentitySeedValue, asyncInitialIdentityValue);

                var maxId = await sqlTrans.ReSeedTableIdentityValueWithMaxIdAsync(TestHelpers.TestTableName);
                Assert.IsTrue(maxId > 0);

                var asyncMaxIdIdentityValue = await sqlTrans.GetTableCurrentIdentityValueAsync(TestHelpers.TestTableName).ConfigureAwait(false);
                Assert.AreEqual(maxId, asyncMaxIdIdentityValue);
                Assert.AreNotEqual(asyncInitialIdentityValue, asyncMaxIdIdentityValue);

                await sqlTrans.CommitAsync().ConfigureAwait(false);
            }

            await using (var sqlConn = await sqlConnectionProvider.NewConnectionAsync().ConfigureAwait(false))
            await using (var sqlTrans = (SqlTransaction)await sqlConn.BeginTransactionAsync().ConfigureAwait(false))
            {
                // ReSharper disable once MethodHasAsyncOverload
                sqlTrans.ReSeedTableIdentityValue(TestHelpers.TestTableName, secondNewIdentitySeedValue);
                // ReSharper disable once MethodHasAsyncOverload
                var syncInitialIdentityValue = sqlTrans.GetTableCurrentIdentityValue(TestHelpers.TestTableName);
                Assert.AreEqual(secondNewIdentitySeedValue, syncInitialIdentityValue);

                // ReSharper disable once MethodHasAsyncOverload
                var maxId = sqlTrans.ReSeedTableIdentityValueWithMaxId(TestHelpers.TestTableName);
                Assert.IsTrue(maxId > 0);

                // ReSharper disable once MethodHasAsyncOverload
                var syncMaxIdIdentityValue = sqlTrans.GetTableCurrentIdentityValue(TestHelpers.TestTableName);
                Assert.AreEqual(maxId, syncMaxIdIdentityValue);
                Assert.AreNotEqual(syncInitialIdentityValue, syncMaxIdIdentityValue);

                sqlTrans.Commit();
            }
        }
    }
}
