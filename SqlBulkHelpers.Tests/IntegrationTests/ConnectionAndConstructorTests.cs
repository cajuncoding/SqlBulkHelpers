using Microsoft.VisualStudio.TestTools.UnitTesting;
using SqlBulkHelpers.Tests;
using System.Collections.Generic;
using System.Configuration;
using Microsoft.Data.SqlClient;
using System.Linq;
using System.Threading.Tasks;

namespace SqlBulkHelpers.IntegrationTests
{
    [TestClass]
    public class ConnectionAndConstructorTests
    {
        [TestMethod]
        public async Task TestBulkInsertConstructorWithDBSchemaLoaderInstanceDeferred()
        {
            ISqlBulkHelpersConnectionProvider sqlConnectionProvider = SqlConnectionHelper.GetConnectionProvider();
            var sqlBulkDbSchemaLoader = SqlBulkHelpersSchemaLoaderCache.GetSchemaLoader(sqlConnectionProvider);

            using (var conn = await sqlConnectionProvider.NewConnectionAsync())
            using (SqlTransaction transaction = conn.BeginTransaction())
            {
                ISqlBulkHelper<TestElement> sqlBulkIdentityHelper = new SqlBulkIdentityHelper<TestElement>(sqlBulkDbSchemaLoader);

                await DoInsertOrUpdateTestAsync(sqlBulkIdentityHelper, transaction);
            }
        }

        [TestMethod]
        public async Task TestBulkInsertConstructorWithDBSchemaLoaderInstanceFromExistingConnectionAndTransaction()
        {
            ISqlBulkHelpersConnectionProvider sqlConnectionProvider = SqlConnectionHelper.GetConnectionProvider();

            using (var conn = await sqlConnectionProvider.NewConnectionAsync())
            using (SqlTransaction transaction = conn.BeginTransaction())
            {
                //TEST the code flow where the DB Schema Loader is initialized from existing
                //Connection + Transaction and immediately initialized
                var sqlBulkDbSchemaLoader = SqlBulkHelpersSchemaLoaderCache.GetSchemaLoader(conn, transaction, true);

                ISqlBulkHelper<TestElement> sqlBulkIdentityHelper = new SqlBulkIdentityHelper<TestElement>(sqlBulkDbSchemaLoader);

                await DoInsertOrUpdateTestAsync(sqlBulkIdentityHelper, transaction);
            }
        }

        [TestMethod]
        public async Task TestBulkInsertConstructorWithSqlConnectionProvider()
        {
            ISqlBulkHelpersConnectionProvider sqlConnectionProvider = SqlConnectionHelper.GetConnectionProvider();

            using (var conn = await sqlConnectionProvider.NewConnectionAsync())
            using (SqlTransaction transaction = conn.BeginTransaction())
            {
                ISqlBulkHelper<TestElement> sqlBulkIdentityHelper = new SqlBulkIdentityHelper<TestElement>(sqlConnectionProvider);

                await DoInsertOrUpdateTestAsync(sqlBulkIdentityHelper, transaction);
            }
        }

        [TestMethod]
        public async Task TestBulkInsertConstructorWithExistingConnectionOnlyAsync()
        {

            ISqlBulkHelpersConnectionProvider sqlConnectionProvider = SqlConnectionHelper.GetConnectionProvider();

            using (var conn = await sqlConnectionProvider.NewConnectionAsync())
            {
                //NOTE: IN THIS CASE we must initialize BEFORE the transaction is created or an error may occur
                //          when initializing the DB Schema Definitions because we are intentionally not passing
                //          in the Transaction to test this code flow.
                ISqlBulkHelper<TestElement> sqlBulkIdentityHelper = new SqlBulkIdentityHelper<TestElement>(conn);

                using (SqlTransaction transaction = conn.BeginTransaction())
                {

                    await DoInsertOrUpdateTestAsync(sqlBulkIdentityHelper, transaction);
                }
            }
        }

        [TestMethod]
        public async Task TestBulkInsertConstructorWithExistingConnectionAndTransactionAsync()
        {
            ISqlBulkHelpersConnectionProvider sqlConnectionProvider = SqlConnectionHelper.GetConnectionProvider();

            using (var conn = await sqlConnectionProvider.NewConnectionAsync())
            using (SqlTransaction transaction = conn.BeginTransaction())
            {
                ISqlBulkHelper<TestElement> sqlBulkIdentityHelper = new SqlBulkIdentityHelper<TestElement>(conn, transaction);

                await DoInsertOrUpdateTestAsync(sqlBulkIdentityHelper, transaction);
            }
        }

        protected async Task DoInsertOrUpdateTestAsync(ISqlBulkHelper<TestElement> sqlBulkIdentityHelper, SqlTransaction transaction)
        {
            List<TestElement> testData = TestHelpers.CreateTestData(10);

            var results = await sqlBulkIdentityHelper.BulkInsertOrUpdateAsync(
                testData,
                TestHelpers.TestTableName,
                transaction
            );

            transaction?.Commit();

            //ASSERT Results are Valid...
            Assert.IsNotNull(results);

            //We Sort the Results by Identity Id to ensure that the inserts occurred in the correct
            //  order with incrementing ID values as specified in the original Data!
            //This validates that data is inserted as expected for Identity columns so that it can 
            //  be correctly sorted by Incrementing Identity value when Queried (e.g. ORDER BY Id)
            var resultsSorted = results.OrderBy(r => r.Id).ToList();
            Assert.AreEqual(resultsSorted.Count(), testData.Count);

            var i = 0;
            foreach (var result in resultsSorted)
            {
                Assert.IsNotNull(result);
                Assert.IsTrue(result.Id > 0);
                Assert.AreEqual(result.Key, testData[i].Key);
                Assert.AreEqual(result.Value, testData[i].Value);
                i++;
            }
        }
    }
}
