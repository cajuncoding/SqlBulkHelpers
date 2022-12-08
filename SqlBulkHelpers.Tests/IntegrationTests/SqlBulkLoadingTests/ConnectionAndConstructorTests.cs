using SqlBulkHelpers.Tests;
using Microsoft.Data.SqlClient;
using SqlBulkHelpers.SqlBulkHelpers;

namespace SqlBulkHelpers.IntegrationTests
{
    [TestClass]
    public class SqlBulkHelpersConnectionAndConstructorTests : BaseTest
    {
        [TestMethod]
        public async Task TestBulkInsertConstructorWithDBSchemaLoaderInstanceDeferred()
        {
            ISqlBulkHelpersConnectionProvider sqlConnectionProvider = SqlConnectionHelper.GetConnectionProvider();
            var sqlBulkDbSchemaLoader = SqlBulkHelpersSchemaLoaderCache.GetSchemaLoader(sqlConnectionProvider);

            await using (var sqlConn = await sqlConnectionProvider.NewConnectionAsync().ConfigureAwait(false))
            await using (var sqlTrans = (SqlTransaction) await sqlConn.BeginTransactionAsync().ConfigureAwait(false))
            {
                ISqlBulkHelper<TestElement> sqlBulkIdentityHelper = new SqlBulkHelper<TestElement>(sqlBulkDbSchemaLoader);

                await DoInsertOrUpdateTestAsync(sqlBulkIdentityHelper, sqlTrans);
            }
        }

        [TestMethod]
        public async Task TestBulkInsertConstructorWithDBSchemaLoaderInstanceFromExistingConnectionAndTransaction()
        {
            ISqlBulkHelpersConnectionProvider sqlConnectionProvider = SqlConnectionHelper.GetConnectionProvider();

            await using (var sqlConn = await sqlConnectionProvider.NewConnectionAsync().ConfigureAwait(false))
            await using (var sqlTrans = (SqlTransaction)await sqlConn.BeginTransactionAsync().ConfigureAwait(false))
            {
                //TEST the code flow where the DB Schema Loader is initialized from existing
                //Connection + sqlTrans and immediately initialized
                var sqlBulkDbSchemaLoader = SqlBulkHelpersSchemaLoaderCache.GetSchemaLoader(sqlConn.ConnectionString);

                ISqlBulkHelper<TestElement> sqlBulkIdentityHelper = new SqlBulkHelper<TestElement>(sqlBulkDbSchemaLoader);

                await DoInsertOrUpdateTestAsync(sqlBulkIdentityHelper, sqlTrans).ConfigureAwait(false);
            }
        }

        [TestMethod]
        public async Task TestBulkInsertConstructorWithSqlConnectionProvider()
        {
            ISqlBulkHelpersConnectionProvider sqlConnectionProvider = SqlConnectionHelper.GetConnectionProvider();

            await using (var sqlConn = await sqlConnectionProvider.NewConnectionAsync().ConfigureAwait(false))
            await using (var sqlTrans = (SqlTransaction)await sqlConn.BeginTransactionAsync().ConfigureAwait(false))
            {
                ISqlBulkHelper<TestElement> sqlBulkIdentityHelper = new SqlBulkHelper<TestElement>(sqlConnectionProvider);

                await DoInsertOrUpdateTestAsync(sqlBulkIdentityHelper, sqlTrans).ConfigureAwait(false);
            }
        }

        [TestMethod]
        public async Task TestBulkInsertConstructorWithExistingConnectionAndTransactionAsync()
        {
            ISqlBulkHelpersConnectionProvider sqlConnectionProvider = SqlConnectionHelper.GetConnectionProvider();

            await using (var sqlConn = await sqlConnectionProvider.NewConnectionAsync().ConfigureAwait(false))
            await using (var sqlTrans = (SqlTransaction)await sqlConn.BeginTransactionAsync().ConfigureAwait(false))
            {
                ISqlBulkHelper<TestElement> sqlBulkIdentityHelper = new SqlBulkHelper<TestElement>(sqlTrans);

                await DoInsertOrUpdateTestAsync(sqlBulkIdentityHelper, sqlTrans).ConfigureAwait(false);
            }
        }

        protected async Task DoInsertOrUpdateTestAsync(ISqlBulkHelper<TestElement> sqlBulkIdentityHelper, SqlTransaction sqlTrans)
        {
            List<TestElement> testData = TestHelpers.CreateTestData(10);

            var results = (await sqlTrans.BulkInsertOrUpdateAsync(
                testData,
                TestHelpers.TestTableName
            ).ConfigureAwait(false)).ToList();

            //Test Inserting of Child Data with Table Name derived from Model Annotation, and FKey constraints to the Parents...
            var childTestData = TestHelpers.CreateChildTestData(results);
            var childResults = await sqlTrans.BulkInsertOrUpdateAsync(childTestData).ConfigureAwait(false);

            await sqlTrans.CommitAsync().ConfigureAwait(false);

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
