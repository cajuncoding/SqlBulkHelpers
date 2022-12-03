using Microsoft.VisualStudio.TestTools.UnitTesting;
using SqlBulkHelpers.Tests;
using System.Collections.Generic;
using System.Configuration;
using Microsoft.Data.SqlClient;
using System.Linq;
using System.Threading.Tasks;
using SqlBulkHelpers.SqlBulkHelpers;

namespace SqlBulkHelpers.IntegrationTests
{
    [TestClass]
    public class BulkInsertTests
    {
        [TestMethod]
        public async Task TestBulkInsertResultSortOrderAsync()
        {
            var testData = TestHelpers.CreateTestData(10);

            var sqlConnectionString = SqlConnectionHelper.GetSqlConnectionString();
            ISqlBulkHelpersConnectionProvider sqlConnectionProvider = new SqlBulkHelpersConnectionProvider(sqlConnectionString);

            using (var sqlConn = await sqlConnectionProvider.NewConnectionAsync())
            using (var sqlTrans = (SqlTransaction)await sqlConn.BeginTransactionAsync().ConfigureAwait(false))
            {
                //Test with Table name being provided...
                var results = (await sqlTrans.BulkInsertAsync(testData, tableName: TestHelpers.TestTableName).ConfigureAwait(false)).ToList();

                //Test Child Data with Table name being derived from Model Annotation...
                var childTestData = TestHelpers.CreateChildTestData(results);
                var childResults = await sqlTrans.BulkInsertOrUpdateAsync(childTestData).ConfigureAwait(false);

                await sqlTrans.CommitAsync().ConfigureAwait(false);

                //ASSERT Results are Valid...
                Assert.IsNotNull(results);

                //We Sort the Results by Identity Id to ensure that the inserts occurred in the correct
                //  ordinal order matching our Array of original values, but now with incrementing ID values!
                //This validates that data is inserted as expected for Identity columns and is validated
                //  correctly by sorting on the Incrementing Identity value when Queried (e.g. ORDER BY Id)
                //  which must then match our original order of data.
                var resultsSorted = results.OrderBy(r => r.Id).ToList();
                Assert.AreEqual(resultsSorted.Count(), testData.Count);

                var i = 0;
                foreach (var result in resultsSorted)
                {
                    Assert.IsNotNull(result);
                    Assert.IsTrue(result.Id > 0);
                    Assert.AreEqual(testData[i].Key, result.Key);
                    Assert.AreEqual(testData[i].Value, result.Value);
                    i++;
                }

                //The Bulk Merge Process should automatically be handling the Sorting for us so this ordinal testing should always work...
                foreach (var childResult in childResults)
                {
                    Assert.IsNotNull(childResult);
                    Assert.IsTrue(childResult.ParentId > 0);
                    Assert.AreEqual(childTestData[i].ChildKey, childResult.ChildKey);
                    Assert.AreEqual(childTestData[i].ChildValue, childResult.ChildValue);
                    i++;
                }

            }
        }

        [TestMethod]
        public async Task TestBulkInsertResultSortOrderWithIdentitySetterSupportAsync()
        {
            var testData = TestHelpers.CreateTestDataWithIdentitySetter(10);

            var sqlConnectionString = SqlConnectionHelper.GetSqlConnectionString();
            ISqlBulkHelpersConnectionProvider sqlConnectionProvider = new SqlBulkHelpersConnectionProvider(sqlConnectionString);

            using (var sqlConn = await sqlConnectionProvider.NewConnectionAsync())
            using (SqlTransaction sqlTrans = sqlConn.BeginTransaction())
            {
                var results = await sqlTrans.BulkInsertAsync(testData, TestHelpers.TestTableName);

                //Test Child Data with Table name being derived from Model Annotation...
                var childTestData = TestHelpers.CreateChildTestData(results.Cast<TestElement>().ToList());
                var childResults = await sqlTrans.BulkInsertOrUpdateAsync(childTestData).ConfigureAwait(false);

                sqlTrans.Commit();

                //ASSERT Results are Valid...
                Assert.IsNotNull(results);

                //We Sort the Results by Identity Id to ensure that the inserts occurred in the correct
                //  ordinal order matching our Array of original values, but now with incrementing ID values!
                //This validates that data is inserted as expected for Identity columns and is validated
                //  correctly by sorting on the Incrementing Identity value when Queried (e.g. ORDER BY Id)
                //  which must then match our original order of data.
                var resultsSorted = results.OrderBy(r => r.Id).ToList();
                Assert.AreEqual(resultsSorted.Count(), testData.Count);

                var i = 0;
                foreach (var result in resultsSorted)
                {
                    Assert.IsNotNull(result);
                    Assert.IsTrue(result.Id > 0);
                    Assert.AreEqual(testData[i].Key, result.Key);
                    Assert.AreEqual(testData[i].Value, result.Value);
                    i++;
                }

                //The Bulk Merge Process should automatically be handling the Sorting for us so this ordinal testing should always work...
                foreach (var childResult in childResults)
                {
                    Assert.IsNotNull(childResult);
                    Assert.IsTrue(childResult.ParentId > 0);
                    Assert.AreEqual(childTestData[i].ChildKey, childResult.ChildKey);
                    Assert.AreEqual(childTestData[i].ChildValue, childResult.ChildValue);
                    i++;
                }


            }
        }
    }
}
