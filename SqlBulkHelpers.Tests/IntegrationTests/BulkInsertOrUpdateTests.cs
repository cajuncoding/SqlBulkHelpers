using SqlBulkHelpers.Tests;
using Microsoft.Data.SqlClient;
using SqlBulkHelpers.SqlBulkHelpers;

namespace SqlBulkHelpers.IntegrationTests
{
    [TestClass]
    public class BulkInsertOrUpdateTests
    {
        [TestMethod]
        public async Task TestBulkInsertOrUpdateWithCustomMatchQualifiersAsync()
        {
            var testData = TestHelpers.CreateTestData(10);
            
            foreach (var t in testData)
                t.Key = $"CUSTOM_QUALIFIER_BY_VALUE-{t.Key}";

            var sqlConnectionString = SqlConnectionHelper.GetSqlConnectionString();
            ISqlBulkHelpersConnectionProvider sqlConnectionProvider = new SqlBulkHelpersConnectionProvider(sqlConnectionString);

            using (var sqlConn = await sqlConnectionProvider.NewConnectionAsync())
            using (SqlTransaction sqlTrans = sqlConn.BeginTransaction())
            {
                //Test using manual Table name provided...
                var results = await sqlTrans.BulkInsertOrUpdateAsync(
                    testData,
                    TestHelpers.TestTableName, 
                    new SqlMergeMatchQualifierExpression(nameof(TestElement.Value))
                    {
                        ThrowExceptionIfNonUniqueMatchesOccur = false
                    }
                );

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
                    Assert.AreEqual(result.Key, testData[i].Key);
                    Assert.AreEqual(result.Value, testData[i].Value);
                    i++;
                }

            }
        }

        [TestMethod]
        public async Task TestBulkInsertOrUpdateWithMultipleCustomMatchQualifiersAsync()
        {
            var testData = TestHelpers.CreateTestDataWithIdentitySetter(10);
            int count = 1;
            foreach (var t in testData)
            {
                t.Id = count++;
                t.Key = $"MULTIPLE_QUALIFIER_TEST-{t.Key}";
            }

            var sqlConnectionString = SqlConnectionHelper.GetSqlConnectionString();
            ISqlBulkHelpersConnectionProvider sqlConnectionProvider = new SqlBulkHelpersConnectionProvider(sqlConnectionString);

            using (var sqlConn = await sqlConnectionProvider.NewConnectionAsync())
            using (SqlTransaction sqlTrans = sqlConn.BeginTransaction())
            {
                ISqlBulkHelper<TestElement> sqlBulkIdentityHelper = new SqlBulkHelper<TestElement>(
                    sqlConnectionProvider, 
                    TestHelpers.BulkHelpersConfig
                );

                var results = await sqlTrans.BulkInsertOrUpdateAsync(
                    testData,
                    TestHelpers.TestTableName,
                    new SqlMergeMatchQualifierExpression(
                        nameof(TestElement.Id), 
                        nameof(TestElement.Value), 
                        nameof(TestElement.Key)
                    )
                );

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
                    Assert.AreEqual(result.Key, testData[i].Key);
                    Assert.AreEqual(result.Value, testData[i].Value);
                    i++;
                }

            }
        }
    }
}
