using SqlBulkHelpers.Tests;
using Microsoft.Data.SqlClient;
using SqlBulkHelpers.MaterializedData;
using SqlBulkHelpers.SqlBulkHelpers;

namespace SqlBulkHelpers.IntegrationTests
{
    [TestClass]
    public class BulkInsertOrUpdateTests
    {
        [TestMethod]
        public async Task TestBulkInsertOrUpdateWithCustomMatchQualifiersAsync()
        {
            var sqlConnectionString = SqlConnectionHelper.GetSqlConnectionString();
            ISqlBulkHelpersConnectionProvider sqlConnectionProvider = new SqlBulkHelpersConnectionProvider(sqlConnectionString);

            await using (var sqlConn = await sqlConnectionProvider.NewConnectionAsync().ConfigureAwait(false))
            await using (var sqlTrans = (SqlTransaction)await sqlConn.BeginTransactionAsync().ConfigureAwait(false))
            {
                //First create a new Table Clone that is empty for the integrity of this test...
                var clonedTableInfo = await sqlTrans.CloneTableAsync(TestHelpers.TestTableName).ConfigureAwait(false);
                
                //First insert Some Existing BaseLine Data (that will be updated)...
                var initialExistingTestData = TestHelpers.CreateTestData(5);
                var initialResults = (await sqlTrans.BulkInsertOrUpdateAsync(
                    initialExistingTestData, 
                    clonedTableInfo.TargetTable
                ).ConfigureAwait(false)).ToList();

                await sqlTrans.CommitAsync().ConfigureAwait(false);

                //Now Run the Insert/Update with Multi-matching Qualifier Expression...
                await using (var sqlTransMultiMatch = (SqlTransaction)await sqlConn.BeginTransactionAsync().ConfigureAwait(false))
                {
                    //Now we insert/update an additional 10 but the first 5 will have duplicates in the Value field...
                    var testData = TestHelpers.CreateTestData(10);
                    const string testKeyPrefix = "CUSTOM_QUALIFIER_MULTIPLE_MATCH_BY_VALUE";

                    foreach (var t in testData)
                        t.Key = $"{testKeyPrefix}-{t.Key}";

                    //Test using manual Table name provided...
                    var results = await sqlTransMultiMatch.BulkInsertOrUpdateAsync(
                        testData,
                        clonedTableInfo.TargetTable,
                        //This will result in DUPLICATE MATCHING results due to existing Values already inserted!
                        new SqlMergeMatchQualifierExpression(nameof(TestElement.Value))
                        {
                            ThrowExceptionIfNonUniqueMatchesOccur = false
                        }
                    ).ConfigureAwait(false);

                    await sqlTransMultiMatch.CommitAsync().ConfigureAwait(false);

                    //ASSERT Results are Valid...
                    Assert.IsNotNull(results);

                    //We Sort the Results by Identity Id to ensure that the inserts occurred in the correct
                    //  ordinal order matching our Array of original values, but now with incrementing ID values!
                    //This validates that data is inserted as expected for Identity columns and is validated
                    //  correctly by sorting on the Incrementing Identity value when Queried (e.g. ORDER BY Id)
                    //  which must then match our original order of data.
                    var resultsSorted = results.OrderBy(r => r.Id).ToList();
                    Assert.AreEqual(resultsSorted.Count, testData.Count);

                    var i = 0;
                    foreach (var result in resultsSorted)
                    {
                        Assert.IsNotNull(result);
                        Assert.IsTrue(result.Id > 0);
                        Assert.IsTrue(result.Key.StartsWith(testKeyPrefix));
                        Assert.AreEqual(result.Key, testData[i].Key);
                        Assert.AreEqual(result.Value, testData[i].Value);
                        i++;
                    }

                    await using (var sqlTransCleanup = (SqlTransaction)await sqlConn.BeginTransactionAsync().ConfigureAwait(false))
                    {
                        await sqlTransCleanup.DropTableAsync(clonedTableInfo.TargetTable);
                        await sqlTransCleanup.CommitAsync().ConfigureAwait(false);
                    }

                }
            }
        }

        [TestMethod]
        public async Task TestBulkInsertOrUpdateWithMultipleCustomMatchQualifiersAsync()
        {
            var testData = TestHelpers.CreateTestDataWithIdentitySetter(10);
            foreach (var t in testData)
            {
                t.Key = $"MULTIPLE_QUALIFIER_TEST-{t.Key}";
            }

            var sqlConnectionString = SqlConnectionHelper.GetSqlConnectionString();
            ISqlBulkHelpersConnectionProvider sqlConnectionProvider = new SqlBulkHelpersConnectionProvider(sqlConnectionString);

            await using var sqlConn = await sqlConnectionProvider.NewConnectionAsync().ConfigureAwait(false);
            await using (var sqlTrans = (SqlTransaction)await sqlConn.BeginTransactionAsync().ConfigureAwait(false))
            {
                var results = await sqlTrans.BulkInsertOrUpdateAsync(
                    testData,
                    TestHelpers.TestTableName,
                    new SqlMergeMatchQualifierExpression(
                        nameof(TestElement.Value),
                        nameof(TestElement.Key) //This will still result in UNIQUE entries that are being inserted (NO UPDATES)
                    )
                );

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
                    Assert.AreEqual((object)result.Key, testData[i].Key);
                    Assert.AreEqual((object)result.Value, testData[i].Value);
                    i++;
                }
            }
        }
    }
}
