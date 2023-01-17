using System;
using SqlBulkHelpers.Tests;
using Microsoft.Data.SqlClient;
using SqlBulkHelpers.MaterializedData;
using SqlBulkHelpers.SqlBulkHelpers;

namespace SqlBulkHelpers.IntegrationTests
{
    [TestClass]
    public class SqlBulkHelpersBulkInsertTests : BaseTest
    {
        [TestMethod]
        public async Task TestBulkInsertResultSortOrderAsync()
        {
            var sqlConnectionString = SqlConnectionHelper.GetSqlConnectionString();
            ISqlBulkHelpersConnectionProvider sqlConnectionProvider = new SqlBulkHelpersConnectionProvider(sqlConnectionString);

            await using (var sqlConn = await sqlConnectionProvider.NewConnectionAsync().ConfigureAwait(false))
            await using (var sqlTrans = (SqlTransaction)await sqlConn.BeginTransactionAsync().ConfigureAwait(false))
            {
                ////Must clear all Data and Related Data to maintain Data Integrity...
                ////NOTE: If we don't clear the related table then the FKey Constraint Check on the Related data (Child table) will FAIL!
                //await sqlTrans.ClearTablesAsync(new[]
                //{
                //    TestHelpers.TestChildTableNameFullyQualified,
                //    TestHelpers.TestTableNameFullyQualified
                //}, forceOverrideOfConstraints: true).ConfigureAwait(false);

                //Test with Table name being provided...
                var testData = TestHelpers.CreateTestData(10);
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

                var parentTestDataLookupById = results.ToLookup(r => r.Id);

                //The Bulk Merge Process should automatically be handling the Sorting for us so this ordinal testing should always work...
                var c = 0;
                foreach (var childResult in childResults)
                {
                    Assert.IsNotNull(childResult);
                    Assert.IsTrue(childResult.ParentId > 0);
                    Assert.IsNotNull(parentTestDataLookupById[childResult.ParentId].FirstOrDefault());
                    Assert.AreEqual(childTestData[c].ChildKey, childResult.ChildKey);
                    Assert.AreEqual(childTestData[c].ChildValue, childResult.ChildValue);
                    c++;
                }

            }
        }

        [TestMethod]
        public async Task TestBulkInsertResultSortOrderWithIdentitySetterSupportAsync()
        {
            var sqlConnectionString = SqlConnectionHelper.GetSqlConnectionString();
            ISqlBulkHelpersConnectionProvider sqlConnectionProvider = new SqlBulkHelpersConnectionProvider(sqlConnectionString);

            await using (var sqlConn = await sqlConnectionProvider.NewConnectionAsync().ConfigureAwait(false))
            await using (SqlTransaction sqlTrans = (SqlTransaction)await sqlConn.BeginTransactionAsync().ConfigureAwait(false))
            {
                ////Must clear all Data and Related Data to maintain Data Integrity...
                ////NOTE: If we don't clear the related table then the FKey Constraint Check on the Related data (Child table) will FAIL!
                await sqlTrans.ClearTablesAsync(new[]
                {
                    TestHelpers.TestChildTableNameFullyQualified,
                    TestHelpers.TestTableNameFullyQualified
                }, forceOverrideOfConstraints: true).ConfigureAwait(false);

                var testData = TestHelpers.CreateTestDataWithIdentitySetter(10);
                var results = (await sqlTrans.BulkInsertAsync(testData, TestHelpers.TestTableName)).ToList();

                //Test Child Data with Table name being derived from Model Annotation...
                var childTestData = TestHelpers.CreateChildTestData(results.Cast<TestElement>().ToList());
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

                var parentTestDataLookupById = results.ToLookup(r => r.Id);

                //The Bulk Merge Process should automatically be handling the Sorting for us so this ordinal testing should always work...
                var c = 0;
                foreach (var childResult in childResults)
                {
                    Assert.IsNotNull(childResult);
                    Assert.IsTrue(childResult.ParentId > 0);
                    Assert.IsNotNull(parentTestDataLookupById[childResult.ParentId].FirstOrDefault());
                    Assert.AreEqual(childTestData[c].ChildKey, childResult.ChildKey);
                    Assert.AreEqual(childTestData[c].ChildValue, childResult.ChildValue);
                    c++;
                }
            }
        }

        [TestMethod]
        public async Task TestBulkInsertOnlySkippingExistingUpdateResultsAsync()
        {
            var initialTestData = TestHelpers.CreateTestData(10);

            var sqlConnectionString = SqlConnectionHelper.GetSqlConnectionString();
            ISqlBulkHelpersConnectionProvider sqlConnectionProvider = new SqlBulkHelpersConnectionProvider(sqlConnectionString);

            //Insert Baseline data as 'existing data'
            await using (var sqlConn = await sqlConnectionProvider.NewConnectionAsync().ConfigureAwait(false))
            await using (var sqlTrans = (SqlTransaction)await sqlConn.BeginTransactionAsync().ConfigureAwait(false))
            {
                //Must clear all Data and Related Data to maintain Data Integrity...
                //NOTE: If we don't clear the related table then the FKey Constraint Check on the Related data (Child table) will FAIL!
                await sqlTrans.ClearTablesAsync(
                    tableNames: new [] 
                    { 
                        TestHelpers.TestChildTableNameFullyQualified,
                        TestHelpers.TestTableNameFullyQualified
                    }, 
                    forceOverrideOfConstraints: true
                ).ConfigureAwait(false);

                //Test with Table name being provided...
                var results = (await sqlTrans.BulkInsertAsync(initialTestData, tableName: TestHelpers.TestTableName).ConfigureAwait(false)).ToList();
                await sqlTrans.CommitAsync().ConfigureAwait(false);
            }

            //Now Test Insert with partially existing and partially new data results in ONLY Inserted data being returned/updated....
            await using (var sqlConn = await sqlConnectionProvider.NewConnectionAsync().ConfigureAwait(false))
            await using (var sqlTrans = (SqlTransaction)await sqlConn.BeginTransactionAsync().ConfigureAwait(false))
            {
                //Now create a mixed set of existing and new data

                var testInsertOnlyData = initialTestData.Skip(3).Take(5).ToList();
                var newTestData = TestHelpers.CreateTestData(5);
                testInsertOnlyData.AddRange(newTestData);

                //Test with Table name being provided...
                var results = (await sqlTrans.BulkInsertAsync(testInsertOnlyData, tableName: TestHelpers.TestTableName).ConfigureAwait(false)).ToList();
                await sqlTrans.CommitAsync().ConfigureAwait(false);

                //ASSERT Results are Valid...
                Assert.IsNotNull(results);

                //We Sort the Results by Identity Id to ensure that the inserts occurred in the correct
                //  ordinal order matching our Array of original values, but now with incrementing ID values!
                //This validates that data is inserted as expected for Identity columns and is validated
                //  correctly by sorting on the Incrementing Identity value when Queried (e.g. ORDER BY Id)
                //  which must then match our original order of data.
                var resultsSorted = results.OrderBy(r => r.Id).ToList();
                Assert.AreEqual(resultsSorted.Count(), newTestData.Count);

                var i = 0;
                foreach (var result in resultsSorted)
                {
                    Assert.IsNotNull(result);
                    Assert.IsTrue(result.Id > 0);
                    Assert.AreEqual(newTestData[i].Key, result.Key);
                    Assert.AreEqual(newTestData[i].Value, result.Value);
                    i++;
                }

                var insertedResultsLookupByKey = results.ToLookup(r => r.Key);

                //Ensure that NONE of the original Existing Data exist in the Results
                foreach (var testItem in initialTestData)
                {
                    //Inserted results should NOT be from the Originally inserted 'existing' test data set...
                    Assert.IsNull(insertedResultsLookupByKey[testItem.Key].FirstOrDefault());
                }
            }
        }
    }
}
