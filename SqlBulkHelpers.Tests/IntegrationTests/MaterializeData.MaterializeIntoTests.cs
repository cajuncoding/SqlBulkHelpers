using System;
using SqlBulkHelpers.Tests;
using SqlBulkHelpers.MaterializedData;
using Microsoft.Data.SqlClient;
using RepoDb;
using SqlBulkHelpers.SqlBulkHelpers;

namespace SqlBulkHelpers.IntegrationTests
{
    [TestClass]
    public class MaterializeDataMaterializeIntoTests
    {
        [TestMethod]
        public async Task TestMaterializeDataIntoMultipleTablesAsync()
        {
            var sqlConnectionProvider = SqlConnectionHelper.GetConnectionProvider();

            //FIRST CLEAR the Tables so we can validate that data changed (not coincidentally the same number of items)!
            await using (var sqlConn = await sqlConnectionProvider.NewConnectionAsync().ConfigureAwait(false))
            await using (var sqlTrans = (SqlTransaction)await sqlConn.BeginTransactionAsync().ConfigureAwait(false))
            {
                await sqlTrans.ClearTablesAsync(new[]
                {
                    TestHelpers.TestChildTableNameFullyQualified,
                    TestHelpers.TestTableNameFullyQualified
                }, forceOverrideOfConstraints: true).ConfigureAwait(false);

                await sqlTrans.CommitAsync().ConfigureAwait(false);

                Assert.AreEqual(0, await sqlConn.CountAllAsync(tableName: TestHelpers.TestTableNameFullyQualified).ConfigureAwait(false));
                Assert.AreEqual(0, await sqlConn.CountAllAsync(tableName: TestHelpers.TestChildTableNameFullyQualified).ConfigureAwait(false));
            }

            //NOW Materialize Data into the Tables!
            await using (var sqlConn = await sqlConnectionProvider.NewConnectionAsync().ConfigureAwait(false))
            await using (var sqlTrans = (SqlTransaction)await sqlConn.BeginTransactionAsync().ConfigureAwait(false))
            await using (var materializeDataContext = await sqlTrans.MaterializeDataIntoAsync(
                 TestHelpers.TestTableNameFullyQualified,
                 TestHelpers.TestChildTableNameFullyQualified
             ).ConfigureAwait(false))
            {
                //Test with Table name being provided...
                var parentMaterializationInfo = materializeDataContext[TestHelpers.TestTableName];
                var testData = TestHelpers.CreateTestData(100);
                var results = (await sqlTrans.BulkInsertAsync(testData, tableName: parentMaterializationInfo.LoadingTable).ConfigureAwait(false)).ToList();

                //Test Child Data with Table name being derived from Model Annotation...
                var childMaterializationInfo = materializeDataContext[TestHelpers.TestChildTableNameFullyQualified];
                var childTestData = TestHelpers.CreateChildTestData(results);
                var childResults = await sqlTrans.BulkInsertOrUpdateAsync(childTestData, tableName: childMaterializationInfo.LoadingTable).ConfigureAwait(false);

                await materializeDataContext.FinishMaterializationProcessAsync().ConfigureAwait(false);
                await sqlTrans.CommitAsync().ConfigureAwait(false);

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

                //Finally validate the actual Table Counts in the database as a double check!
                var parentTableCount = await sqlConn.CountAllAsync(tableName: parentMaterializationInfo.LiveTable);
                Assert.AreEqual(testData.Count, parentTableCount);
                var childTableCount = await sqlConn.CountAllAsync(tableName: childMaterializationInfo.LiveTable);
                Assert.AreEqual(childTestData.Count, childTableCount);
            }
        }
    }
}
