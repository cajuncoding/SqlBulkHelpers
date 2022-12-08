using System;
using System.Diagnostics;
using SqlBulkHelpers.Tests;
using SqlBulkHelpers.MaterializedData;
using Microsoft.Data.SqlClient;
using RepoDb;
using SqlBulkHelpers.SqlBulkHelpers;

namespace SqlBulkHelpers.IntegrationTests
{
    [TestClass]
    public class MaterializeDataMaterializeIntoTests : BaseTest
    {
        [TestMethod]
        public async Task TestMaterializeDataIntoMultipleTablesAsync()
        {
            var sqlConnectionProvider = SqlConnectionHelper.GetConnectionProvider();

            var totalTimer = Stopwatch.StartNew();
            var timer = Stopwatch.StartNew();

            //FIRST CLEAR the Tables so we can validate that data changed (not coincidentally the same number of items)!
            await using (var sqlConn = await sqlConnectionProvider.NewConnectionAsync().ConfigureAwait(false))
            await using (var sqlTrans = (SqlTransaction)await sqlConn.BeginTransactionAsync().ConfigureAwait(false))
            {
                TestContext.WriteLine($"Initial (First) Connection & Transaction started in [{timer.ElapsedMilliseconds}] millis...");
                timer.Restart();

                await sqlTrans.ClearTablesAsync(new[]
                {
                    TestHelpers.TestChildTableNameFullyQualified,
                    TestHelpers.TestTableNameFullyQualified
                }, forceOverrideOfConstraints: true).ConfigureAwait(false);

                TestContext.WriteLine($"Cleared Tables in [{timer.ElapsedMilliseconds}] millis... (This includes initial Schema Loading & warming up of Cache!)");
                timer.Restart();

                await sqlTrans.CommitAsync().ConfigureAwait(false);

                TestContext.WriteLine($"Committed Clearing Transaction in [{timer.ElapsedMilliseconds}] millis...");
                timer.Restart();

                Assert.AreEqual(0, await sqlConn.CountAllAsync(tableName: TestHelpers.TestTableNameFullyQualified).ConfigureAwait(false));
                Assert.AreEqual(0, await sqlConn.CountAllAsync(tableName: TestHelpers.TestChildTableNameFullyQualified).ConfigureAwait(false));
            }

            timer.Restart();

            //NOW Materialize Data into the Tables!
            await using (var sqlConn = await sqlConnectionProvider.NewConnectionAsync().ConfigureAwait(false))
            await using (var sqlTrans = (SqlTransaction)await sqlConn.BeginTransactionAsync().ConfigureAwait(false))
            {
                TestContext.WriteLine($"Second Connection & Transaction started in [{timer.ElapsedMilliseconds}] millis...");
                timer.Restart();

                await using (var materializeDataContext = await sqlTrans.MaterializeDataIntoAsync(
                    TestHelpers.TestTableNameFullyQualified,
                    TestHelpers.TestChildTableNameFullyQualified
                ).ConfigureAwait(false))
                {
                    TestContext.WriteLine($"MaterializedData Context Created in [{timer.ElapsedMilliseconds}] millis...");
                    timer.Restart();

                    //Test with Table name being provided...
                    var parentMaterializationInfo = materializeDataContext[TestHelpers.TestTableNameFullyQualified];
                    var parentTestData = TestHelpers.CreateTestData(1500);
                    var parentResults = (await sqlTrans.BulkInsertAsync(parentTestData, tableName: parentMaterializationInfo.LoadingTable).ConfigureAwait(false)).ToList();

                    TestContext.WriteLine($"Parent Data Created [{parentTestData.Count}] items in [{timer.ElapsedMilliseconds}] millis...");
                    timer.Restart();

                    //Test Child Data with Table name being derived from Model Annotation...
                    var childMaterializationInfo = materializeDataContext[TestHelpers.TestChildTableNameFullyQualified];
                    var childTestData = TestHelpers.CreateChildTestData(parentResults);
                    var childResults = await sqlTrans.BulkInsertOrUpdateAsync(childTestData, tableName: childMaterializationInfo.LoadingTable).ConfigureAwait(false);

                    TestContext.WriteLine($"Child/Related Data Created [{childTestData.Count}] items in [{timer.ElapsedMilliseconds}] millis...");
                    timer.Restart();

                    await materializeDataContext.FinishMaterializationProcessAsync().ConfigureAwait(false);

                    TestContext.WriteLine($"Materialization Process Completed/Finished in [{timer.ElapsedMilliseconds}] millis...");
                    timer.Restart();

                    await sqlTrans.CommitAsync().ConfigureAwait(false);

                    TestContext.WriteLine($"Transaction Committed in [{timer.ElapsedMilliseconds}] millis...");
                    timer.Restart();

                    //We Sort the Results by Identity Id to ensure that the inserts occurred in the correct
                    //  ordinal order matching our Array of original values, but now with incrementing ID values!
                    //This validates that data is inserted as expected for Identity columns and is validated
                    //  correctly by sorting on the Incrementing Identity value when Queried (e.g. ORDER BY Id)
                    //  which must then match our original order of data.
                    var resultsSorted = parentResults.OrderBy(r => r.Id).ToList();
                    Assert.AreEqual(resultsSorted.Count(), parentTestData.Count);

                    var i = 0;
                    foreach (var result in resultsSorted)
                    {
                        Assert.IsNotNull(result);
                        Assert.IsTrue(result.Id > 0);
                        Assert.AreEqual(parentTestData[i].Key, result.Key);
                        Assert.AreEqual(parentTestData[i].Value, result.Value);
                        i++;
                    }

                    var parentTestDataLookupById = parentResults.ToLookup(r => r.Id);

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
                    Assert.AreEqual(parentTestData.Count, parentTableCount);
                    var childTableCount = await sqlConn.CountAllAsync(tableName: childMaterializationInfo.LiveTable);
                    Assert.AreEqual(childTestData.Count, childTableCount);
                }
            }

            TestContext.WriteLine($"{Environment.NewLine}Total Execution time was [{totalTimer.Elapsed.TotalSeconds}] seconds...");
        }

        [TestMethod]
        public async Task TestMaterializeDataWithFKeyConstraintFailedValidationAsync()
        {
            Exception sqlException = null;
            try
            {
                await InsertDataWithInvalidFKeyState(validationEnabled: true);
            }
            catch (Exception exc)
            {
                sqlException = exc;
            }

            //ASSERT Results are Valid...
            Assert.IsNotNull(sqlException);
            Assert.IsTrue(sqlException.Message.Contains("conflicted with the FOREIGN KEY constraint", StringComparison.OrdinalIgnoreCase));
        }

        [TestMethod]
        public async Task TestMaterializeDataWithFKeyConstraintForcingOverrideOfValidationAsync()
        {
            Exception sqlException = null;
            try
            {
                await InsertDataWithInvalidFKeyState(validationEnabled: false);
            }
            catch (Exception exc)
            {
                sqlException = exc;
            }

            //ASSERT Results are Valid...
            Assert.IsNull(sqlException);
        }

        private async Task InsertDataWithInvalidFKeyState(bool validationEnabled)
        {
            var sqlConnectionProvider = SqlConnectionHelper.GetConnectionProvider();

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
                var parentTestData = TestHelpers.CreateTestData(100);
                var parentResults = (await sqlTrans.BulkInsertAsync(parentTestData, tableName: parentMaterializationInfo.LoadingTable).ConfigureAwait(false)).ToList();

                //***********************************************************************
                //Now Clear the Parent Table to FORCE INVALID FKey STATE!!!
                //***********************************************************************
                await sqlTrans.ClearTableAsync(parentMaterializationInfo.LoadingTable);

                //Test Child Data with Table name being derived from Model Annotation...
                var childMaterializationInfo = materializeDataContext[TestHelpers.TestChildTableNameFullyQualified];
                var childTestData = TestHelpers.CreateChildTestData(parentResults);
                var childResults = await sqlTrans.BulkInsertOrUpdateAsync(childTestData, tableName: childMaterializationInfo.LoadingTable).ConfigureAwait(false);

                //By overriding this Value we force SQL Server to skip all validation of FKey constraints when re-enabling them!
                //  This can easily leave our data in an invalid state leaving the implementor responsible for ensuring Data Integrity of all tables being Materialized!
                materializeDataContext.EnableDataConstraintChecksOnCompletion = validationEnabled;

                await materializeDataContext.FinishMaterializationProcessAsync().ConfigureAwait(false);
                await sqlTrans.CommitAsync().ConfigureAwait(false);
            }
        }
    }
}
