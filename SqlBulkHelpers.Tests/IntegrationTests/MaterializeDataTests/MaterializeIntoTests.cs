using System;
using System.Diagnostics;
using SqlBulkHelpers.Tests;
using SqlBulkHelpers.MaterializedData;
using Microsoft.Data.SqlClient;
using RepoDb;
using SqlBulkHelpers.SqlBulkHelpers;
using SqlBulkHelpers.Utilities;

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
            {
                TestContext.WriteLine($"Second Connection & Transaction started in [{timer.ElapsedMilliseconds}] millis...");
                timer.Restart();

                List<TestElement>? parentResults = null;
                List<ChildTestElement>? childTestData = null;
                List<ChildTestElement>? childResults = null;
                MaterializationTableInfo? parentMaterializationInfo = null;
                MaterializationTableInfo? childMaterializationInfo = null;
                var parentTestData = TestHelpers.CreateTestData(1500);

                //******************************************************************************************
                //START the Materialize Data Process...
                //******************************************************************************************
                var tableNames = new[]
                {
                    TestHelpers.TestTableNameFullyQualified,
                    TestHelpers.TestChildTableNameFullyQualified
                };

                await sqlConn.ExecuteMaterializeDataProcessAsync(tableNames, async (materializeDataContext, sqlTransaction) =>
                {
                    timer.Stop();
                    TestContext.WriteLine($"MaterializedData Context Created in [{timer.ElapsedMilliseconds}] millis...");
                    foreach (var table in materializeDataContext.Tables)
                        TestContext.WriteLine($"    - {table.LoadingTable.FullyQualifiedTableName} ==> {table.LiveTable.FullyQualifiedTableName}");

                    //Test with Table name being provided...
                    timer.Restart();
                    parentMaterializationInfo = materializeDataContext.FindMaterializationTableInfoCaseInsensitive(TestHelpers.TestTableNameFullyQualified);
                    parentResults = (await sqlTransaction.BulkInsertAsync(parentTestData, tableName: parentMaterializationInfo.LoadingTable).ConfigureAwait(false)).ToList();

                    TestContext.WriteLine($"Parent Data Created [{parentTestData.Count}] items in [{timer.ElapsedMilliseconds}] millis...");
                    timer.Restart();

                    //Test Child Data with Table name being derived from Model Annotation...
                    childMaterializationInfo = materializeDataContext[TestHelpers.TestChildTableNameFullyQualified];
                    childTestData = TestHelpers.CreateChildTestData(parentResults);
                    childResults = (await sqlTransaction.BulkInsertOrUpdateAsync(childTestData, tableName: childMaterializationInfo.LoadingTable).ConfigureAwait(false)).ToList();

                    TestContext.WriteLine($"Child/Related Data Created [{childTestData.Count}] items in [{timer.ElapsedMilliseconds}] millis...");
                    timer.Restart();

                }).ConfigureAwait(false);
                

                TestContext.WriteLine($"Materialization Process Completed/Finished in [{timer.ElapsedMilliseconds}] millis...");
                timer.Restart();

                //We Sort the Results by Identity Id to ensure that the inserts occurred in the correct
                //  ordinal order matching our Array of original values, but now with incrementing ID values!
                //This validates that data is inserted as expected for Identity columns and is validated
                //  correctly by sorting on the Incrementing Identity value when Queried (e.g. ORDER BY Id)
                //  which must then match our original order of data.
                var resultsSorted = parentResults!.OrderBy(r => r.Id).ToList();
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

                var parentTestDataLookupById = parentResults!.ToLookup(r => r.Id);

                //The Bulk Merge Process should automatically be handling the Sorting for us so this ordinal testing should always work...
                var c = 0;
                foreach (var childResult in childResults)
                {
                    Assert.IsNotNull(childResult);
                    Assert.IsTrue(childResult.ParentId > 0);
                    Assert.IsNotNull(parentTestDataLookupById[childResult.ParentId].FirstOrDefault());
                    Assert.AreEqual(childTestData![c].ChildKey, childResult.ChildKey);
                    Assert.AreEqual(childTestData[c].ChildValue, childResult.ChildValue);
                    c++;
                }

                //Finally validate the actual Table Counts in the database as a double check!
                var parentTableCount = await sqlConn.CountAllAsync(tableName: parentMaterializationInfo!.LiveTable);
                Assert.AreEqual(parentTestData.Count, parentTableCount);
                var childTableCount = await sqlConn.CountAllAsync(tableName: childMaterializationInfo!.LiveTable);
                Assert.AreEqual(childTestData!.Count, childTableCount);
            }

            TestContext.WriteLine($"{Environment.NewLine}Total Execution time was [{totalTimer.Elapsed.TotalSeconds}] seconds...");
        }

        [TestMethod]
        public async Task TestMaterializeDataClearTableWithIdentityValueAsync()
        {
            var sqlConnectionProvider = SqlConnectionHelper.GetConnectionProvider();

            //FIRST CLEAR the Tables so we can validate that data changed (not coincidentally the same number of items)!
            await using (var sqlConn = await sqlConnectionProvider.NewConnectionAsync().ConfigureAwait(false))
            {
                var initialIdentityValue = await sqlConn.GetTableCurrentIdentityValueAsync(TestHelpers.TestTableNameFullyQualified).ConfigureAwait(false);

                await using (var sqlTrans = (SqlTransaction)await sqlConn.BeginTransactionAsync().ConfigureAwait(false))
                {
                    await sqlTrans.ClearTablesAsync(new[]
                        {
                            TestHelpers.TestTableNameFullyQualified, 
                            TestHelpers.TestChildTableNameFullyQualified
                        }, 
                        forceOverrideOfConstraints: true
                    ).ConfigureAwait(false);
                    
                    await sqlTrans.CommitAsync().ConfigureAwait(false);
                }

                var finalIdentityValue = await sqlConn.GetTableCurrentIdentityValueAsync(TestHelpers.TestTableNameFullyQualified).ConfigureAwait(false);

                Assert.AreEqual(initialIdentityValue, finalIdentityValue);
            }
        }

        [TestMethod]
        public async Task TestMaterializeDataIntoTableWithIdentityColumnSyncingAsync()
        {
            var sqlConnectionProvider = SqlConnectionHelper.GetConnectionProvider();

            //FIRST CLEAR the Tables so we can validate that data changed (not coincidentally the same number of items)!
            await using (var sqlConn = await sqlConnectionProvider.NewConnectionAsync().ConfigureAwait(false))
            {
                var initialIdentityValue = await sqlConn.GetTableCurrentIdentityValueAsync(TestHelpers.TestTableNameFullyQualified).ConfigureAwait(false);
                int testDataCount = 15;

                await sqlConn.ExecuteMaterializeDataProcessAsync(TestHelpers.TestTableNameFullyQualified, async (materializeDataContext, sqlTransaction) =>
                {
                    TestContext.WriteLine($"MaterializedData Context:");
                    foreach (var table in materializeDataContext.Tables)
                        TestContext.WriteLine($"    - {table.LoadingTable.FullyQualifiedTableName} ==> {table.LiveTable.FullyQualifiedTableName}");

                    //Test with Table name being provided...
                    var loadingTableTerm = materializeDataContext.GetLoadingTableName(TestHelpers.TestTableNameFullyQualified);

                    var testData = TestHelpers.CreateTestData(testDataCount);

                    //MANUALLY Initialize ALL Identity ID Values..
                    int idCounter = (int)initialIdentityValue + 1;
                    foreach (var testItem in testData)
                        testItem.Id = (idCounter++);

                    var initialLoadingIdentityValue = await sqlTransaction.GetTableCurrentIdentityValueAsync(loadingTableTerm).ConfigureAwait(false);
                    Assert.AreEqual(initialIdentityValue, initialLoadingIdentityValue);

                    await sqlTransaction.BulkInsertAsync(testData, tableName: loadingTableTerm).ConfigureAwait(false);

                    var finalLoadingIdentityValue = await sqlTransaction.GetTableCurrentIdentityValueAsync(loadingTableTerm).ConfigureAwait(false);

                    //NOTE: Identity value will assign the current value so we offset by -1 to correctly calculate the final value...
                    Assert.AreEqual((initialLoadingIdentityValue-1) + testDataCount, finalLoadingIdentityValue);
                }).ConfigureAwait(false);

                var finalIdentityValue = await sqlConn.GetTableCurrentIdentityValueAsync(TestHelpers.TestTableNameFullyQualified).ConfigureAwait(false);

                //NOTE: Identity value will assign the current value so we offset by -1 to correctly calculate the final value...
                Assert.AreEqual((initialIdentityValue-1) + testDataCount, finalIdentityValue);
            }
        }

        [TestMethod]
        public async Task TestMaterializeDataIntoTableWithFullTextIndexAsync()
        {
            var sqlConnectionProvider = SqlConnectionHelper.GetConnectionProvider();
            var timer = Stopwatch.StartNew();

            //Update our Configuration to provide a Connection Factory which Enables Concurrent Connection Processing...
            SqlBulkHelpersConfig.ConfigureDefaults(config =>
            {
                //Enable FullTextIndexHandling as it's disabled by default since it cannot be handled within a SQL Transaction and is instead managed by Code (with error handling).
                config.IsFullTextIndexHandlingEnabled = true;
                //As an optimization we enable Concurrent Processing which can improve performance when re-enabling FullTextIndexes, etc.
                config.EnableConcurrentSqlConnectionProcessing(sqlConnectionProvider);
            });

            //NOW Materialize Data into the Tables!
            await using (var sqlConn = await sqlConnectionProvider.NewConnectionAsync().ConfigureAwait(false))
            {
                await using (var sqlClearTransaction = (SqlTransaction)await sqlConn.BeginTransactionAsync().ConfigureAwait(false))
                {
                    await sqlClearTransaction.ClearTableAsync(TestHelpers.TestTableWithFullTextIndexFullyQualified);
                    await sqlClearTransaction.CommitAsync().ConfigureAwait(false);
                }

                var testDataCount = 1500;

                //******************************************************************************************
                //START the Materialize Data Process...
                //******************************************************************************************
                await sqlConn.ExecuteMaterializeDataProcessAsync(TestHelpers.TestTableWithFullTextIndexFullyQualified, async (materializeDataContext, sqlTransaction) =>
                {
                    TestContext.WriteLine($"MaterializedData Context:");
                    foreach (var table in materializeDataContext.Tables)
                        TestContext.WriteLine($"    - {table.LoadingTable.FullyQualifiedTableName} ==> {table.LiveTable.FullyQualifiedTableName}");

                    //Test with Table name being provided...
                    var testData = TestHelpers.CreateTestData(testDataCount);
                    var loadingTableTerm = materializeDataContext.GetLoadingTableName(TestHelpers.TestTableWithFullTextIndexFullyQualified);
                    await sqlTransaction.BulkInsertAsync(testData, tableName: loadingTableTerm).ConfigureAwait(false);
                    
                    //NOTE: WE CANNOT Commit the Transaction or else the rest of the Materialization Process cannot complete...
                    //await sqlTransaction.CommitAsync().ConfigureAwait(false);
                }).ConfigureAwait(false);


                timer.Stop();
                TestContext.WriteLine($"Materialization Process Completed/Finished in [{timer.ElapsedMilliseconds}] millis...");

                var finalCount = (int)await sqlConn.CountAllAsync(TestHelpers.TestTableWithFullTextIndexFullyQualified);
                Assert.AreEqual(testDataCount, finalCount);

                //NOTE: FOR large tables (500,000+) records it takes time for the Full Text Index to re-build so this Schema query is blocked
                //      therefore we implement a Retry to stabilize this test...
                var tableSchema = await Retry.WithExponentialBackoffAsync(20, 
                    async () => await sqlConn.GetTableSchemaDefinitionAsync(
                    TestHelpers.TestTableWithFullTextIndexFullyQualified,
                    TableSchemaDetailLevel.ExtendedDetails,
                    forceCacheReload: true
                )).ConfigureAwait(false);

                Assert.IsNotNull(tableSchema);
                Assert.IsNotNull(tableSchema.FullTextIndex);
                Assert.AreEqual("PK_SqlBulkHelpersTestElements_WithFullTextIndex", tableSchema.FullTextIndex.UniqueIndexName);
            }
        }

        [TestMethod]
        public async Task TestMaterializeDataContextRetrieveTableInfoAsync()
        {
            var sqlConnectionProvider = SqlConnectionHelper.GetConnectionProvider();
            var timer = Stopwatch.StartNew();

            //NOW Materialize Data into the Tables!
            await using (var sqlConn = await sqlConnectionProvider.NewConnectionAsync().ConfigureAwait(false))
            {
                var testDataCount = 1;

                //******************************************************************************************
                //START the Materialize Data Process...
                //******************************************************************************************
                await sqlConn.ExecuteMaterializeDataProcessAsync(new[] { typeof(TestElementWithMappedNames) }, async (materializeDataContext, sqlTransaction) =>
                {
                    var tableInfoByIndex = materializeDataContext[0];
                    Assert.IsNotNull(tableInfoByIndex);
                    TestContext.WriteLine($"Retrieve Loading Table By Index [0]: [{tableInfoByIndex.LoadingTable}]");

                    var tableInfoByName = materializeDataContext[TestHelpers.TestTableName];
                    Assert.IsNotNull(tableInfoByName);
                    TestContext.WriteLine($"Retrieve Loading Table By Name [{TestHelpers.TestTableName}]: [{tableInfoByName.LoadingTable}]");

                    var tableInfoByType = materializeDataContext[typeof(TestElementWithMappedNames)];
                    Assert.IsNotNull(tableInfoByType);
                    TestContext.WriteLine($"Retrieve Loading Table By Model Type [{nameof(TestElementWithMappedNames)}]: [{tableInfoByType.LoadingTable}]");

                    //TEST Passive Cancellation process (no need to throw an exception in this advanced use case)...
                    materializeDataContext.CancelMaterializationProcess();
                }).ConfigureAwait(false);


                timer.Stop();
                TestContext.WriteLine($"Materialization Test Completed/Finished in [{timer.ElapsedMilliseconds}] millis...");
            }
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
                TestContext.WriteLine(exc.Message);
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
            {
                ////Must clear all Data and Related Data to maintain Data Integrity...
                ////NOTE: If we don't clear the related table then the FKey Constraint Check on the Related data (Child table) will FAIL!
                //await sqlTrans.ClearTablesAsync(new[]
                //{
                //    TestHelpers.TestChildTableNameFullyQualified,
                //    TestHelpers.TestTableNameFullyQualified
                //}, forceOverrideOfConstraints: true).ConfigureAwait(false);

                //******************************************************************************************
                //START the Materialize Data Process...
                //******************************************************************************************
                var materializeDataContext = await sqlTrans.StartMaterializeDataProcessAsync(
                    TestHelpers.TestTableNameFullyQualified,
                    TestHelpers.TestChildTableNameFullyQualified
                ).ConfigureAwait(false);

                //Test with Table name being provided...
                var parentMaterializationInfo = materializeDataContext[TestHelpers.TestTableName];
                var parentTestData = TestHelpers.CreateTestData(100);
                var parentResults = (await sqlTrans.BulkInsertAsync(parentTestData, tableName: parentMaterializationInfo.LoadingTable).ConfigureAwait(false)).ToList();

                //***********************************************************************
                //Now Clear the Parent Table to FORCE INVALID FKey STATE!!!
                //***********************************************************************
                await sqlTrans.ClearTableAsync(parentMaterializationInfo.LoadingTable).ConfigureAwait(false);

                //Test Child Data with Table name being derived from Model Annotation...
                var childMaterializationInfo = materializeDataContext[TestHelpers.TestChildTableNameFullyQualified];
                var childTestData = TestHelpers.CreateChildTestData(parentResults);
                var childResults = await sqlTrans.BulkInsertOrUpdateAsync(childTestData, tableName: childMaterializationInfo.LoadingTable).ConfigureAwait(false);

                //By overriding this Value we force SQL Server to skip all validation of FKey constraints when re-enabling them!
                //  This can easily leave our data in an invalid state leaving the implementor responsible for ensuring Data Integrity of all tables being Materialized!
                materializeDataContext.EnableDataConstraintChecksOnCompletion = validationEnabled;

                //******************************************************************************************
                //FINISH the Materialize Data Process...
                //******************************************************************************************
                await materializeDataContext.FinishMaterializeDataProcessAsync().ConfigureAwait(false);
                await sqlTrans.CommitAsync().ConfigureAwait(false);
            }
        }
    }
}
