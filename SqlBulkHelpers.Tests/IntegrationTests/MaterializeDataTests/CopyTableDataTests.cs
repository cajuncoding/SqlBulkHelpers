using System;
using SqlBulkHelpers.Tests;
using SqlBulkHelpers.MaterializedData;
using Microsoft.Data.SqlClient;
using RepoDb;
using SqlBulkHelpers.CustomExtensions;
using SqlBulkHelpers.SqlBulkHelpers;

namespace SqlBulkHelpers.IntegrationTests
{
    [TestClass]
    public class MaterializeDataCopyTableDataTests : BaseTest
    {
 
        [TestMethod]
        public async Task TestBasicCopyTableDataAsync()
        {
            var sqlConnectionProvider = SqlConnectionHelper.GetConnectionProvider();

            //We can construct this multiple ways, so here we test the Parse and Switch Schema methods...
            var targetTableNameTerm = TestHelpers.TestTableNameFullyQualified
                .ParseAsTableNameTerm()
                .ApplyNamePrefixOrSuffix(suffix: "_CopyTableDataTest");

            await using (var sqlConn = await sqlConnectionProvider.NewConnectionAsync().ConfigureAwait(false))
            await using (var sqlTrans = (SqlTransaction)await sqlConn.BeginTransactionAsync().ConfigureAwait(false))
            {
                //FIRST Copy the Table to ensure we have a Target Table...
                var cloneInfo = await sqlTrans.CloneTableAsync(
                    sourceTableName: TestHelpers.TestTableNameFullyQualified,
                    targetTableName: targetTableNameTerm,
                    recreateIfExists: true,
                    copyDataFromSource: false
                ).ConfigureAwait(false);

                //Second Copy the Data using the New explicit API..
                var resultCopyInfo = await sqlTrans.CopyTableDataAsync(TestHelpers.TestTableNameFullyQualified, targetTableNameTerm).ConfigureAwait(false);

                await sqlTrans.CommitAsync().ConfigureAwait(false);
                //await sqlTrans.RollbackAsync().ConfigureAwait(false);

                //Validate that the new table has No Data!
                Assert.IsNotNull(cloneInfo);
                Assert.IsNotNull(resultCopyInfo);
                Assert.AreEqual(resultCopyInfo.SourceTable.FullyQualifiedTableName, TestHelpers.TestTableNameFullyQualified);
                Assert.AreEqual(resultCopyInfo.TargetTable.FullyQualifiedTableName, targetTableNameTerm);
                var sourceTableCount = await sqlConn.CountAllAsync(tableName: cloneInfo.SourceTable).ConfigureAwait(false);
                var targetTableCount = await sqlConn.CountAllAsync(tableName: cloneInfo.TargetTable).ConfigureAwait(false);

                //Ensure both Source & Target contain the same number of records!
                Assert.AreEqual(sourceTableCount, targetTableCount);

                ////CLEANUP The Cloned Table so that other Tests Work as expected (e.g. Some tests validate Referencing FKeys, etc.
                ////  that are now increased with the table clone).
                //await using (var sqlTransForCleanup = (SqlTransaction)await sqlConn.BeginTransactionAsync().ConfigureAwait(false))
                //{
                //    await sqlTransForCleanup.DropTableAsync(cloneInfo.TargetTable).ConfigureAwait(false);
                //    await sqlTransForCleanup.CommitAsync().ConfigureAwait(false);
                //}
            }
        }
    }
}
