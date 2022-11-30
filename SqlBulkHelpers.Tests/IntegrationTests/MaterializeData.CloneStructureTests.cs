using System;
using SqlBulkHelpers.Tests;
using SqlBulkHelpers.MaterializedData;
using Microsoft.Data.SqlClient;

namespace SqlBulkHelpers.IntegrationTests
{
    [TestClass]
    public class MaterializeDataCloneStructureTests
    {
        [TestMethod]
        public async Task TestCloneTableStructureByAnnotationAsync()
        {
            var sqlConnectionProvider = SqlConnectionHelper.GetConnectionProvider();

            await using (var sqlConn = await sqlConnectionProvider.NewConnectionAsync().ConfigureAwait(false))
            await using (var sqlTransaction = (SqlTransaction)await sqlConn.BeginTransactionAsync().ConfigureAwait(false))
            {
                var cloneInfo = await sqlTransaction.CloneTableAsync<TestElementWithMappedNames>().ConfigureAwait(false);

                await sqlTransaction.RollbackAsync().ConfigureAwait(false);

                //ASSERT Results are Valid...
                Assert.IsNotNull(cloneInfo);
                Assert.AreEqual(TestHelpers.TestTableName, cloneInfo.SourceTable.TableName);
                Assert.AreEqual(TestHelpers.TestTableName, cloneInfo.TargetTable.TableName);
                Assert.AreNotEqual(cloneInfo.SourceTable.SchemaName, cloneInfo.TargetTable.SchemaName);
            }
        }

        [TestMethod]
        public async Task TestCloneTableStructureIntoCustomTargetSchemaAsync()
        {
            var sqlConnectionProvider = SqlConnectionHelper.GetConnectionProvider();
            const string CUSTOM_TARGET_SCHEMA = "materialized_data";

            var targetTableNameTerm = TableNameTerm.From(CUSTOM_TARGET_SCHEMA, TestHelpers.TestTableName);

            await using (var sqlConn = await sqlConnectionProvider.NewConnectionAsync().ConfigureAwait(false))
            await using (var sqlTransaction = (SqlTransaction)await sqlConn.BeginTransactionAsync().ConfigureAwait(false))
            {
                var cloneInfo = await sqlTransaction.CloneTableAsync(
                    sourceTableName: TestHelpers.TestTableNameFullyQualified, 
                    targetTableName: targetTableNameTerm
                ).ConfigureAwait(false);

                await sqlTransaction.RollbackAsync().ConfigureAwait(false);

                //ASSERT Results are Valid...
                Assert.IsNotNull(cloneInfo);
                Assert.AreEqual(TestHelpers.TestTableNameFullyQualified, cloneInfo.SourceTable.FullyQualifiedTableName);
                Assert.AreEqual(targetTableNameTerm, cloneInfo.TargetTable.FullyQualifiedTableName);
                Assert.AreNotEqual(cloneInfo.SourceTable.SchemaName, cloneInfo.TargetTable.SchemaName);
            }
        }

        [TestMethod]
        public async Task TestCloneTableStructureFailureIfExistsAsync()
        {
            var sqlConnectionProvider = SqlConnectionHelper.GetConnectionProvider();

            await using (var sqlConn = await sqlConnectionProvider.NewConnectionAsync().ConfigureAwait(false))
            await using (var sqlTransaction = (SqlTransaction)await sqlConn.BeginTransactionAsync().ConfigureAwait(false))
            {
                //First Clone and force re-creation to make this Test Idempotent!
                var successfulCloneInfo = await sqlTransaction.CloneTableAsync(
                    TestHelpers.TestTableNameFullyQualified, 
                    TestHelpers.TestTableName, 
                    recreateIfExists: true
                ).ConfigureAwait(false);
                
                CloneTableInfo? failedCloneInfo = null;
                Exception? failedToCloneException = null;
                try
                {
                    //Second attempt the clone again but this time expecting it to now already exist and fail out!
                    failedCloneInfo = await sqlTransaction.CloneTableAsync(
                        TestHelpers.TestTableNameFullyQualified, 
                        TestHelpers.TestTableName, 
                        recreateIfExists: false
                    ).ConfigureAwait(false);
                }
                catch (Exception cloneExc)
                {
                    failedToCloneException = cloneExc;
                }

                //Now Clean up the Cloned Table...
                //await sqlTransaction.DropTableAsync(successfulCloneInfo.TargetTable);
                await sqlTransaction.RollbackAsync().ConfigureAwait(false);

                //ASSERT Results are Valid...
                Assert.IsNotNull(successfulCloneInfo);
                Assert.IsNotNull(failedToCloneException);
                Assert.IsNull(failedCloneInfo);
            }
        }
    }
}
