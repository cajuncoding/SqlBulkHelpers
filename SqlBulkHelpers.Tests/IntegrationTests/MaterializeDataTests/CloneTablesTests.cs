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
    public class MaterializeDataCloneTablesTests : BaseTest
    {
        [TestMethod]
        public async Task TestCloneTableStructureByAnnotationAsync()
        {
            var sqlConnectionProvider = SqlConnectionHelper.GetConnectionProvider();

            await using (var sqlConn = await sqlConnectionProvider.NewConnectionAsync().ConfigureAwait(false))
            await using (var sqlTrans = (SqlTransaction)await sqlConn.BeginTransactionAsync().ConfigureAwait(false))
            {
                var cloneInfo = await sqlTrans.CloneTableAsync<TestElementWithMappedNames>().ConfigureAwait(false);

                var sourceTableSchema = sqlTrans.GetTableSchemaDefinition(cloneInfo.SourceTable.FullyQualifiedTableName);
                var clonedTableSchema = sqlTrans.GetTableSchemaDefinition(cloneInfo.TargetTable.FullyQualifiedTableName);

                await sqlTrans.RollbackAsync().ConfigureAwait(false);
                //await sqlTransaction.CommitAsync().ConfigureAwait(false);

                //ASSERT Results are Valid...
                Assert.IsNotNull(cloneInfo);
                Assert.AreEqual(TestHelpers.TestTableName, cloneInfo.SourceTable.TableName);
                Assert.AreNotEqual(cloneInfo.SourceTable.FullyQualifiedTableName, cloneInfo.TargetTable.FullyQualifiedTableName);

                //Validate the schema of the cloned table...
                Assert.IsNotNull(sourceTableSchema);
                Assert.IsNotNull(clonedTableSchema);
                Assert.AreEqual(cloneInfo.TargetTable.FullyQualifiedTableName, clonedTableSchema.TableNameTerm.FullyQualifiedTableName);
                Assert.AreEqual(sourceTableSchema.TableIndexes.Count, clonedTableSchema.TableIndexes.Count);
                Assert.AreEqual(sourceTableSchema.ForeignKeyConstraints.Count, clonedTableSchema.ForeignKeyConstraints.Count);
                Assert.AreEqual(sourceTableSchema.ColumnDefaultConstraints.Count, clonedTableSchema.ColumnDefaultConstraints.Count);
                Assert.AreEqual(sourceTableSchema.ColumnCheckConstraints.Count, clonedTableSchema.ColumnCheckConstraints.Count);
                Assert.AreEqual(sourceTableSchema.IdentityColumn.ColumnName, clonedTableSchema.IdentityColumn.ColumnName);
                Assert.AreEqual(
                    sourceTableSchema.PrimaryKeyConstraint.KeyColumns.OrderBy(k => k.OrdinalPosition).Select(k => k.ColumnName).ToCsv(),
                    clonedTableSchema.PrimaryKeyConstraint.KeyColumns.OrderBy(k => k.OrdinalPosition).Select(k => k.ColumnName).ToCsv()
                );
                Assert.AreEqual(
                    sourceTableSchema.TableColumns.OrderBy(k => k.OrdinalPosition).Select(k => k.ColumnName).ToCsv(),
                    clonedTableSchema.TableColumns.OrderBy(k => k.OrdinalPosition).Select(k => k.ColumnName).ToCsv()
                );
            }
        }

        [TestMethod]
        public async Task TestCloneTableStructureIntoCustomTargetSchemaAsync()
        {
            var sqlConnectionProvider = SqlConnectionHelper.GetConnectionProvider();
            const string CUSTOM_TARGET_SCHEMA = "materialized_data";

            //We can construct this multiple ways, so here we test the Parse and Switch Schema methods...
            var targetTableNameTerm = TestHelpers.TestTableNameFullyQualified
                .ParseAsTableNameTerm()
                .SwitchSchema(CUSTOM_TARGET_SCHEMA);

            await using (var sqlConn = await sqlConnectionProvider.NewConnectionAsync().ConfigureAwait(false))
            await using (var sqlTrans = (SqlTransaction)await sqlConn.BeginTransactionAsync().ConfigureAwait(false))
            {
                var cloneInfo = await sqlTrans.CloneTableAsync(
                    sourceTableName: TestHelpers.TestTableNameFullyQualified, 
                    targetTableName: targetTableNameTerm, 
                    recreateIfExists: true
                ).ConfigureAwait(false);

                await sqlTrans.CommitAsync().ConfigureAwait(false);
                //await sqlTrans.RollbackAsync().ConfigureAwait(false);

                //ASSERT Results are Valid...
                Assert.IsNotNull(cloneInfo);
                Assert.AreEqual(TestHelpers.TestTableNameFullyQualified, cloneInfo.SourceTable.FullyQualifiedTableName);
                Assert.AreEqual(cloneInfo.SourceTable.TableName, cloneInfo.TargetTable.TableName);
                Assert.AreNotEqual(cloneInfo.SourceTable.FullyQualifiedTableName, cloneInfo.TargetTable.FullyQualifiedTableName);
                Assert.AreEqual(targetTableNameTerm, cloneInfo.TargetTable.FullyQualifiedTableName);
                Assert.AreNotEqual(cloneInfo.SourceTable.SchemaName, cloneInfo.TargetTable.SchemaName);

                //Validate that the new table has No Data!
                var targetTableCount = await sqlConn.CountAllAsync(tableName: cloneInfo.TargetTable).ConfigureAwait(false);
                Assert.AreEqual(0, targetTableCount);

                //CLEANUP The Cloned Table so that other Tests Work as expected (e.g. Some tests validate Referencing FKeys, etc.
                //  that are now increased with the table clone).
                await using (var sqlTransForCleanup = (SqlTransaction)await sqlConn.BeginTransactionAsync().ConfigureAwait(false))
                {
                    await sqlTransForCleanup.DropTableAsync(cloneInfo.TargetTable).ConfigureAwait(false);
                    await sqlTransForCleanup.CommitAsync().ConfigureAwait(false);
                }
            }
        }

        [TestMethod]
        public async Task TestCloneTableStructureWithCopiedDataAsync()
        {
            var sqlConnectionProvider = SqlConnectionHelper.GetConnectionProvider();
            const string CUSTOM_TARGET_SCHEMA = "materialized_data";

            //We can construct this multiple ways, so here we test the Parse and Switch Schema methods...
            var targetTableNameTerm = TestHelpers.TestTableNameFullyQualified
                .ParseAsTableNameTerm()
                .SwitchSchema(CUSTOM_TARGET_SCHEMA);

            await using (var sqlConn = await sqlConnectionProvider.NewConnectionAsync().ConfigureAwait(false))
            await using (var sqlTrans = (SqlTransaction)await sqlConn.BeginTransactionAsync().ConfigureAwait(false))
            {
                var cloneInfo = await sqlTrans.CloneTableAsync(
                    sourceTableName: TestHelpers.TestTableNameFullyQualified,
                    targetTableName: targetTableNameTerm,
                    recreateIfExists: true,
                    copyDataFromSource: true
                ).ConfigureAwait(false);

                await sqlTrans.CommitAsync().ConfigureAwait(false);
                //await sqlTrans.RollbackAsync().ConfigureAwait(false);

                //Validate that the new table has No Data!
                Assert.IsNotNull(cloneInfo);
                var sourceTableCount = await sqlConn.CountAllAsync(tableName: cloneInfo.SourceTable).ConfigureAwait(false);
                var targetTableCount = await sqlConn.CountAllAsync(tableName: cloneInfo.TargetTable).ConfigureAwait(false);

                //Ensure both Source & Target contain the same number of records!
                Assert.AreEqual(sourceTableCount, targetTableCount);

                //CLEANUP The Cloned Table so that other Tests Work as expected (e.g. Some tests validate Referencing FKeys, etc.
                //  that are now increased with the table clone).
                await using (var sqlTransForCleanup = (SqlTransaction)await sqlConn.BeginTransactionAsync().ConfigureAwait(false))
                {
                    await sqlTransForCleanup.DropTableAsync(cloneInfo.TargetTable).ConfigureAwait(false);
                    await sqlTransForCleanup.CommitAsync().ConfigureAwait(false);
                }
            }
        }

        [TestMethod]
        public async Task TestCloneTableStructureFailureIfExistsAsync()
        {
            var sqlConnectionProvider = SqlConnectionHelper.GetConnectionProvider();

            var clonedTableName = $"{TestHelpers.TestTableName}_CLONING_TEST";

            await using (var sqlConn = await sqlConnectionProvider.NewConnectionAsync().ConfigureAwait(false))
            await using (var sqlTrans = (SqlTransaction)await sqlConn.BeginTransactionAsync().ConfigureAwait(false))
            {
                //First Clone and force re-creation to make this Test Idempotent!
                var successfulCloneInfo = await sqlTrans.CloneTableAsync(
                    TestHelpers.TestTableNameFullyQualified,
                    clonedTableName, 
                    recreateIfExists: true
                ).ConfigureAwait(false);
                
                CloneTableInfo? failedCloneInfo = null;
                Exception? failedToCloneException = null;
                try
                {
                    //Second attempt the clone again but this time expecting it to now already exist and fail out!
                    failedCloneInfo = await sqlTrans.CloneTableAsync(
                        TestHelpers.TestTableNameFullyQualified,
                        clonedTableName, 
                        recreateIfExists: false
                    ).ConfigureAwait(false);
                }
                catch (Exception cloneExc)
                {
                    failedToCloneException = cloneExc;
                }

                //Now Clean up the Cloned Table...
                //await sqlTransaction.DropTableAsync(successfulCloneInfo.TargetTable);
                await sqlTrans.RollbackAsync().ConfigureAwait(false);

                //ASSERT Results are Valid...
                Assert.IsNotNull(successfulCloneInfo);
                Assert.IsNotNull(failedToCloneException);
                Assert.IsNull(failedCloneInfo);
            }
        }
    }
}
