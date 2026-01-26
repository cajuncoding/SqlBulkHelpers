using System;
using System.Diagnostics;
using Microsoft.Data.SqlClient;
using RepoDb;

namespace SqlBulkHelpers.Tests.IntegrationTests
{
    [TestClass]
    public class SchemaLoaderTests : BaseTest
    {
        [TestMethod]
        public void TestTableDefinitionLoadingBasicDetailsWithTransactionSyncMethod()
        {
            using var sqlConn = SqlConnectionHelper.NewConnection();
            using var sqlTransaction = sqlConn.BeginTransaction();

            var tableDefinition = sqlTransaction.GetTableSchemaDefinition(
                TestHelpers.TestTableNameFullyQualified,
                TableSchemaDetailLevel.BasicDetails
            );

            AssertTableDefinitionIsValidForTestElementParentTable(tableDefinition, TableNameTerm.From<object>(TestHelpers.TestTableNameFullyQualified), TableSchemaDetailLevel.BasicDetails);
        }

        [TestMethod]
        public void TestTableDefinitionLoadingBasicDetailsForTempTableWithTransactionSyncMethod()
        {
            using var sqlConn = SqlConnectionHelper.NewConnection();
            using var sqlTransaction = sqlConn.BeginTransaction();

            var tempTableName = "#TempTableSchemaLoaderTest";
            sqlConn.ExecuteNonQuery($@"
                CREATE TABLE [{tempTableName}] ([Id] INT NOT NULL PRIMARY KEY);
            ", transaction: sqlTransaction);

            var tableDefinition = sqlTransaction.GetTableSchemaDefinition(
                tempTableName,
                TableSchemaDetailLevel.BasicDetails
            );

            AssertTableDefinitionIsValidForTempTable(tempTableName, tableDefinition);
        }

        [TestMethod]
        public void TestTableDefinitionLoadingForTableWithComputedColumnsWithTransactionSyncMethod()
        {
            using var sqlConn = SqlConnectionHelper.NewConnection();
            using var sqlTransaction = sqlConn.BeginTransaction();

            var computedColumnTestTableName = "[dbo].[SqlBulkHelpersComputedColumnSchemaTest]";

            //Validate Computed Columnd details with the BasicDetails of the Schema...
            var tableDefinitionBasciDetails = sqlTransaction.GetTableSchemaDefinition(
                computedColumnTestTableName,
                TableSchemaDetailLevel.BasicDetails
            );

            AssertTableDefinitionIsValidForComputedColumnsTable(computedColumnTestTableName, tableDefinitionBasciDetails);

            //Validate Computed Columnd details with the ExtendedDetails of the Schema...
            var tableDefinitionExtendedDetails = sqlTransaction.GetTableSchemaDefinition(
                computedColumnTestTableName,
                TableSchemaDetailLevel.ExtendedDetails
            );

            AssertTableDefinitionIsValidForComputedColumnsTable(computedColumnTestTableName, tableDefinitionExtendedDetails);
        }

        [TestMethod]
        public void TestTableDefinitionLoadingExtendedDetailsSyncMethods()
        {
            using var sqlConn = SqlConnectionHelper.NewConnection();
            var tableDefinition = sqlConn.GetTableSchemaDefinition(
                TestHelpers.TestTableNameFullyQualified,
                TableSchemaDetailLevel.ExtendedDetails
            );

            AssertTableDefinitionIsValidForTestElementParentTable(tableDefinition, TableNameTerm.From<object>(TestHelpers.TestTableNameFullyQualified), TableSchemaDetailLevel.ExtendedDetails);
        }

        [TestMethod]
        public async Task TestTableDefinitionLoadingBasicDetailsWithTransactionAsync()
        {
            var sqlConnectionProvider = SqlConnectionHelper.GetConnectionProvider();
            await using var sqlConn = await sqlConnectionProvider.NewConnectionAsync().ConfigureAwait(false);
            await using var sqlTransaction = (SqlTransaction)await sqlConn.BeginTransactionAsync();

            var tableDefinition = await sqlTransaction.GetTableSchemaDefinitionAsync(
                TestHelpers.TestTableNameFullyQualified,
                TableSchemaDetailLevel.BasicDetails
            ).ConfigureAwait(false);

            AssertTableDefinitionIsValidForTestElementParentTable(tableDefinition, TableNameTerm.From<object>(TestHelpers.TestTableNameFullyQualified), TableSchemaDetailLevel.BasicDetails);
        }

        [TestMethod]
        public async Task TestTableDefinitionLoadingBasicDetailsForTempTableWithTransactionAsync()
        {
            var sqlConnectionProvider = SqlConnectionHelper.GetConnectionProvider();
            await using var sqlConn = await sqlConnectionProvider.NewConnectionAsync().ConfigureAwait(false);
            await using var sqlTransaction = (SqlTransaction)await sqlConn.BeginTransactionAsync();

            var tempTableName = "#TempTableSchemaLoaderTest";
            await sqlConn.ExecuteNonQueryAsync($@"
                CREATE TABLE [{tempTableName}] ([Id] INT NOT NULL PRIMARY KEY);
            ", transaction: sqlTransaction);

            var tableDefinition = await sqlTransaction.GetTableSchemaDefinitionAsync(
                tempTableName,
                TableSchemaDetailLevel.BasicDetails
            ).ConfigureAwait(false);

            AssertTableDefinitionIsValidForTempTable(tempTableName, tableDefinition);
        }

        [TestMethod]
        public async Task TestTableDefinitionLoadingForTableWithComputedColumnsWithTransactionAsync()
        {
            var sqlConnectionProvider = SqlConnectionHelper.GetConnectionProvider();
            await using var sqlConn = await sqlConnectionProvider.NewConnectionAsync().ConfigureAwait(false);
            await using var sqlTransaction = (SqlTransaction)await sqlConn.BeginTransactionAsync();

            var computedColumnTestTableName = "[dbo].[SqlBulkHelpersComputedColumnSchemaTest]";
            var tableDefinition = await sqlTransaction.GetTableSchemaDefinitionAsync(
                computedColumnTestTableName,
                TableSchemaDetailLevel.BasicDetails
            ).ConfigureAwait(false);

            AssertTableDefinitionIsValidForComputedColumnsTable(computedColumnTestTableName, tableDefinition);

            //Validate Computed Columnd details with the ExtendedDetails of the Schema...
            var tableDefinitionExtendedDetails = await sqlTransaction.GetTableSchemaDefinitionAsync(
                computedColumnTestTableName,
                TableSchemaDetailLevel.ExtendedDetails
            ).ConfigureAwait(false);

            AssertTableDefinitionIsValidForComputedColumnsTable(computedColumnTestTableName, tableDefinitionExtendedDetails);

        }


        [TestMethod]
        public async Task TestTableDefinitionLoadingExtendedDetailsAsync()
        {
            var sqlConnectionProvider = SqlConnectionHelper.GetConnectionProvider();
            await using var sqlConn = await sqlConnectionProvider.NewConnectionAsync().ConfigureAwait(false);

            var tableDefinition = await sqlConn.GetTableSchemaDefinitionAsync(
                TestHelpers.TestTableNameFullyQualified,
                TableSchemaDetailLevel.ExtendedDetails
            ).ConfigureAwait(false);

            AssertTableDefinitionIsValidForTestElementParentTable(tableDefinition, TableNameTerm.From<object>(TestHelpers.TestTableNameFullyQualified), TableSchemaDetailLevel.ExtendedDetails);
            Assert.IsEmpty(tableDefinition.ForeignKeyConstraints);
            Assert.HasCount(1, tableDefinition.ReferencingForeignKeyConstraints);

            var childTableDefinition = await sqlConn.GetTableSchemaDefinitionAsync(
                TestHelpers.TestChildTableNameFullyQualified,
                TableSchemaDetailLevel.ExtendedDetails
            ).ConfigureAwait(false);

            AssertTableDefinitionIsValidForTestElementParentTable(childTableDefinition, TableNameTerm.From<object>(TestHelpers.TestChildTableNameFullyQualified), TableSchemaDetailLevel.ExtendedDetails);
            Assert.IsEmpty(childTableDefinition.ReferencingForeignKeyConstraints);
            Assert.HasCount(1, childTableDefinition.ForeignKeyConstraints);

            var childKeyColumn = childTableDefinition.FindColumnCaseInsensitive("ChildKey");
            Assert.IsTrue(childKeyColumn.DataType.Equals("nvarchar", StringComparison.OrdinalIgnoreCase));
            Assert.AreEqual(250, childKeyColumn.CharacterMaxLength);
        }

        private void AssertTableDefinitionIsValidForTestElementParentTable(SqlBulkHelpersTableDefinition tableDefinition, TableNameTerm tableNameTerm, TableSchemaDetailLevel expectedDetailLevel)
        {
            Assert.IsNotNull(tableDefinition);

            Assert.AreEqual(expectedDetailLevel, tableDefinition.SchemaDetailLevel);
            Assert.AreEqual(tableNameTerm.SchemaName, tableDefinition.TableSchema);
            Assert.AreEqual(tableNameTerm.TableName, tableDefinition.TableName);
            Assert.AreEqual(tableNameTerm.FullyQualifiedTableName, tableDefinition.TableFullyQualifiedName);
            Assert.IsGreaterThan(2, tableDefinition.TableColumns.Count);
            Assert.IsNotNull(tableDefinition.PrimaryKeyConstraint);
        }

        private void AssertTableDefinitionIsValidForTempTable(string tempTableName, SqlBulkHelpersTableDefinition tableDefinition)
        {
            Assert.IsNotNull(tableDefinition);

            var tableNameTerm = TableNameTerm.From(tempTableName);
            Assert.IsTrue(tableNameTerm.IsTempTableName);
            Assert.AreEqual(TableSchemaDetailLevel.BasicDetails, tableDefinition.SchemaDetailLevel);
            Assert.AreEqual(tableNameTerm.SchemaName, tableDefinition.TableSchema);
            //NOTE: Table Names will not match exactly due to internal Hashing of the Temp Name for Session isolation, etc...
            //Assert.AreEqual(tableNameTerm.TableName, tableDefinition.TableName);
            //Assert.AreEqual(tableNameTerm.FullyQualifiedTableName, tableDefinition.TableFullyQualifiedName);
            Assert.HasCount(1, tableDefinition.TableColumns);
            Assert.IsNotNull(tableDefinition.PrimaryKeyConstraint);
            Assert.IsEmpty(tableDefinition.ForeignKeyConstraints);
        }

        private void AssertTableDefinitionIsValidForComputedColumnsTable(string tempTableName, SqlBulkHelpersTableDefinition tableDefinition)
        {
            Assert.IsNotNull(tableDefinition);

            var tableNameTerm = TableNameTerm.From(tempTableName);
            Assert.IsFalse(tableNameTerm.IsTempTableName);
            Assert.AreEqual(tableNameTerm.SchemaName, tableDefinition.TableSchema);
            Assert.IsNotNull(tableDefinition.PrimaryKeyConstraint);
            Assert.IsEmpty(tableDefinition.ForeignKeyConstraints);

            //Validate Computed Columns Exist as expected!
            Assert.HasCount(2, tableDefinition.TableColumns.Where(c => c.IsComputedColumn));
            Assert.IsGreaterThan(2, tableDefinition.TableColumns.Count);

            //Computed Columsn should exist...
            var computedColumnDefs = new List<TableColumnDefinition> {
                tableDefinition.FindColumnCaseInsensitive("PartNumberNormalized"),
                tableDefinition.FindColumnCaseInsensitive("SupplierPartNumberNormalized")
            };

            computedColumnDefs.ForEach(c =>
            {
                Assert.IsNotNull(c);
                Assert.IsTrue(c.IsComputedColumn);
                Assert.IsNotEmpty(c.ComputedColumnDefinition);
                Assert.IsTrue(c.IsPersistedColumn);
            });

            //Computed Columns should NOT be udatable (not returned by FindUpdatableColumn method)...
            Assert.IsNull(tableDefinition.FindUpdatableColumnCaseInsensitive("PartNumberNormalized"));
            Assert.IsNull(tableDefinition.FindUpdatableColumnCaseInsensitive("SupplierPartNumberNormalized"));
        }

        [TestMethod]
        public void TestTableDefinitionLoadingAndCachingSyncMethods()
        {
            const int TestCount = 5;

            var loadedDefinitions = new List<SqlBulkHelpersTableDefinition>();
            for (var x = 0; x < TestCount; x++)
            {
                using var sqlConn = SqlConnectionHelper.NewConnection();
                var tableDefinition = sqlConn.GetTableSchemaDefinition(TestHelpers.TestTableNameFullyQualified);
                
                Assert.IsNotNull(tableDefinition);

                loadedDefinitions.Add(tableDefinition);
            }

            var uniqueDefinitions = loadedDefinitions.Distinct().ToList();
            Assert.AreEqual(1, uniqueDefinitions.Count);
        }

        [TestMethod]
        public async Task TestTableDefinitionLoadingAndCachingAsync()
        {
            const int TestCount = 5;
            var sqlConnectionProvider = SqlConnectionHelper.GetConnectionProvider();

            var loadedDefinitions = new List<SqlBulkHelpersTableDefinition>();
            var timer = new Stopwatch();
            for (var x = 0; x < TestCount; x++)
            {
                if(x > 0 & !timer.IsRunning) timer.Restart();

                await using var sqlConn = await sqlConnectionProvider.NewConnectionAsync().ConfigureAwait(false);
                var tableDefinition = await sqlConn.GetTableSchemaDefinitionAsync(TestHelpers.TestTableNameFullyQualified);

                Assert.IsNotNull(tableDefinition);

                loadedDefinitions.Add(tableDefinition);
            }

            timer.Stop();
            var uniqueDefinitions = loadedDefinitions.Distinct().ToList();
            Assert.AreEqual(1, uniqueDefinitions.Count);
            Assert.IsTrue(timer.ElapsedMilliseconds <= 1);
        }
    }
}
