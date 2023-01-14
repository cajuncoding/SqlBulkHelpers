using System;
using System.Diagnostics;
using Microsoft.Data.SqlClient;
using SqlBulkHelpers.SqlBulkHelpers;

namespace SqlBulkHelpers.Tests.IntegrationTests
{
    [TestClass]
    public class SchemaLoaderTests : BaseTest
    {
        [TestMethod]
        public void TestTableDefinitionLoadingBasicDetailsWithTransaction()
        {
            using var sqlConn = SqlConnectionHelper.NewConnection();
            using var sqlTransaction = sqlConn.BeginTransaction();

            var tableDefinition = sqlTransaction.GetTableSchemaDefinition(
                TestHelpers.TestTableNameFullyQualified,
                TableSchemaDetailLevel.BasicDetails
            );

            AssertTableDefinitionIsValidForTestElementParentTable(tableDefinition, TableSchemaDetailLevel.BasicDetails);
        }

        [TestMethod]
        public void TestTableDefinitionLoadingExtendedDetails()
        {
            using var sqlConn = SqlConnectionHelper.NewConnection();
            var tableDefinition = sqlConn.GetTableSchemaDefinition(
                TestHelpers.TestTableNameFullyQualified,
                TableSchemaDetailLevel.ExtendedDetails
            );

            AssertTableDefinitionIsValidForTestElementParentTable(tableDefinition, TableSchemaDetailLevel.ExtendedDetails);
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

            AssertTableDefinitionIsValidForTestElementParentTable(tableDefinition, TableSchemaDetailLevel.BasicDetails);
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

            AssertTableDefinitionIsValidForTestElementParentTable(tableDefinition, TableSchemaDetailLevel.ExtendedDetails);
        }

        private void AssertTableDefinitionIsValidForTestElementParentTable(SqlBulkHelpersTableDefinition tableDefinition, TableSchemaDetailLevel expectedDetailLevel)
        {
            Assert.IsNotNull(tableDefinition);

            var tableNameTerm = TableNameTerm.From<object>(TestHelpers.TestTableNameFullyQualified);
            Assert.AreEqual(expectedDetailLevel, tableDefinition.SchemaDetailLevel);
            Assert.AreEqual(tableNameTerm.SchemaName, tableDefinition.TableSchema);
            Assert.AreEqual(tableNameTerm.TableName, tableDefinition.TableName);
            Assert.AreEqual(tableNameTerm.FullyQualifiedTableName, tableDefinition.TableFullyQualifiedName);
            Assert.AreEqual(3, tableDefinition.TableColumns.Count);
            Assert.IsNotNull(tableDefinition.PrimaryKeyConstraint);
            Assert.IsNotNull(tableDefinition.IdentityColumn);
            Assert.AreEqual(0, tableDefinition.ForeignKeyConstraints.Count);

            //EXTENDED Details includes FKeys and Referencing Keys...
            if (expectedDetailLevel == TableSchemaDetailLevel.ExtendedDetails)
            {
                Assert.AreEqual(0, tableDefinition.ForeignKeyConstraints.Count);
                Assert.AreEqual(1, tableDefinition.ReferencingForeignKeyConstraints.Count);
            }
        }

        [TestMethod]
        public void TestTableDefinitionLoadingAndCaching()
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
