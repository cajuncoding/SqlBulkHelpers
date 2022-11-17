using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using SqlBulkHelpers.SqlBulkHelpers;

namespace SqlBulkHelpers.Tests.IntegrationTests
{
    [TestClass]
    public class SchemaLoaderTests
    {

        [TestMethod]
        public void TestTableDefinitionLoading()
        {
            using var sqlConn = SqlConnectionHelper.NewConnection();

            var dbSchemaLoader = SqlBulkHelpersSchemaLoaderCache.GetSchemaLoader(sqlConn.ConnectionString);

            var tableDefinition = dbSchemaLoader.GetTableSchemaDefinition(
                TestHelpers.TestTableNameFullyQualified,
                sqlConnectionFactory: () => sqlConn
            );

            Assert.IsNotNull(tableDefinition);

            var tableNameTerm = TableNameTerm.From<object>(TestHelpers.TestTableNameFullyQualified);
            Assert.AreEqual(tableNameTerm.SchemaName, tableDefinition.TableSchema);
            Assert.AreEqual(tableNameTerm.TableName, tableDefinition.TableName);
            Assert.AreEqual(tableNameTerm.FullyQualifiedTableName, tableDefinition.TableFullyQualifiedName);
            
            Assert.IsTrue(tableDefinition.KeyConstraints.Count > 0);
            Assert.IsTrue(tableDefinition.TableColumns.Count > 0);

            Assert.IsNotNull(tableDefinition.PrimaryKeyConstraint);
            Assert.IsNotNull(tableDefinition.IdentityColumn);
        }

        [TestMethod]
        public void TestTableDefinitionLoadingFromConvenienceMethod()
        {
            using var sqlConn = SqlConnectionHelper.NewConnection();

            var tableDefinition = sqlConn.GetTableSchemaDefinition(TestHelpers.TestTableNameFullyQualified);

            Assert.IsNotNull(tableDefinition);

            var tableNameTerm = TableNameTerm.From<object>(TestHelpers.TestTableNameFullyQualified);
            Assert.AreEqual(tableNameTerm.SchemaName, tableDefinition.TableSchema);
            Assert.AreEqual(tableNameTerm.TableName, tableDefinition.TableName);
            Assert.AreEqual(tableNameTerm.FullyQualifiedTableName, tableDefinition.TableFullyQualifiedName);

            Assert.IsTrue(tableDefinition.KeyConstraints.Count > 0);
            Assert.IsTrue(tableDefinition.TableColumns.Count > 0);

            Assert.IsNotNull(tableDefinition.PrimaryKeyConstraint);
            Assert.IsNotNull(tableDefinition.IdentityColumn);
        }

        [TestMethod]
        public void TestTableDefinitionLoadingWithConvenienceMethodAndCaching()
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
    }
}
