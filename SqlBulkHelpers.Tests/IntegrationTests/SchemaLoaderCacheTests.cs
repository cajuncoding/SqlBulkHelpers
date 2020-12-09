using Microsoft.VisualStudio.TestTools.UnitTesting;
using SqlBulkHelpers.Tests;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace SqlBulkHelpers.IntegrationTests
{
    [TestClass]
    public class SchemaLoaderCacheTests
    {
        [TestMethod]
        public void TestSchemaLoaderCacheWithLazyLoading()
        {
            ISqlBulkHelpersConnectionProvider sqlConnectionProvider = SqlBulkHelpersConnectionProvider.Default;

            List<ISqlBulkHelpersDBSchemaLoader> schemaLoadersList = new List<ISqlBulkHelpersDBSchemaLoader>
            {
                SqlBulkHelpersSchemaLoaderCache.GetSchemaLoader(sqlConnectionProvider),
                SqlBulkHelpersSchemaLoaderCache.GetSchemaLoader(sqlConnectionProvider),
                SqlBulkHelpersSchemaLoaderCache.GetSchemaLoader(sqlConnectionProvider)
            };

            Assert.IsNotNull(schemaLoadersList[0]);
            Assert.IsNotNull(schemaLoadersList[1]);
            Assert.IsNotNull(schemaLoadersList[2]);

            Assert.AreEqual(schemaLoadersList[0], schemaLoadersList[1]);
            Assert.AreEqual(schemaLoadersList[1], schemaLoadersList[2]);
            Assert.AreEqual(schemaLoadersList[0], schemaLoadersList[2]);

            //NOTE: We can't actually test this since it's Cached and the TestFramework may have already
            //  initialized the Schema Definitions for the connection from other tests!
            //Assert.IsFalse(((SqlBulkHelpersDBSchemaStaticLoader)schemaLoadersList[0]).IsInitialized);
            schemaLoadersList[1].InitializeSchemaDefinitions();

            Assert.IsTrue(((SqlBulkHelpersDBSchemaStaticLoader)schemaLoadersList[0]).IsInitialized);
            Assert.IsTrue(((SqlBulkHelpersDBSchemaStaticLoader)schemaLoadersList[1]).IsInitialized);
            Assert.IsTrue(((SqlBulkHelpersDBSchemaStaticLoader)schemaLoadersList[2]).IsInitialized);
        }

        [TestMethod]
        public async Task TestSchemaLoaderCacheWithExistingConnectionAndImmediateLoadingAsync()
        {
            ISqlBulkHelpersConnectionProvider sqlConnectionProvider = SqlBulkHelpersConnectionProvider.Default;

            List<ISqlBulkHelpersDBSchemaLoader> schemaLoadersList = new List<ISqlBulkHelpersDBSchemaLoader>();

            using (var conn = await sqlConnectionProvider.NewConnectionAsync())
            {
                schemaLoadersList.Add(SqlBulkHelpersSchemaLoaderCache.GetSchemaLoader(conn));
            }

            using (var conn = await sqlConnectionProvider.NewConnectionAsync())
            {
                schemaLoadersList.Add(SqlBulkHelpersSchemaLoaderCache.GetSchemaLoader(conn));
            }

            using (var conn = await sqlConnectionProvider.NewConnectionAsync())
            {
                schemaLoadersList.Add(SqlBulkHelpersSchemaLoaderCache.GetSchemaLoader(conn));
            }

            Assert.IsNotNull(schemaLoadersList[0]);
            Assert.IsNotNull(schemaLoadersList[1]);
            Assert.IsNotNull(schemaLoadersList[2]);

            Assert.AreEqual(schemaLoadersList[0], schemaLoadersList[1]);
            Assert.AreEqual(schemaLoadersList[1], schemaLoadersList[2]);
            Assert.AreEqual(schemaLoadersList[0], schemaLoadersList[2]);

            //ALL should already be initialized since used existing connections to construct them!
            Assert.IsTrue(((SqlBulkHelpersDBSchemaStaticLoader)schemaLoadersList[0]).IsInitialized);
            Assert.IsTrue(((SqlBulkHelpersDBSchemaStaticLoader)schemaLoadersList[1]).IsInitialized);
            Assert.IsTrue(((SqlBulkHelpersDBSchemaStaticLoader)schemaLoadersList[2]).IsInitialized);
        }
    }
}
