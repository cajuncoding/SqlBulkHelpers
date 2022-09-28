using System;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using SqlBulkHelpers.Tests;
using System.Collections.Generic;
using System.Threading.Tasks;
using Microsoft.Data.SqlClient;

namespace SqlBulkHelpers.IntegrationTests
{
    [TestClass]
    public class SchemaLoaderCacheTests
    {
        [TestMethod]
        public void TestSchemaLoaderCacheWithLazyLoadingFromMultipleConnectionProviders()
        {
            ISqlBulkHelpersConnectionProvider sqlConnectionProvider = SqlConnectionHelper.GetConnectionProvider();

            List<ISqlBulkHelpersDBSchemaLoader> schemaLoadersList = new List<ISqlBulkHelpersDBSchemaLoader>
            {
                SqlBulkHelpersSchemaLoaderCache.GetSchemaLoader(sqlConnectionProvider), //0
                SqlBulkHelpersSchemaLoaderCache.GetSchemaLoader(sqlConnectionProvider), //1
                SqlBulkHelpersSchemaLoaderCache.GetSchemaLoader(sqlConnectionProvider), //2
                SqlBulkHelpersSchemaLoaderCache.GetSchemaLoader(new SqlBulkHelpersConnectionProvider( //3
                    "SECOND_CONNECTION_TEST", 
                    () => new SqlConnection(SqlConnectionHelper.GetSqlConnectionString())
                )),
            };

            Assert.IsNotNull(schemaLoadersList[0]);
            Assert.IsNotNull(schemaLoadersList[1]);
            Assert.IsNotNull(schemaLoadersList[2]);
            Assert.IsNotNull(schemaLoadersList[3]);

            Assert.AreEqual(schemaLoadersList[0], schemaLoadersList[1]);
            Assert.AreEqual(schemaLoadersList[1], schemaLoadersList[2]);
            Assert.AreEqual(schemaLoadersList[0], schemaLoadersList[2]);
            Assert.AreNotEqual(schemaLoadersList[2], schemaLoadersList[3]);

            schemaLoadersList[1].InitializeSchemaDefinitions();
            Assert.IsTrue(((SqlBulkHelpersDBSchemaLoader)schemaLoadersList[0]).IsInitialized);
            Assert.IsTrue(((SqlBulkHelpersDBSchemaLoader)schemaLoadersList[1]).IsInitialized);
            Assert.IsTrue(((SqlBulkHelpersDBSchemaLoader)schemaLoadersList[2]).IsInitialized);

            //Validate that the second connection was never initialized!
            var secondConnectionSchemaLoader = (SqlBulkHelpersDBSchemaLoader)schemaLoadersList[3];
            Assert.IsFalse(secondConnectionSchemaLoader.IsInitialized);
        }

        [TestMethod]
        public async Task TestSchemaLoaderCacheWithExistingConnectionAndImmediateLoadingAsync()
        {
            ISqlBulkHelpersConnectionProvider sqlConnectionProvider = SqlConnectionHelper.GetConnectionProvider();

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
            Assert.IsTrue(((SqlBulkHelpersDBSchemaLoader)schemaLoadersList[0]).IsInitialized);
            Assert.IsTrue(((SqlBulkHelpersDBSchemaLoader)schemaLoadersList[1]).IsInitialized);
            Assert.IsTrue(((SqlBulkHelpersDBSchemaLoader)schemaLoadersList[2]).IsInitialized);
        }

        [TestMethod]
        public void TestSchemaLoaderCacheFromConnectionFactoryInitializationAsync()
        {
            string sqlConnectionString = SqlConnectionHelper.GetSqlConnectionString();

            var dbSchemaLoaderFromFactoryFunc = SqlBulkHelpersSchemaLoaderCache.GetSchemaLoader(
                new SqlBulkHelpersConnectionProvider(
                $"SQL_CONNECTION_CUSTOM_CACHE_KEY::{Guid.NewGuid()}",
                () => new SqlConnection(sqlConnectionString)
                )
            );

            Assert.IsNotNull(dbSchemaLoaderFromFactoryFunc);
            Assert.IsFalse(((SqlBulkHelpersDBSchemaLoader)dbSchemaLoaderFromFactoryFunc).IsInitialized);

            var tableDefinitions = dbSchemaLoaderFromFactoryFunc.InitializeSchemaDefinitions();
            Assert.IsTrue(((SqlBulkHelpersDBSchemaLoader)dbSchemaLoaderFromFactoryFunc).IsInitialized);
            Assert.IsTrue(tableDefinitions.Count > 0);
        }
    }
}
