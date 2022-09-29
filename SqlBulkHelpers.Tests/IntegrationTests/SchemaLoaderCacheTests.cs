using System;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using SqlBulkHelpers.Tests;
using System.Collections.Generic;
using System.Linq;
using System.Security.Cryptography.X509Certificates;
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

        [TestMethod]
        public void TestSchemaLoaderCacheWithBadConnectionDueToPendingTransactionFor_v1_2()
        {
            //****************************************************************************************************
            // Check that Invalid Connection Fails as expected and Lazy continues to re-throw the Exception!!!
            //****************************************************************************************************
            using var sqlConnInvalidWithTransaction = SqlConnectionHelper.NewConnection();
            //START a Pending Transaction which will NOT be available to the DBSchemaLoader (as of v1.2)!
            using var sqlTrans = sqlConnInvalidWithTransaction.BeginTransaction();

            var dbSchemaLoaderFromFactoryFuncInvalid = SqlBulkHelpersSchemaLoaderCache.GetSchemaLoader(
                new SqlBulkHelpersConnectionProvider(
                    $"SQL_INVALID_CONNECTION_CACHE_KEY::{Guid.NewGuid()}",
                    () => sqlConnInvalidWithTransaction
                )
            );

            Assert.IsNotNull(dbSchemaLoaderFromFactoryFuncInvalid);
            Assert.IsFalse(((SqlBulkHelpersDBSchemaLoader)dbSchemaLoaderFromFactoryFuncInvalid).IsInitialized);

            List<Exception> exceptions = new();
            var loopCount = 3;

            for (int x = 0; x < loopCount; x++)
            {
                try
                {
                    //Initial Call should result in SQL Exception due to Pending Transaction...
                    var tableDefinitions = dbSchemaLoaderFromFactoryFuncInvalid.InitializeSchemaDefinitions();
                    Assert.IsTrue(((SqlBulkHelpersDBSchemaLoader)dbSchemaLoaderFromFactoryFuncInvalid).IsInitialized);
                    Assert.IsTrue(tableDefinitions.Count > 0);
                }
                catch (Exception exc)
                {
                    exceptions.Add(exc);
                }
            }

            var firstExc = exceptions.FirstOrDefault();
            Assert.AreEqual(loopCount, exceptions.Count);
            Assert.IsTrue(exceptions.TrueForAll(
                //Assert that ALL Exceptions (HResult, Message) are identical!
                (exc) => exc.HResult == firstExc.HResult && exc.Message == firstExc.Message
            ));

            //****************************************************************************************************
            // Check that New Connection works even with Old in Scope!
            //****************************************************************************************************
            //Create a NEW Connection now that is Valid without the Transaction (but the originals are STILL IN SCOPE...
            using var sqlConnOkNewConnection = SqlConnectionHelper.NewConnection();

            var dbSchemaLoaderFromFactoryFuncOk = SqlBulkHelpersSchemaLoaderCache.GetSchemaLoader(
                new SqlBulkHelpersConnectionProvider(
                    $"SQL_VALID_CONNECTION_CUSTOM_CACHE_KEY::{Guid.NewGuid()}",
                    () => sqlConnOkNewConnection
                )
            );

            //Initial Call should result in SQL Exception due to Pending Transaction...
            var tableDefinitionsSuccessful = dbSchemaLoaderFromFactoryFuncOk.InitializeSchemaDefinitions();
            Assert.IsTrue(((SqlBulkHelpersDBSchemaLoader)dbSchemaLoaderFromFactoryFuncOk).IsInitialized);
            Assert.IsTrue(tableDefinitionsSuccessful.Count > 0);
        }
    }
}
