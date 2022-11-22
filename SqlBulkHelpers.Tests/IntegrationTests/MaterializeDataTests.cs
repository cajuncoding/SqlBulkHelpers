using System;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using SqlBulkHelpers.Tests;
using SqlBulkHelpers.MaterializedData;
using Microsoft.Data.SqlClient;
using System.Threading.Tasks;
using RepoDb;

namespace SqlBulkHelpers.IntegrationTests
{
    [TestClass]
    public class MaterializeDataTests
    {
        [TestMethod]
        public async Task TestCloneTableStructureAsync()
        {
            var sqlConnectionString = SqlConnectionHelper.GetSqlConnectionString();
            ISqlBulkHelpersConnectionProvider sqlConnectionProvider = new SqlBulkHelpersConnectionProvider(sqlConnectionString);

            using (var conn = await sqlConnectionProvider.NewConnectionAsync())
            using (SqlTransaction transaction = conn.BeginTransaction())
            {
                var targetTableNameTerm = TableNameTerm.From(TestHelpers.MaterializeDataLoadingSchema, TestHelpers.TestTableName);
                var cloneInfo = await transaction.CloneTableAsync(TestHelpers.TestTableNameFullyQualified, targetTableNameTerm);

                transaction.Commit();

                //ASSERT Results are Valid...
                Assert.IsNotNull(cloneInfo);
            }
        }

        [TestMethod]
        public async Task TestCloneTableStructureFailureIfExistsAsync()
        {
            var sqlConnectionString = SqlConnectionHelper.GetSqlConnectionString();
            ISqlBulkHelpersConnectionProvider sqlConnectionProvider = new SqlBulkHelpersConnectionProvider(sqlConnectionString);

            using (var conn = await sqlConnectionProvider.NewConnectionAsync())
            using (SqlTransaction transaction = conn.BeginTransaction())
            {
                var targetTableNameTerm = TableNameTerm.From(TestHelpers.MaterializeDataLoadingSchema, TestHelpers.TestTableName);
                //First Clone and force re-creation to make this Test Idempotent!
                var successfulCloneInfo = await transaction.CloneTableAsync(TestHelpers.TestTableNameFullyQualified, targetTableNameTerm, recreateIfExists: true);
                
                //NOTE: We have to Commit in order to detect if the Table actually exists...
                transaction.Commit();

                CloneTableInfo? failedCloneInfo = null;
                Exception failedToCloneException = null;
                try
                {
                    using (var transaction2 = conn.BeginTransaction())
                    {
                        //Second attempt the clone again but this time expecting it to now already exist and fail out!
                        failedCloneInfo = await transaction2.CloneTableAsync(TestHelpers.TestTableNameFullyQualified, targetTableNameTerm, recreateIfExists: false);
                    }
                }
                catch (Exception cloneExc)
                {
                    failedToCloneException = cloneExc;
                }

                //Now Clean up the Cloned Table...
                await conn.ExecuteNonQueryAsync($"DROP TABLE {successfulCloneInfo.TargetTable};");

                //ASSERT Results are Valid...
                Assert.IsNotNull(successfulCloneInfo);
                Assert.IsNotNull(failedCloneInfo);
                Assert.IsNull(failedToCloneException);
            }
        }
    }
}
