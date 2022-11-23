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
            using (SqlTransaction sqlTransaction = conn.BeginTransaction())
            {
                var targetTableNameTerm = TableNameTerm.From(TestHelpers.MaterializeDataLoadingSchema, TestHelpers.TestTableName);
                var cloneInfo = await sqlTransaction.CloneTableAsync(TestHelpers.TestTableNameFullyQualified, targetTableNameTerm);

                sqlTransaction.Commit();

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
            using (SqlTransaction sqlTransaction = conn.BeginTransaction())
            {
                var targetTableNameTerm = TableNameTerm.From(TestHelpers.MaterializeDataLoadingSchema, TestHelpers.TestTableName);
                //First Clone and force re-creation to make this Test Idempotent!
                var successfulCloneInfo = await sqlTransaction.CloneTableAsync(TestHelpers.TestTableNameFullyQualified, targetTableNameTerm, recreateIfExists: true);
                
                CloneTableInfo? failedCloneInfo = null;
                Exception failedToCloneException = null;
                try
                {
                    //Second attempt the clone again but this time expecting it to now already exist and fail out!
                    failedCloneInfo = await sqlTransaction.CloneTableAsync(TestHelpers.TestTableNameFullyQualified, targetTableNameTerm, recreateIfExists: false);
                }
                catch (Exception cloneExc)
                {
                    failedToCloneException = cloneExc;
                }

                //Now Clean up the Cloned Table...
                //TODO: Consider adding an API facilitate Drop Table do this...
                await sqlTransaction.DropTableAsync(successfulCloneInfo.TargetTable);

                //ASSERT Results are Valid...
                Assert.IsNotNull(successfulCloneInfo);
                Assert.IsNull(failedCloneInfo);
                Assert.IsNotNull(failedToCloneException);
            }
        }
    }
}
