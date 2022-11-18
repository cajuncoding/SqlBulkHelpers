using Microsoft.VisualStudio.TestTools.UnitTesting;
using SqlBulkHelpers.Tests;
using SqlBulkHelpers.MaterializedData;
using Microsoft.Data.SqlClient;
using System.Linq;
using System.Threading.Tasks;

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
                var cloneInfo = await transaction.CloneTableAsync(TestHelpers.TestTableNameFullyQualified, targetTableNameTerm, true);

                transaction.Commit();

                //ASSERT Results are Valid...
                Assert.IsNotNull(cloneInfo);
            }
        }
    }
}
