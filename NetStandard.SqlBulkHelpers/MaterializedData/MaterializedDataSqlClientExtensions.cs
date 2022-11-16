using Microsoft.Data.SqlClient;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using SqlBulkHelpers.MaterializedData;

namespace SqlBulkHelpers.SqlBulkHelpers.MaterializedData
{
    public static class MaterializedDataSqlClientExtensions
    {
        public static async Task<CloneTableInfo> CloneTableAsync<T>(
            this SqlTransaction sqlTransaction,
            string tableName = null,
            ISqlBulkHelpersConfig bulkHelpersConfig = null
        ) where T : class
        {
            var results = await new MaterializeDataHelper<T>(sqlTransaction, bulkHelpersConfig)
                .CloneTableStructureAsync(sqlTransaction, tableName)
                .ConfigureAwait(false);

            return results;
        }


    }
}
