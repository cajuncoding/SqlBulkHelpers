using Microsoft.Data.SqlClient;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using SqlBulkHelpers.MaterializedData;

namespace SqlBulkHelpers.MaterializedData
{
    public static class MaterializedDataSqlClientExtensions
    {
        public static async Task<CloneTableInfo> CloneTableAsync(
            this SqlTransaction sqlTransaction,
            string sourceTableName,
            string targetTableName,
            bool recreateIfExists = true,
            ISqlBulkHelpersConfig bulkHelpersConfig = null
        )
        {
            var results = await new MaterializeDataHelper<ISkipMappingLookup>(sqlTransaction, bulkHelpersConfig)
                .CloneTableStructureAsync(sqlTransaction, sourceTableName, targetTableName, recreateIfExists)
                .ConfigureAwait(false);

            return results;
        }

        public static async Task<CloneTableInfo> CloneTableAsync<T>(
            this SqlTransaction sqlTransaction,
            string sourceTableNameOverride = null,
            string targetTableNameOverride = null,
            bool recreateIfExists = true,
            ISqlBulkHelpersConfig bulkHelpersConfig = null
        ) where T : class
        {
            var results = await new MaterializeDataHelper<T>(sqlTransaction, bulkHelpersConfig)
                .CloneTableStructureAsync(sqlTransaction, sourceTableNameOverride, targetTableNameOverride, recreateIfExists)
                .ConfigureAwait(false);

            return results;
        }


    }
}
