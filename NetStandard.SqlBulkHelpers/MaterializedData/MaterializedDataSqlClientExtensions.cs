using Microsoft.Data.SqlClient;
using System;
using System.Collections.Generic;
using System.Linq;
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
            sqlTransaction.AssertArgumentIsNotNull(nameof(sqlTransaction));

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
            sqlTransaction.AssertArgumentIsNotNull(nameof(sqlTransaction));

            var results = await new MaterializeDataHelper<T>(sqlTransaction, bulkHelpersConfig)
                .CloneTableStructureAsync(sqlTransaction, sourceTableNameOverride, targetTableNameOverride, recreateIfExists)
                .ConfigureAwait(false);

            return results;
        }

        public static Task<TableNameTerm[]> DropTableAsync(
            this SqlTransaction sqlTransaction,
            string tableName,
            ISqlBulkHelpersConfig bulkHelpersConfig = null
        ) => DropTablesAsync(sqlTransaction, new[] { tableName }, bulkHelpersConfig);

        public static async Task<TableNameTerm[]> DropTablesAsync(
            this SqlTransaction sqlTransaction,
            IEnumerable<string> tableNames,
            ISqlBulkHelpersConfig bulkHelpersConfig = null
        )
        {
            sqlTransaction.AssertArgumentIsNotNull(nameof(sqlTransaction));

            var results = await new MaterializeDataHelper<ISkipMappingLookup>(sqlTransaction, bulkHelpersConfig)
                .DropTablesAsync(sqlTransaction, tableNames.ToArray())
                .ConfigureAwait(false);

            return results;
        }

        public static async Task<TableNameTerm[]> DropTableAsync<T>(
            this SqlTransaction sqlTransaction,
            string tableNameOverride = null,
            ISqlBulkHelpersConfig bulkHelpersConfig = null
        ) where T : class
        {
            sqlTransaction.AssertArgumentIsNotNull(nameof(sqlTransaction));

            var results = await new MaterializeDataHelper<T>(sqlTransaction, bulkHelpersConfig)
                .DropTableAsync(sqlTransaction, tableNameOverride)
                .ConfigureAwait(false);

            return results;
        }

    }
}
