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
        public static Task ExecuteMaterializedDataSqlScriptAsync(this SqlTransaction sqlTransaction, MaterializedDataScriptBuilder sqlScriptBuilder, int? commandTimeout = null)
            => ExecuteMaterializedDataSqlScriptAsync(sqlTransaction, sqlScriptBuilder.BuildSqlScript(), commandTimeout);

        public static async Task ExecuteMaterializedDataSqlScriptAsync(this SqlTransaction sqlTransaction, string materializedDataSqlScript, int? commandTimeout = null)
        {
            sqlTransaction.AssertArgumentIsNotNull(nameof(sqlTransaction));
            materializedDataSqlScript.AssertArgumentIsNotNullOrWhiteSpace(nameof(materializedDataSqlScript));

            using (var sqlCmd = new SqlCommand(materializedDataSqlScript, sqlTransaction.Connection, sqlTransaction))
            {
                if(commandTimeout.HasValue)
                    sqlCmd.CommandTimeout = commandTimeout.Value;

                using (var sqlReader = await sqlCmd.ExecuteReaderAsync().ConfigureAwait(false))
                {
                    bool isSuccessful = false;
                    if ((await sqlReader.ReadAsync().ConfigureAwait(false)) && sqlReader.FieldCount >= 1 && sqlReader.GetFieldType(0) == typeof(bool))
                        isSuccessful = await sqlReader.GetFieldValueAsync<bool>(0).ConfigureAwait(false);

                    //This pretty-much will never happen as SQL Server will likely raise it's own exceptions/errors;
                    //  but at least if it does we cancel the process and raise an exception...
                    if (!isSuccessful)
                        throw new InvalidOperationException("An unknown error occurred while executing the SQL Script.");
                }
            }
        }

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
