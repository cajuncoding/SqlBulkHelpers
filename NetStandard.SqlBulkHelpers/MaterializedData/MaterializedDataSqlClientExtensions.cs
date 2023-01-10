using Microsoft.Data.SqlClient;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;

namespace SqlBulkHelpers.MaterializedData
{
    public static class MaterializedDataSqlClientExtensions
    {
        #region Script Execution Extensions

        internal static Task ExecuteMaterializedDataSqlScriptAsync(this SqlTransaction sqlTransaction, MaterializedDataScriptBuilder sqlScriptBuilder, int? commandTimeout = null)
            => ExecuteMaterializedDataSqlScriptAsync(sqlTransaction, sqlScriptBuilder.BuildSqlScript(), commandTimeout);
        
        internal static async Task ExecuteMaterializedDataSqlScriptAsync(this SqlTransaction sqlTransaction, string materializedDataSqlScript, int? commandTimeout = null)
        {
            sqlTransaction.AssertArgumentIsNotNull(nameof(sqlTransaction));
            materializedDataSqlScript.AssertArgumentIsNotNullOrWhiteSpace(nameof(materializedDataSqlScript));

            using (var sqlCmd = new SqlCommand(materializedDataSqlScript, sqlTransaction.Connection, sqlTransaction))
            {
                #if DEBUG
                if (Debugger.IsAttached)
                {
                    Debug.WriteLine($"[{nameof(SqlBulkHelpers)}] Executing Materialized Data SQL Script:");
                    Debug.WriteLine(materializedDataSqlScript);
                }
                #endif

                if (commandTimeout.HasValue)
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

        #endregion

        #region Clone Table Extensions

        public static async Task<CloneTableInfo> CloneTableAsync<T>(
            this SqlTransaction sqlTransaction,
            bool recreateIfExists = false,
            bool copyDataFromSource = false,
            ISqlBulkHelpersConfig bulkHelpersConfig = null
        ) where T : class => (await CloneTablesAsync(
            sqlTransaction, 
            new[] { CloneTableInfo.From<T, T>() }, 
            recreateIfExists, 
            copyDataFromSource, 
            bulkHelpersConfig
        ).ConfigureAwait(false)).FirstOrDefault();

        public static async Task<CloneTableInfo> CloneTableAsync(
            this SqlTransaction sqlTransaction,
            string sourceTableName,
            string targetTableName = null,
            bool recreateIfExists = false,
            bool copyDataFromSource = false,
            ISqlBulkHelpersConfig bulkHelpersConfig = null
        ) => (await CloneTablesAsync(
            sqlTransaction,
            new[] { CloneTableInfo.From(sourceTableName, targetTableName) },
            recreateIfExists,
            copyDataFromSource,
            bulkHelpersConfig
        ).ConfigureAwait(false)).FirstOrDefault();

        public static Task<CloneTableInfo[]> CloneTablesAsync(
            this SqlTransaction sqlTransaction,
            IEnumerable<(string SourceTableName, string TargetTableName)> tablesToClone,
            bool recreateIfExists = false,
            bool copyDataFromSource = false,
            ISqlBulkHelpersConfig bulkHelpersConfig = null
        ) => CloneTablesAsync(
            sqlTransaction,
            tablesToClone.Select(t => CloneTableInfo.From(t.SourceTableName, t.TargetTableName)),
            recreateIfExists,
            copyDataFromSource,
            bulkHelpersConfig
        );

        public static async Task<CloneTableInfo[]> CloneTablesAsync(
            this SqlTransaction sqlTransaction,
            IEnumerable<CloneTableInfo> tablesToClone,
            bool recreateIfExists = false,
            bool copyDataFromSource = false,
            ISqlBulkHelpersConfig bulkHelpersConfig = null
        )
        {
            sqlTransaction.AssertArgumentIsNotNull(nameof(sqlTransaction));

            var results = await new MaterializeDataHelper<ISkipMappingLookup>(sqlTransaction, bulkHelpersConfig)
                .CloneTablesAsync(sqlTransaction, tablesToClone.AsArray(), recreateIfExists, copyDataFromSource)
                .ConfigureAwait(false);

            return results;
        }

        #endregion

        #region Drop Table Extensions

        public static async Task<TableNameTerm> DropTableAsync<T>(
            this SqlTransaction sqlTransaction,
            ISqlBulkHelpersConfig bulkHelpersConfig = null
        ) where T : class => (await DropTablesAsync(sqlTransaction, new [] { typeof(T) }, bulkHelpersConfig).ConfigureAwait(false)).FirstOrDefault();

        public static async Task<TableNameTerm> DropTableAsync(
            this SqlTransaction sqlTransaction,
            string tableName,
            ISqlBulkHelpersConfig bulkHelpersConfig = null
        ) => (await DropTablesAsync(sqlTransaction, new[] { tableName }, bulkHelpersConfig).ConfigureAwait(false)).FirstOrDefault();

        public static Task<TableNameTerm[]> DropTablesAsync(
            this SqlTransaction sqlTransaction,
            IEnumerable<Type> mappedModelTypes,
            ISqlBulkHelpersConfig bulkHelpersConfig = null
        ) => DropTablesAsync(sqlTransaction, ConvertToMappedTableNamesInternal(mappedModelTypes), bulkHelpersConfig);

        public static async Task<TableNameTerm[]> DropTablesAsync(
            this SqlTransaction sqlTransaction,
            IEnumerable<string> tableNames,
            ISqlBulkHelpersConfig bulkHelpersConfig = null
        )
        {
            sqlTransaction.AssertArgumentIsNotNull(nameof(sqlTransaction));

            var results = await new MaterializeDataHelper<ISkipMappingLookup>(sqlTransaction, bulkHelpersConfig)
                .DropTablesAsync(sqlTransaction, tableNames.AsArray())
                .ConfigureAwait(false);

            return results;
        }

        #endregion

        #region Truncate Table Extensions

        public static async Task<TableNameTerm> ClearTableAsync<T>(
            this SqlTransaction sqlTransaction,
            bool forceOverrideOfConstraints = false,
            ISqlBulkHelpersConfig bulkHelpersConfig = null
        ) where T : class => (await ClearTablesAsync(sqlTransaction, new[] { typeof(T) }, forceOverrideOfConstraints, bulkHelpersConfig).ConfigureAwait(false)).FirstOrDefault();


        public static async Task<TableNameTerm> ClearTableAsync(
            this SqlTransaction sqlTransaction,
            string tableName,
            bool forceOverrideOfConstraints = false,
            ISqlBulkHelpersConfig bulkHelpersConfig = null
        ) => (await ClearTablesAsync(sqlTransaction, new[] { tableName }, forceOverrideOfConstraints, bulkHelpersConfig).ConfigureAwait(false)).FirstOrDefault();

        public static Task<TableNameTerm[]> ClearTablesAsync(
            this SqlTransaction sqlTransaction,
            IEnumerable<Type> mappedModelTypes,
            bool forceOverrideOfConstraints = false,
            ISqlBulkHelpersConfig bulkHelpersConfig = null
        ) => ClearTablesAsync(sqlTransaction, ConvertToMappedTableNamesInternal(mappedModelTypes), forceOverrideOfConstraints, bulkHelpersConfig);

        public static async Task<TableNameTerm[]> ClearTablesAsync(
            this SqlTransaction sqlTransaction,
            IEnumerable<string> tableNames,
            bool forceOverrideOfConstraints = false,
            ISqlBulkHelpersConfig bulkHelpersConfig = null
        )
        {
            sqlTransaction.AssertArgumentIsNotNull(nameof(sqlTransaction));

            var results = await new MaterializeDataHelper<ISkipMappingLookup>(sqlTransaction, bulkHelpersConfig)
                .ClearTablesAsync(sqlTransaction, forceOverrideOfConstraints, tableNames.AsArray())
                .ConfigureAwait(false);

            return results;
        }

        #endregion

        #region Materialize Data Extensions
        public static Task<MaterializeDataContext> StartMaterializeDataProcessAsync<T>(
            this SqlTransaction sqlTransaction,
            ISqlBulkHelpersConfig bulkHelpersConfig = null
        ) where T : class => StartMaterializeDataProcessAsync(sqlTransaction, new[] { typeof(T) }, bulkHelpersConfig);

        public static Task<MaterializeDataContext> StartMaterializeDataProcessAsync(
            this SqlTransaction sqlTransaction,
            string tableName,
            ISqlBulkHelpersConfig bulkHelpersConfig = null
        ) => StartMaterializeDataProcessAsync(sqlTransaction, new[] { tableName }, bulkHelpersConfig);

        public static Task<MaterializeDataContext> StartMaterializeDataProcessAsync(
            this SqlTransaction sqlTransaction,
            params Type[] mappedModelTypeParams
        ) => StartMaterializeDataProcessAsync(sqlTransaction, ConvertToMappedTableNamesInternal(mappedModelTypeParams), null);

        public static Task<MaterializeDataContext> StartMaterializeDataProcessAsync(
            this SqlTransaction sqlTransaction,
            IEnumerable<Type> mappedModelTypes,
            ISqlBulkHelpersConfig bulkHelpersConfig = null
        ) => StartMaterializeDataProcessAsync(sqlTransaction, ConvertToMappedTableNamesInternal(mappedModelTypes), bulkHelpersConfig);

        public static Task<MaterializeDataContext> StartMaterializeDataProcessAsync(
            this SqlTransaction sqlTransaction,
            params string[] tableNameParams
        ) => StartMaterializeDataProcessAsync(sqlTransaction, tableNameParams, null);

        public static async Task<MaterializeDataContext> StartMaterializeDataProcessAsync(
            this SqlTransaction sqlTransaction,
            IEnumerable<string> tableNames,
            ISqlBulkHelpersConfig bulkHelpersConfig = null
        )
        {
            sqlTransaction.AssertArgumentIsNotNull(nameof(sqlTransaction));

            var materializeDataContext = await new MaterializeDataHelper<ISkipMappingLookup>(sqlTransaction, bulkHelpersConfig)
                .StartMaterializeDataProcessAsync(sqlTransaction, tableNames.AsArray())
                .ConfigureAwait(false);

            return materializeDataContext;
        }

        #endregion

        #region Internal Helpers

        private static IEnumerable<string> ConvertToMappedTableNamesInternal(IEnumerable<Type> mappedModelTypes)
        {
            return mappedModelTypes
                .Where(type => type != null)
                .Select(type => type.GetSqlBulkHelpersMappedTableNameTerm().FullyQualifiedTableName);
        }

        #endregion
    }
}
