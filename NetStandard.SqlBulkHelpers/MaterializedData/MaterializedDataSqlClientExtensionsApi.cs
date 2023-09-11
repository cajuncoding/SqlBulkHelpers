using Microsoft.Data.SqlClient;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;
using SqlBulkHelpers.CustomExtensions;

namespace SqlBulkHelpers.MaterializedData
{
    public static class MaterializedDataSqlClientExtensionsApi
    {
        #region Script Execution Extensions

        internal static Task ExecuteMaterializedDataSqlScriptAsync(this SqlTransaction sqlTransaction, MaterializedDataScriptBuilder sqlScriptBuilder, int? commandTimeout = null)
            => ExecuteMaterializedDataSqlScriptAsync(sqlTransaction, sqlScriptBuilder.BuildSqlScript(), commandTimeout);

        internal static async Task ExecuteMaterializedDataSqlScriptAsync(this SqlTransaction sqlTransaction, string materializedDataSqlScript, int? commandTimeout = null)
        {
            sqlTransaction.AssertArgumentIsNotNull(nameof(sqlTransaction));
            await ExecuteMaterializedDataSqlScriptAsync(sqlTransaction.Connection, materializedDataSqlScript, commandTimeout: commandTimeout, sqlTransaction: sqlTransaction);
        }

        internal static Task ExecuteMaterializedDataSqlScriptAsync(this SqlConnection sqlConnection, MaterializedDataScriptBuilder sqlScriptBuilder, int? commandTimeout = null)
            => ExecuteMaterializedDataSqlScriptAsync(sqlConnection, sqlScriptBuilder.BuildSqlScript(), commandTimeout: commandTimeout, sqlTransaction: null);

        internal static async Task ExecuteMaterializedDataSqlScriptAsync(this SqlConnection sqlConnection, string materializedDataSqlScript, int? commandTimeout = null, SqlTransaction sqlTransaction = null)
        {
            sqlConnection.AssertArgumentIsNotNull(nameof(sqlConnection));
            materializedDataSqlScript.AssertArgumentIsNotNullOrWhiteSpace(nameof(materializedDataSqlScript));

            using (var sqlCmd = new SqlCommand(materializedDataSqlScript, sqlConnection, sqlTransaction))
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
            new[] { CloneTableInfo.From<T, T>(copyDataFromSource: copyDataFromSource) }, 
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
            new[] { CloneTableInfo.From(sourceTableName, targetTableName, copyDataFromSource: copyDataFromSource) },
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
            tablesToClone.Select(t => CloneTableInfo.From(t.SourceTableName, t.TargetTableName, copyDataFromSource: copyDataFromSource)),
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

            var results = await new MaterializeDataHelper<ISkipMappingLookup>(bulkHelpersConfig)
                .CloneTablesAsync(sqlTransaction, tablesToClone.AsArray(), recreateIfExists, copyDataFromSource)
                .ConfigureAwait(false);

            return results;
        }

        #endregion

        #region Copy Table Data Extensions

        public static async Task<CloneTableInfo> CopyTableDataAsync<TSource, TTarget>(
            this SqlTransaction sqlTransaction,
            ISqlBulkHelpersConfig bulkHelpersConfig = null
        )
        where TSource : class 
        where TTarget : class => (await CopyTableDataAsync(
            sqlTransaction,
            new[] { CloneTableInfo.From<TSource, TTarget>(copyDataFromSource: true) },
            bulkHelpersConfig
        ).ConfigureAwait(false)).FirstOrDefault();

        public static async Task<CloneTableInfo> CopyTableDataAsync(
            this SqlTransaction sqlTransaction,
            string sourceTableName,
            string targetTableName = null,
            ISqlBulkHelpersConfig bulkHelpersConfig = null
        ) => (await CopyTableDataAsync(
            sqlTransaction,
            new[] { CloneTableInfo.From(sourceTableName, targetTableName, copyDataFromSource: true) },
            bulkHelpersConfig
        ).ConfigureAwait(false)).FirstOrDefault();

        public static Task<CloneTableInfo[]> CopyTableDataAsync(
            this SqlTransaction sqlTransaction,
            IEnumerable<(string SourceTableName, string TargetTableName)> tablesToProcess,
            ISqlBulkHelpersConfig bulkHelpersConfig = null
        ) => CopyTableDataAsync(
            sqlTransaction,
            tablesToProcess.Select(t => CloneTableInfo.From(t.SourceTableName, t.TargetTableName, copyDataFromSource: true)),
            bulkHelpersConfig
        );

        public static async Task<CloneTableInfo[]> CopyTableDataAsync(
            this SqlTransaction sqlTransaction,
            IEnumerable<CloneTableInfo> tablesToClone,
            ISqlBulkHelpersConfig bulkHelpersConfig = null
        )
        {
            sqlTransaction.AssertArgumentIsNotNull(nameof(sqlTransaction));

            var results = await new MaterializeDataHelper<ISkipMappingLookup>(bulkHelpersConfig)
                .CopyTableDataAsync(sqlTransaction, tablesToClone.AsArray())
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

            var results = await new MaterializeDataHelper<ISkipMappingLookup>(bulkHelpersConfig)
                .DropTablesAsync(sqlTransaction, tableNames.AsArray())
                .ConfigureAwait(false);

            return results;
        }

        #endregion

        #region Clear Table Extensions - Truncate for Materialize as Empty...

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

            var results = await new MaterializeDataHelper<ISkipMappingLookup>(bulkHelpersConfig)
                .ClearTablesAsync(sqlTransaction, forceOverrideOfConstraints, tableNames.AsArray())
                .ConfigureAwait(false);

            return results;
        }

        #endregion

        #region Table Identity Column Helper Methods (e.g. Get/Set Tables Current Identity Value)

        public static Task<long> GetTableCurrentIdentityValueAsync(this SqlTransaction sqlTransaction, string tableName, ISqlBulkHelpersConfig bulkHelpersConfig = null)
            => GetTableCurrentIdentityValueAsync<ISkipMappingLookup>(sqlTransaction, tableName, bulkHelpersConfig);

        public static Task<long> GetTableCurrentIdentityValueAsync<T>(this SqlTransaction sqlTransaction, string tableNameOverride = null, ISqlBulkHelpersConfig bulkHelpersConfig = null)
            where T : class
        {
            sqlTransaction.AssertArgumentIsNotNull(nameof(sqlTransaction));
            return GetTableCurrentIdentityValueInternalAsync<T>(sqlTransaction.Connection, tableNameOverride, sqlTransaction, bulkHelpersConfig);
        }

        public static Task<long> GetTableCurrentIdentityValueAsync(this SqlConnection sqlConnection, string tableName, ISqlBulkHelpersConfig bulkHelpersConfig = null)
            => GetTableCurrentIdentityValueInternalAsync<ISkipMappingLookup>(sqlConnection, tableName, bulkHelpersConfig: bulkHelpersConfig);

        private static async Task<long> GetTableCurrentIdentityValueInternalAsync<T>(
            this SqlConnection sqlConnection,
            string tableNameOverride = null,
            SqlTransaction sqlTransaction = null,
            ISqlBulkHelpersConfig bulkHelpersConfig = null
        ) where T : class
        {
            sqlConnection.AssertArgumentIsNotNull(nameof(sqlConnection));

            var currentIdentityValue = await new MaterializeDataHelper<T>(bulkHelpersConfig)
                .GetTableCurrentIdentityValueAsync(sqlConnection, sqlTransaction, tableNameOverride)
                .ConfigureAwait(false);

            return currentIdentityValue;
        }

        public static long GetTableCurrentIdentityValue(this SqlTransaction sqlTransaction, string tableName, ISqlBulkHelpersConfig bulkHelpersConfig = null)
            => GetTableCurrentIdentityValue<ISkipMappingLookup>(sqlTransaction, tableName, bulkHelpersConfig);

        public static long GetTableCurrentIdentityValue<T>(this SqlTransaction sqlTransaction, string tableNameOverride = null, ISqlBulkHelpersConfig bulkHelpersConfig = null)
            where T : class
        {
            sqlTransaction.AssertArgumentIsNotNull(nameof(sqlTransaction));
            return GetTableCurrentIdentityValueInternal<T>(sqlTransaction.Connection, tableNameOverride, sqlTransaction, bulkHelpersConfig);
        }

        public static long GetTableCurrentIdentityValue(this SqlConnection sqlConnection, string tableName, ISqlBulkHelpersConfig bulkHelpersConfig = null)
            => GetTableCurrentIdentityValueInternal<ISkipMappingLookup>(sqlConnection, tableName, bulkHelpersConfig: bulkHelpersConfig);

        private static long GetTableCurrentIdentityValueInternal<T>(
            this SqlConnection sqlConnection,
            string tableNameOverride = null,
            SqlTransaction sqlTransaction = null, 
            ISqlBulkHelpersConfig bulkHelpersConfig = null
        ) where T : class
        {
            sqlConnection.AssertArgumentIsNotNull(nameof(sqlConnection));

            var currentIdentityValue = new MaterializeDataHelper<T>(bulkHelpersConfig).GetTableCurrentIdentityValue(sqlConnection, sqlTransaction, tableNameOverride);
            return currentIdentityValue;
        }

        public static Task ReSeedTableIdentityValueAsync(this SqlTransaction sqlTransaction, string tableName, long newIdentitySeedValue, ISqlBulkHelpersConfig bulkHelpersConfig = null)
            => ReSeedTableIdentityValueAsync<ISkipMappingLookup>(sqlTransaction, newIdentitySeedValue, tableName, bulkHelpersConfig);

        public static Task ReSeedTableIdentityValueAsync<T>(this SqlTransaction sqlTransaction, long newIdentitySeedValue, string tableNameOverride = null, ISqlBulkHelpersConfig bulkHelpersConfig = null)
            where T : class
        {
            sqlTransaction.AssertArgumentIsNotNull(nameof(sqlTransaction));
            return ReSeedTableIdentityValueInternalAsync<T>(sqlTransaction.Connection, newIdentitySeedValue, tableNameOverride, sqlTransaction, bulkHelpersConfig);
        }

        public static Task ReSeedTableIdentityValueAsync(this SqlConnection sqlConnection, string tableName, long newIdentitySeedValue, ISqlBulkHelpersConfig bulkHelpersConfig = null)
            => ReSeedTableIdentityValueInternalAsync<ISkipMappingLookup>(sqlConnection, newIdentitySeedValue, tableName, bulkHelpersConfig: bulkHelpersConfig);

        private static async Task ReSeedTableIdentityValueInternalAsync<T>(
            this SqlConnection sqlConnection, 
            long newIdentitySeedValue,
            string tableNameOverride = null,
            SqlTransaction sqlTransaction = null, 
            ISqlBulkHelpersConfig bulkHelpersConfig = null
        ) where T : class
        {
            sqlConnection.AssertArgumentIsNotNull(nameof(sqlConnection));

            await new MaterializeDataHelper<T>(bulkHelpersConfig)
                .ReSeedTableIdentityValueAsync(sqlConnection, newIdentitySeedValue, sqlTransaction, tableNameOverride)
                .ConfigureAwait(false);
        }


        public static void ReSeedTableIdentityValue(this SqlTransaction sqlTransaction, string tableName, long newIdentitySeedValue, ISqlBulkHelpersConfig bulkHelpersConfig = null)
            => ReSeedTableIdentityValue<ISkipMappingLookup>(sqlTransaction, newIdentitySeedValue, tableName, bulkHelpersConfig);

        public static void ReSeedTableIdentityValue<T>(this SqlTransaction sqlTransaction, long newIdentitySeedValue, string tableNameOverride = null, ISqlBulkHelpersConfig bulkHelpersConfig = null)
            where T : class
        {
            sqlTransaction.AssertArgumentIsNotNull(nameof(sqlTransaction));
            ReSeedTableIdentityValueInternal<T>(sqlTransaction.Connection, newIdentitySeedValue, tableNameOverride, sqlTransaction, bulkHelpersConfig);
        }

        public static void ReSeedTableIdentityValue(this SqlConnection sqlConnection, string tableName, long newIdentitySeedValue, ISqlBulkHelpersConfig bulkHelpersConfig = null)
            => ReSeedTableIdentityValueInternal<ISkipMappingLookup>(sqlConnection, newIdentitySeedValue, tableName, bulkHelpersConfig: bulkHelpersConfig);

        private static void ReSeedTableIdentityValueInternal<T>(
            this SqlConnection sqlConnection,
            long newIdentitySeedValue,
            string tableNameOverride = null,
            SqlTransaction sqlTransaction = null,
            ISqlBulkHelpersConfig bulkHelpersConfig = null
        ) where T : class
        {
            sqlConnection.AssertArgumentIsNotNull(nameof(sqlConnection));
            new MaterializeDataHelper<T>(bulkHelpersConfig).ReSeedTableIdentityValue(sqlConnection, newIdentitySeedValue, sqlTransaction, tableNameOverride);
        }

        public static Task<long> ReSeedTableIdentityValueWithMaxIdAsync(this SqlTransaction sqlTransaction, string tableName, ISqlBulkHelpersConfig bulkHelpersConfig = null)
            => ReSeedTableIdentityValueWithMaxIdAsync<ISkipMappingLookup>(sqlTransaction, tableName, bulkHelpersConfig);

        public static Task<long> ReSeedTableIdentityValueWithMaxIdAsync<T>(this SqlTransaction sqlTransaction, string tableNameOverride = null, ISqlBulkHelpersConfig bulkHelpersConfig = null)
            where T : class
        {
            sqlTransaction.AssertArgumentIsNotNull(nameof(sqlTransaction));
            return ReSeedTableIdentityValueWithMaxIdInternalAsync<T>(sqlTransaction.Connection, tableNameOverride, sqlTransaction, bulkHelpersConfig);
        }

        public static Task<long> ReSeedTableIdentityValueWithMaxIdAsync(this SqlConnection sqlConnection, string tableName, ISqlBulkHelpersConfig bulkHelpersConfig = null)
            => ReSeedTableIdentityValueWithMaxIdInternalAsync<ISkipMappingLookup>(sqlConnection, tableName, bulkHelpersConfig: bulkHelpersConfig);

        private static async Task<long> ReSeedTableIdentityValueWithMaxIdInternalAsync<T>(
            this SqlConnection sqlConnection,
            string tableNameOverride = null,
            SqlTransaction sqlTransaction = null,
            ISqlBulkHelpersConfig bulkHelpersConfig = null
        ) where T : class
        {
            sqlConnection.AssertArgumentIsNotNull(nameof(sqlConnection));

            return await new MaterializeDataHelper<T>(bulkHelpersConfig)
                .ReSeedTableIdentityValueWithMaxIdAsync(sqlConnection, sqlTransaction, tableNameOverride)
                .ConfigureAwait(false);
        }

        public static long ReSeedTableIdentityValueWithMaxId(this SqlTransaction sqlTransaction, string tableName, ISqlBulkHelpersConfig bulkHelpersConfig = null)
            => ReSeedTableIdentityValueWithMaxId<ISkipMappingLookup>(sqlTransaction, tableName, bulkHelpersConfig);

        public static long ReSeedTableIdentityValueWithMaxId<T>(this SqlTransaction sqlTransaction, string tableNameOverride = null, ISqlBulkHelpersConfig bulkHelpersConfig = null)
            where T : class
        {
            sqlTransaction.AssertArgumentIsNotNull(nameof(sqlTransaction));
            return ReSeedTableIdentityValueWithMaxIdInternal<T>(sqlTransaction.Connection, tableNameOverride, sqlTransaction, bulkHelpersConfig);
        }

        public static long ReSeedTableIdentityValueWithMaxId(this SqlConnection sqlConnection, string tableName, ISqlBulkHelpersConfig bulkHelpersConfig = null)
            => ReSeedTableIdentityValueWithMaxIdInternal<ISkipMappingLookup>(sqlConnection, tableName, bulkHelpersConfig: bulkHelpersConfig);

        private static long ReSeedTableIdentityValueWithMaxIdInternal<T>(
            this SqlConnection sqlConnection,
            string tableNameOverride = null,
            SqlTransaction sqlTransaction = null,
            ISqlBulkHelpersConfig bulkHelpersConfig = null
        ) where T : class
        {
            sqlConnection.AssertArgumentIsNotNull(nameof(sqlConnection));
            return new MaterializeDataHelper<T>(bulkHelpersConfig).ReSeedTableIdentityWithMaxId(sqlConnection, sqlTransaction, tableNameOverride);
        }

        #endregion

        #region Full Text Index (cannot be altered within a Transaction)...

        /// <summary>
        /// Remove and Return the Details for the Full Text Index of specified mapped table model type.
        /// NOTE: THIS API is Unique in that this CANNOT be called within the context of a Transaction and therefore
        ///         must be executed on a Connection without a Transaction!!!
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="sqlConnection"></param>
        /// <param name="bulkHelpersConfig"></param>
        /// <returns></returns>
        public static Task<FullTextIndexDefinition> RemoveFullTextIndexAsync<T>(
            this SqlConnection sqlConnection,
            ISqlBulkHelpersConfig bulkHelpersConfig = null
        ) where T : class => RemoveFullTextIndexAsync(sqlConnection, typeof(T).GetSqlBulkHelpersMappedTableNameTerm().FullyQualifiedTableName, bulkHelpersConfig);


        /// <summary>
        /// Remove and Return the Details for the Full Text Index of specified mapped table model type.
        /// NOTE: THIS API is Unique in that this CANNOT be called within the context of a Transaction and therefore
        ///         must be executed on a Connection without a Transaction!!!
        /// </summary>
        /// <param name="sqlConnection"></param>
        /// <param name="tableName"></param>
        /// <param name="bulkHelpersConfig"></param>
        /// <returns></returns>
        public static async Task<FullTextIndexDefinition> RemoveFullTextIndexAsync(
            this SqlConnection sqlConnection,
            string tableName,
            ISqlBulkHelpersConfig bulkHelpersConfig = null
        )
        {
            sqlConnection.AssertArgumentIsNotNull(nameof(sqlConnection));

            var results = await new MaterializeDataHelper<ISkipMappingLookup>(bulkHelpersConfig)
                .RemoveFullTextIndexAsync(sqlConnection, tableName)
                .ConfigureAwait(false);

            return results;
        }

        /// <summary>
        /// Add the Full Text Index specified by the Definition to specified mapped table model.
        /// NOTE: THIS API is Unique in that this CANNOT be called within the context of a Transaction and therefore
        ///         must be executed on a Connection without a Transaction!!!
        /// NOTE: This is usually done when Materializing data into a table that has a Full Text Index, so the index must be removed and re-added outside
        ///         of the Transaction.
        /// </summary>
        /// <param name="sqlConnection"></param>
        /// <param name="fullTextIndex"></param>
        /// <param name="bulkHelpersConfig"></param>
        /// <returns></returns>
        public static Task AddFullTextIndexAsync<T>(
            this SqlConnection sqlConnection,
            FullTextIndexDefinition fullTextIndex,
            ISqlBulkHelpersConfig bulkHelpersConfig = null
        ) where T : class => AddFullTextIndexAsync(sqlConnection, typeof(T).GetSqlBulkHelpersMappedTableNameTerm().FullyQualifiedTableName, fullTextIndex, bulkHelpersConfig);

        /// <summary>
        /// Remove and Return the Details for the Full Text Index of specified table.
        /// NOTE: THIS API is Unique in that this CANNOT be called within the context of a Transaction and therefore
        ///         must be executed on a Connection without a Transaction!!!
        /// NOTE: This is usually done when Materializing data into a table that has a Full Text Index, so the index must be removed and re-added outside
        ///         of the Transaction.
        /// </summary>
        /// <param name="sqlConnection"></param>
        /// <param name="tableName"></param>
        /// <param name="fullTextIndex"></param>
        /// <param name="bulkHelpersConfig"></param>
        /// <returns></returns>
        public static async Task AddFullTextIndexAsync(
            this SqlConnection sqlConnection,
            string tableName,
            FullTextIndexDefinition fullTextIndex,
            ISqlBulkHelpersConfig bulkHelpersConfig = null
        )
        {
            sqlConnection.AssertArgumentIsNotNull(nameof(sqlConnection));
            fullTextIndex.AssertArgumentIsNotNull(nameof(fullTextIndex));

            await new MaterializeDataHelper<ISkipMappingLookup>(bulkHelpersConfig)
                .AddFullTextIndexAsync(sqlConnection, fullTextIndex, tableName)
                .ConfigureAwait(false);
        }
        #endregion

        #region Materialize Data Extensions

        /// <summary>
        /// Execute the Materialized Data Process with the defined async function handler.
        /// This method will handle all aspects needed and manage the Sql Transaction; this includes
        ///     handling tasks that cannot be managed within the Transaction (e.g. Full Text Indexes).
        /// This should be the easiest way to work for most use cases, however for more manual control
        ///     you may manage your own flow (e.g. Transaction) using the StartMaterializeDataProcessAsync method(s).
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="sqlConnection"></param>
        /// <param name="materializeDataHandlerActionAsync"></param>
        /// <param name="bulkHelpersConfig"></param>
        /// <returns></returns>
        public static Task ExecuteMaterializeDataProcessAsync<T>(
            this SqlConnection sqlConnection,
            Func<IMaterializeDataContext, SqlTransaction, Task> materializeDataHandlerActionAsync,
            ISqlBulkHelpersConfig bulkHelpersConfig = null
        ) where T : class => ExecuteMaterializeDataProcessAsync(sqlConnection, new[] { typeof(T) }, materializeDataHandlerActionAsync, bulkHelpersConfig);


        /// <summary>
        /// Execute the Materialized Data Process with the defined async function handler.
        /// This method will handle all aspects needed and manage the Sql Transaction; this includes
        ///     handling tasks that cannot be managed within the Transaction (e.g. Full Text Indexes).
        /// This should be the easiest way to work for most use cases, however for more manual control
        ///     you may manage your own flow (e.g. Transaction) using the StartMaterializeDataProcessAsync method(s).
        /// </summary>
        /// <param name="sqlConnection"></param>
        /// <param name="tableName"></param>
        /// <param name="materializeDataHandlerActionAsync"></param>
        /// <param name="bulkHelpersConfig"></param>
        /// <returns></returns>
        public static Task ExecuteMaterializeDataProcessAsync(
            this SqlConnection sqlConnection,
            string tableName,
            Func<IMaterializeDataContext, SqlTransaction, Task> materializeDataHandlerActionAsync,
            ISqlBulkHelpersConfig bulkHelpersConfig = null
        ) => ExecuteMaterializeDataProcessAsync(sqlConnection, new[] { tableName }, materializeDataHandlerActionAsync, bulkHelpersConfig);

        /// <summary>
        /// Execute the Materialized Data Process with the defined async function handler.
        /// This method will handle all aspects needed and manage the Sql Transaction; this includes
        ///     handling tasks that cannot be managed within the Transaction (e.g. Full Text Indexes).
        /// This should be the easiest way to work for most use cases, however for more manual control
        ///     you may manage your own flow (e.g. Transaction) using the StartMaterializeDataProcessAsync method(s).
        /// </summary>
        /// <param name="sqlConnection"></param>
        /// <param name="mappedModelTypes"></param>
        /// <param name="materializeDataHandlerActionAsync"></param>
        /// <param name="bulkHelpersConfig"></param>
        /// <returns></returns>
        public static Task ExecuteMaterializeDataProcessAsync(
            this SqlConnection sqlConnection,
            IEnumerable<Type> mappedModelTypes,
            Func<IMaterializeDataContext, SqlTransaction, Task> materializeDataHandlerActionAsync,
            ISqlBulkHelpersConfig bulkHelpersConfig = null
        ) => ExecuteMaterializeDataProcessAsync(sqlConnection, ConvertToMappedTableNamesInternal(mappedModelTypes), materializeDataHandlerActionAsync, bulkHelpersConfig);

        /// <summary>
        /// Execute the Materialized Data Process with the defined async function handler.
        /// This method will handle all aspects needed and manage the Sql Transaction; this includes
        ///     handling tasks that cannot be managed within the Transaction (e.g. Full Text Indexes).
        /// This should be the easiest way to work for most use cases, however for more manual control
        ///     you may manage your own flow (e.g. Transaction) using the StartMaterializeDataProcessAsync method(s).
        /// </summary>
        /// <param name="sqlConnection"></param>
        /// <param name="tableNames"></param>
        /// <param name="materializeDataHandlerActionAsync"></param>
        /// <param name="bulkHelpersConfig"></param>
        /// <returns></returns>
        public static async Task ExecuteMaterializeDataProcessAsync(
            this SqlConnection sqlConnection,
            IEnumerable<string> tableNames,
            Func<IMaterializeDataContext, SqlTransaction, Task> materializeDataHandlerActionAsync,
            ISqlBulkHelpersConfig bulkHelpersConfig = null
        )
        {
            sqlConnection.AssertArgumentIsNotNull(nameof(sqlConnection));

            var sqlBulkHelpersConfig = bulkHelpersConfig ?? SqlBulkHelpersConfig.DefaultConfig;

            var distinctTableNames = tableNames.Distinct().ToArray();

            var materializationDataHelper = new MaterializeDataHelper<ISkipMappingLookup>(bulkHelpersConfig);
            MaterializeDataContext materializeDataContext = null;

            //Initialize our Schema outside the transaction to avoid Schema locks (now Default Behavior as of v2.3.0)...
            if (sqlBulkHelpersConfig.MaterializedDataSchemaCopyMode == SchemaCopyMode.OutsideTransactionAvoidSchemaLocks)
            {
                #if NETSTANDARD2_0
                using (var sqlTransaction = sqlConnection.BeginTransaction())
                #else
                await using (var sqlTransaction = (SqlTransaction)await sqlConnection.BeginTransactionAsync().ConfigureAwait(false))
                #endif
                {
                    materializeDataContext = await materializationDataHelper
                        .StartMaterializeDataProcessAsync(sqlTransaction, distinctTableNames)
                        .ConfigureAwait(false);

                    #if NETSTANDARD2_0
                    sqlTransaction.Commit();
                    #else
                    await sqlTransaction.CommitAsync().ConfigureAwait(false);
                    #endif
                }
            }

            try
            {
                //SECOND Execute the Materialized Data process!
                #if NETSTANDARD2_0
                using (var sqlTransaction = sqlConnection.BeginTransaction())
                #else
                await using (var sqlTransaction = (SqlTransaction)await sqlConnection.BeginTransactionAsync().ConfigureAwait(false))
                #endif
                {

                    //If configured (not already handled above) then we initialize our Schema inside the transaction...
                    if (sqlBulkHelpersConfig.MaterializedDataSchemaCopyMode == SchemaCopyMode.InsideTransactionAllowSchemaLocks)
                    {
                        materializeDataContext = await materializationDataHelper
                            .StartMaterializeDataProcessAsync(sqlTransaction, distinctTableNames)
                            .ConfigureAwait(false);
                    }

                    if (materializeDataContext is null)
                        throw new InvalidOperationException("The Materialized Data Context is null but must be fully initialized before processing may continue.");

                    //The Handler is always an Async process so we must await it...
                    await materializeDataHandlerActionAsync.Invoke(materializeDataContext, sqlTransaction).ConfigureAwait(false);

                    //Enable Passive cancellation vs throwing an Exception for advanced use cases...
                    if (!materializeDataContext.IsCancelled)
                    {
                        try
                        {
                            //Some tasks MUST be done outside of the Materialized Data Transaction (e.g. handling FullTextIndexes)
                            //  so we handle those here (as needed and if enabled)...
                            await materializeDataContext.HandleParallelConnectionTasksBeforeMaterialization().ConfigureAwait(false);

                            //***************************************************************************************************
                            //****HERE We actually Execute the Materialized Data Processing SWITCH and Data Integrity Checks!
                            //***************************************************************************************************
                            //Once completed without errors we Finish the Materialized Data process...
                            await materializeDataContext.FinishMaterializeDataProcessAsync(sqlTransaction).ConfigureAwait(false);

                            //And if initialized inside the Transaction then we also Clean up inside the Transaction!
                            if (sqlBulkHelpersConfig.MaterializedDataSchemaCopyMode == SchemaCopyMode.InsideTransactionAllowSchemaLocks)
                            {
                                await materializationDataHelper
                                    .CleanupMaterializeDataProcessAsync(sqlTransaction, materializeDataContext)
                                    .ConfigureAwait(false);
                            }

                            //NOW we must commit our Transaction to save all Changes performed within the Transaction!
                            #if NETSTANDARD2_0
                            sqlTransaction.Commit();
                            #else
                            await sqlTransaction.CommitAsync().ConfigureAwait(false);
                            #endif
                        }
                        finally
                        {
                            //Some tasks MUST be handled outside of the Materialized Data Transaction (e.g. handling FullTextIndexes)
                            //  so we handle those here (as needed and if enabled)...
                            await materializeDataContext.HandleParallelConnectionTasksAfterMaterialization().ConfigureAwait(false);
                        }
                    }
                }
            }
            finally
            {
                //Initialize our Schema outside the transaction to avoid Schema locks (now Default Behaviour as of v2.3.0)...
                if (materializeDataContext != null && sqlBulkHelpersConfig.MaterializedDataSchemaCopyMode == SchemaCopyMode.OutsideTransactionAvoidSchemaLocks)
                {
                    #if NETSTANDARD2_0
                    using (var sqlTransaction = sqlConnection.BeginTransaction())
                    #else
                    await using (var sqlTransaction = (SqlTransaction)await sqlConnection.BeginTransactionAsync().ConfigureAwait(false))
                    #endif
                    {
                        await materializationDataHelper
                            .CleanupMaterializeDataProcessAsync(sqlTransaction, materializeDataContext)
                            .ConfigureAwait(false);

                        #if NETSTANDARD2_0
                        sqlTransaction.Commit();
                        #else
                        await sqlTransaction.CommitAsync().ConfigureAwait(false);
                        #endif
                    }
                }
            }
        }

        public static Task<IMaterializeDataContextCompletionSource> StartMaterializeDataProcessAsync<T>(
            this SqlTransaction sqlTransaction,
            ISqlBulkHelpersConfig bulkHelpersConfig = null
        ) where T : class => StartMaterializeDataProcessAsync(sqlTransaction, new[] { typeof(T) }, bulkHelpersConfig);

        public static Task<IMaterializeDataContextCompletionSource> StartMaterializeDataProcessAsync(
            this SqlTransaction sqlTransaction,
            string tableName,
            ISqlBulkHelpersConfig bulkHelpersConfig = null
        ) => StartMaterializeDataProcessAsync(sqlTransaction, new[] { tableName }, bulkHelpersConfig);

        public static Task<IMaterializeDataContextCompletionSource> StartMaterializeDataProcessAsync(
            this SqlTransaction sqlTransaction,
            params Type[] mappedModelTypeParams
        ) => StartMaterializeDataProcessAsync(sqlTransaction, ConvertToMappedTableNamesInternal(mappedModelTypeParams), null);

        public static Task<IMaterializeDataContextCompletionSource> StartMaterializeDataProcessAsync(
            this SqlTransaction sqlTransaction,
            IEnumerable<Type> mappedModelTypes,
            ISqlBulkHelpersConfig bulkHelpersConfig = null
        ) => StartMaterializeDataProcessAsync(sqlTransaction, ConvertToMappedTableNamesInternal(mappedModelTypes), bulkHelpersConfig);

        public static Task<IMaterializeDataContextCompletionSource> StartMaterializeDataProcessAsync(
            this SqlTransaction sqlTransaction,
            params string[] tableNameParams
        ) => StartMaterializeDataProcessAsync(sqlTransaction, tableNameParams, null);

        public static async Task<IMaterializeDataContextCompletionSource> StartMaterializeDataProcessAsync(
            this SqlTransaction sqlTransaction,
            IEnumerable<string> tableNames,
            ISqlBulkHelpersConfig bulkHelpersConfig = null
        )
        {
            sqlTransaction.AssertArgumentIsNotNull(nameof(sqlTransaction));

            var materializeDataContext = await new MaterializeDataHelper<ISkipMappingLookup>(bulkHelpersConfig)
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
