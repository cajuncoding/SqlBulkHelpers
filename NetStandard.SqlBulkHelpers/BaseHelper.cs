using Microsoft.Data.SqlClient;
using System;
using System.Threading.Tasks;

namespace SqlBulkHelpers
{
    internal abstract class BaseHelper<T> where T : class
    {
        protected ISqlBulkHelpersDBSchemaLoader SqlDbSchemaLoader { get; set; }

        public ISqlBulkHelpersConfig BulkHelpersConfig { get; protected set; }

        protected SqlBulkHelpersProcessingDefinition BulkHelpersProcessingDefinition { get; set; }

        protected static readonly Type GenericType = typeof(T);

        #region Constructors

        /// <summary>
        /// Constructor that should be used for most use cases; Sql DB Schemas will be automatically resolved internally
        /// with caching support via SqlBulkHelpersSchemaLoaderCache.
        /// </summary>
        protected BaseHelper(ISqlBulkHelpersConfig bulkHelpersConfig = null)
        {
            this.BulkHelpersConfig = bulkHelpersConfig ?? SqlBulkHelpersConfig.DefaultConfig;
            this.BulkHelpersProcessingDefinition = SqlBulkHelpersProcessingDefinition.GetProcessingDefinition<T>();
        }

        #endregion

        protected virtual TableNameTerm GetMappedTableNameTerm(string tableNameOverride = null)
            => GenericType.GetSqlBulkHelpersMappedTableNameTerm(tableNameOverride);

        //BBernard
        //NOTE: MOST APIs will use a Transaction to get the DB Schema loader so this is the recommended method to use for all cases except for edge cases
        //      that cannot run within a Transaction (e.g. FullTableIndex APIs).
        //NOTE: Prevent SqlInjection - by validating that the TableName must be a valid value (as retrieved from the DB Schema) 
        //      we eliminate risk of Sql Injection.
        //NOTE: All other parameters are Strongly typed (vs raw Strings) thus eliminating risk of Sql Injection
        protected virtual SqlBulkHelpersTableDefinition GetTableSchemaDefinitionInternal(TableSchemaDetailLevel detailLevel, SqlConnection sqlConnection, SqlTransaction sqlTransaction = null, string tableNameOverride = null, bool forceCacheReload = false)
        {
            //Initialize the DB Schema loader (if specified, or from our Cache)...
            var dbSchemaLoader = SqlBulkHelpersSchemaLoaderCache.GetSchemaLoader(sqlConnection.ConnectionString);

            //BBernard
            //Load the Table Schema Definitions from the table name term provided or fall-back to use mapped data
            //NOTE: Prevent SqlInjection - by validating that the TableName must be a valid value (as retrieved from the DB Schema) 
            //      we eliminate risk of Sql Injection.
            var tableNameTerm = GetMappedTableNameTerm(tableNameOverride);
            var tableDefinition = dbSchemaLoader.GetTableSchemaDefinition(tableNameTerm, detailLevel, sqlConnection, sqlTransaction, forceCacheReload);
            AssertTableDefinitionIsValid(tableNameTerm, tableDefinition);

            return tableDefinition;
        }

        //BBernard
        //NOTE: MOST APIs will use a Transaction to get the DB Schema loader so this is the recommended method to use for all cases except for edge cases
        //      that cannot run within a Transaction (e.g. FullTableIndex APIs).
        //NOTE: Prevent SqlInjection - by validating that the TableName must be a valid value (as retrieved from the DB Schema) 
        //      we eliminate risk of Sql Injection.
        //NOTE: All other parameters are Strongly typed (vs raw Strings) thus eliminating risk of Sql Injection
        protected virtual async Task<SqlBulkHelpersTableDefinition> GetTableSchemaDefinitionInternalAsync(TableSchemaDetailLevel detailLevel, SqlConnection sqlConnection, SqlTransaction sqlTransaction = null, string tableNameOverride = null, bool forceCacheReload = false)
        {
            //Initialize the DB Schema loader (if specified, or from our Cache)...
            var dbSchemaLoader = SqlBulkHelpersSchemaLoaderCache.GetSchemaLoader(sqlConnection.ConnectionString);

            //BBernard
            //Load the Table Schema Definitions from the table name term provided or fall-back to use mapped data
            //NOTE: Prevent SqlInjection - by validating that the TableName must be a valid value (as retrieved from the DB Schema) 
            //      we eliminate risk of Sql Injection.
            var tableNameTerm = GetMappedTableNameTerm(tableNameOverride);
            var tableDefinition = await dbSchemaLoader.GetTableSchemaDefinitionAsync(tableNameTerm, detailLevel, sqlConnection, sqlTransaction, forceCacheReload).ConfigureAwait(false);
            AssertTableDefinitionIsValid(tableNameTerm, tableDefinition);

            return tableDefinition;
        }

        protected void AssertTableDefinitionIsValid(TableNameTerm tableNameTerm, SqlBulkHelpersTableDefinition tableDefinition)
        {
            if (tableDefinition == null)
                throw new ArgumentOutOfRangeException(nameof(tableNameTerm), $"The specified {nameof(tableNameTerm)} argument value of [{tableNameTerm}] is invalid; no table definition could be resolved.");
        }

    }
}
