using Microsoft.Data.SqlClient;
using System;

namespace SqlBulkHelpers
{
    public abstract class BaseHelper<T> where T : class
    {
        public ISqlBulkHelpersDBSchemaLoader SqlDbSchemaLoader { get; protected set; }

        public ISqlBulkHelpersConfig BulkHelpersConfig { get; protected set; }

        protected SqlBulkHelpersProcessingDefinition BulkHelpersProcessingDefinition { get; set; }

        #region Constructors

        /// <summary>
        /// Constructor that support passing in a customized Sql DB Schema Loader implementation.
        /// NOTE: This is usually a shared/cached/static class (such as SqlBulkHelpersDBSchemaStaticLoader) because it may 
        ///         cache the Sql DB Schema for maximum performance of all Bulk insert/update activities within an application; 
        ///         because Schemas usually do not change during the lifetime of an application restart.
        /// NOTE: With this overload there is no internal caching done, as it assumes that the instance provided is already
        ///         being managed for performance.
        /// </summary>
        /// <param name="sqlDbSchemaLoader"></param>
        /// <param name="bulkHelpersConfig"></param>
        protected BaseHelper(ISqlBulkHelpersDBSchemaLoader sqlDbSchemaLoader, ISqlBulkHelpersConfig bulkHelpersConfig = null)
            : this(bulkHelpersConfig)
        {
            this.SqlDbSchemaLoader = sqlDbSchemaLoader.AssertArgumentIsNotNull(nameof(sqlDbSchemaLoader));
        }

        /// <summary>
        /// Constructor that support passing in an SqlConnection Provider which will enable deferred (lazy) initialization of the
        /// Sql DB Schema Loader and Schema Definitions internally. the Sql DB Schema Loader will be resolved internally using
        /// the SqlBulkHelpersSchemaLoaderCache manager for performance.
        /// NOTE: With this overload the resolve ISqlBulkHelpersDBSchemaLoader will be resolved for this unique Connection,
        ///         as an internally managed cached resource for performance.
        /// </summary>
        /// <param name="sqlBulkHelpersConnectionProvider"></param>
        /// <param name="bulkHelpersConfig"></param>
        protected BaseHelper(ISqlBulkHelpersConnectionProvider sqlBulkHelpersConnectionProvider, ISqlBulkHelpersConfig bulkHelpersConfig = null)
            : this(bulkHelpersConfig)
        {
            sqlBulkHelpersConnectionProvider.AssertArgumentIsNotNull(nameof(sqlBulkHelpersConnectionProvider));
            this.SqlDbSchemaLoader = SqlBulkHelpersSchemaLoaderCache.GetSchemaLoader(sqlBulkHelpersConnectionProvider);
        }

        /// <summary>
        /// Convenience constructor that support passing in an existing Transaction for an open Connection; whereby
        /// the Sql DB Schema Loader will be resolved internally using the SqlBulkHelpersSchemaLoaderCache manager.
        /// NOTE: With this overload the resolve ISqlBulkHelpersDBSchemaLoader will be resolved for this unique Connection,
        ///         as an internally managed cached resource for performance.
        /// </summary>
        /// <param name="sqlTransaction"></param>
        /// <param name="bulkHelpersConfig"></param>
        protected BaseHelper(SqlTransaction sqlTransaction, ISqlBulkHelpersConfig bulkHelpersConfig = null)
            : this(bulkHelpersConfig)
        {
            sqlTransaction.AssertArgumentIsNotNull(nameof(sqlTransaction));
            this.SqlDbSchemaLoader = SqlBulkHelpersSchemaLoaderCache.GetSchemaLoader(sqlTransaction.Connection.ConnectionString);
        }

        //Private Constructor for common element initialization...
        private BaseHelper(ISqlBulkHelpersConfig bulkHelpersConfig = null)
        {
            this.BulkHelpersConfig = bulkHelpersConfig ?? SqlBulkHelpersConfig.DefaultConfig;
            this.BulkHelpersProcessingDefinition = SqlBulkHelpersProcessingDefinition.GetProcessingDefinition<T>();
        }

        #endregion

        //BBernard
        //NOTE: Prevent SqlInjection - by validating that the TableName must be a valid value (as retrieved from the DB Schema) 
        //      we eliminate risk of Sql Injection.
        //NOTE: All other parameters are Strongly typed (vs raw Strings) thus eliminating risk of Sql Injection
        protected virtual SqlBulkHelpersTableDefinition GetTableSchemaDefinitionInternal(SqlTransaction sqlTransaction, string tableNameParam = null)
        {
            //***STEP #1: Get the correct table name to lookup whether it is specified or if we fall back to the mapped data from the Model.
            string tableName = tableNameParam;
            if (string.IsNullOrWhiteSpace(tableName) && this.BulkHelpersProcessingDefinition.IsMappingLookupEnabled)
                tableName = this.BulkHelpersProcessingDefinition.MappedDbTableName;


            //***STEP #2: Load the Table Schema Definitions from the name provided or fall-back to use mapped data
            //BBernard
            //NOTE: Prevent SqlInjection - by validating that the TableName must be a valid value (as retrieved from the DB Schema) 
            //      we eliminate risk of Sql Injection.
            var tableDefinition = this.SqlDbSchemaLoader.GetTableSchemaDefinition(tableName, sqlTransaction);
            if (tableDefinition == null) 
                throw new ArgumentOutOfRangeException(nameof(tableNameParam), $"The specified {nameof(tableNameParam)} argument value of [{tableNameParam}] is invalid; no table definition could be resolved.");
            
            return tableDefinition;
        }

    }
}
