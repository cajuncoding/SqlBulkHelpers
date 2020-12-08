using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Linq;

namespace SqlBulkHelpers
{
    //BBernard - Base Class for future flexibility...
    public abstract class BaseSqlBulkHelper<T> where T: class
    {
        public ISqlBulkHelpersDBSchemaLoader SqlDbSchemaLoader { get; protected set; }

        /// <summary>
        /// Constructor that support passing in a customized Sql DB Schema Loader implementation.
        /// NOTE: This is usually a shared/cached/static class (such as SqlBulkHelpersDBSchemaStaticLoader) because it may 
        ///         cache the Sql DB Schema for maximum performance of all Bulk insert/update activities within an application; 
        ///         because Schemas usually do not change during the lifetime of an application restart.
        /// NOTE: With this overload there is no internal caching done, as it assumes that the instance provided is already
        ///         being managed for performance (e.g. is static in the consuming code logic).
        /// </summary>
        /// <param name="sqlDbSchemaLoader"></param>
        protected BaseSqlBulkHelper(ISqlBulkHelpersDBSchemaLoader sqlDbSchemaLoader)
        {
            this.SqlDbSchemaLoader = sqlDbSchemaLoader;
        }

        /// <summary>
        /// Convenience constructor that supports easier initialization of the DB Schema loader by passing in the
        /// Sql Connection Provider using the default a SqlBulkHelpersDBSchemaStaticLoader implementation.
        /// </summary>
        /// <param name="sqlBulkHelpersConnectionProvider"></param>
        protected BaseSqlBulkHelper(ISqlBulkHelpersConnectionProvider sqlBulkHelpersConnectionProvider)
        {
            var sqlConnProvider = sqlBulkHelpersConnectionProvider.AssertArgumentIsNotNull(nameof(sqlBulkHelpersConnectionProvider));
            this.SqlDbSchemaLoader = SqlBulkHelpersStaticSchemaLoaderCache.GetSchemaLoader(sqlConnProvider);
        }

        /// <summary>
        /// Default constructor that will implement the default implementation for Sql Server DB Schema loading that supports static caching/lazy loading for performance.
        /// </summary>
        protected BaseSqlBulkHelper()
        {
            //Initialize the default Sql DB Schema Loader (which is dependent on the Sql Connection Provider).
            this.SqlDbSchemaLoader = SqlBulkHelpersDBSchemaStaticLoader.Default;
        }

        public virtual SqlBulkHelpersTableDefinition GetTableSchemaDefinition(String tableName)
        {
            //BBernard
            //NOTE: Prevent SqlInjection - by validating that the TableName must be a valid value (as retrieved from the DB Schema) 
            //      we eliminate risk of Sql Injection.
            var tableDefinition = this.SqlDbSchemaLoader.GetTableSchemaDefinition(tableName);
            if (tableDefinition == null) throw new ArgumentOutOfRangeException(nameof(tableName), $"The specified {nameof(tableName)} argument value of [{tableName}] is invalid.");
            return tableDefinition;
        }

        protected virtual DataTable ConvertEntitiesToDataTableHelper(
            IEnumerable<T> entityList, 
            SqlBulkHelpersColumnDefinition identityColumnDefinition = null
        )
        {
            SqlBulkHelpersObjectMapper sqlBulkHelperModelMapper = new SqlBulkHelpersObjectMapper();
            DataTable dataTable = sqlBulkHelperModelMapper.ConvertEntitiesToDataTable(entityList, identityColumnDefinition);
            return dataTable;
        }

        protected virtual SqlBulkCopy CreateSqlBulkCopyHelper(
            DataTable dataTable, 
            SqlBulkHelpersTableDefinition tableDefinition, 
            SqlTransaction transaction
        )
        {
            var factory = new SqlBulkCopyFactory(); //Load with all Defaults from our Factory.
            var sqlBulkCopy = factory.CreateSqlBulkCopy(dataTable, tableDefinition, transaction);
            return sqlBulkCopy;
        }

        //TODO: BBernard - If beneficial, we can Add Caching here at this point to cache the fully formed Merge Queries!
        protected virtual SqlMergeScriptResults BuildSqlMergeScriptsHelper(
            SqlBulkHelpersTableDefinition tableDefinition, 
            SqlBulkHelpersMergeAction mergeAction,
            SqlMergeMatchQualifierExpression matchQualifierExpression = null
        )
        {
            var mergeScriptBuilder = new SqlBulkHelpersMergeScriptBuilder();
            var sqlScripts = mergeScriptBuilder.BuildSqlMergeScripts(
                tableDefinition, 
                mergeAction, 
                matchQualifierExpression
            );

            return sqlScripts;
        }

        //NOTE: This is Protected Class because it is ONLY needed by the SqlBulkHelper implementations with Merge Operations 
        //          for organized code when post-processing results.
        protected class MergeResult
        {
            public int RowNumber { get; set; }
            public int IdentityId { get; set; }
            public SqlBulkHelpersMergeAction MergeAction { get; set; }
        }

        protected virtual List<T> PostProcessEntitiesWithMergeResults(List<T> entityList, List<MergeResult> mergeResultsList, SqlBulkHelpersColumnDefinition identityColumnDefinition)
        {
            var propDefs = SqlBulkHelpersObjectReflectionFactory.GetPropertyDefinitions<T>(identityColumnDefinition);
            var identityPropDef = propDefs.FirstOrDefault(pi => pi.IsIdentityProperty);
            var identityPropInfo = identityPropDef?.PropInfo;

            foreach (var mergeResult in mergeResultsList.Where(r => r.MergeAction.HasFlag(SqlBulkHelpersMergeAction.Insert)))
            {
                //NOTE: List is 0 (zero) based, but our RowNumber is 1 (one) based.
                var entity = entityList[mergeResult.RowNumber - 1];
                
                //BBernard
                //GENERICALLY Set the Identity Value to the Int value returned, this eliminates any dependency on a Base Class!
                //TODO: If needed we can optimize this with a Delegate for faster property access (vs pure Reflection).
                //(entity as Debug.ConsoleApp.TestElement).Id = mergeResult.IdentityId;
                identityPropInfo?.SetValue(entity, mergeResult.IdentityId);
            }

            //Return the Updated Entities List (for fluent chain-ability) and easier to read code
            //NOTE: even though we have actually mutated the original list by reference this is very intuitive and helps with code readability.
            return entityList;
        }

    }
}
