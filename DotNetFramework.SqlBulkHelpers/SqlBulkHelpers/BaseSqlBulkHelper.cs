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
        public virtual ISqlBulkHelpersDBSchemaLoader SqlDbSchemaLoader { get; protected set; }

        /// <summary>
        /// Constructor that support passing in a customized Sql DB Schema Loader implementation.
        /// </summary>
        /// <param name="sqlDbSchemaLoader"></param>
        protected BaseSqlBulkHelper(ISqlBulkHelpersDBSchemaLoader sqlDbSchemaLoader)
        {
            this.SqlDbSchemaLoader = sqlDbSchemaLoader.AssertArgumentNotNull(nameof(sqlDbSchemaLoader));
        }

        /// <summary>
        /// Default constructor that will implement the default implementation for Sql Server DB Schema loading that supports static caching/lazy loading for performance.
        /// </summary>
        protected BaseSqlBulkHelper()
        {
            //Initialize the default Sql DB Schema Loader (which is dependent on the Sql Connection Provider);
            this.SqlDbSchemaLoader = SqlBulkHelpersDBSchemaStaticLoader.Default;
        }

        public virtual SqlBulkHelpersTableDefinition GetTableSchemaDefinition(String tableName)
        {
            //BBernard
            //NOTE: Prevent SqlInjection - by validating that the TableName must be a valid value (as retrieved from the DB Schema) 
            //      we eliminate risk of Sql Injection.
            var tableDefinition = this.SqlDbSchemaLoader.GetTableSchemaDefinition(tableName);
            if (tableDefinition == null) throw new ArgumentOutOfRangeException(nameof(tableName), $"The specified argument [{tableName}] is invalid.");
            return tableDefinition;
        }

        protected virtual DataTable ConvertEntitiesToDataTableHelper(IEnumerable<T> entityList, SqlBulkHelpersColumnDefinition identityColumnDefinition = null)
        {
            SqlBulkHelpersObjectMapper _sqlBulkHelperModelMapper = new SqlBulkHelpersObjectMapper();
            DataTable dataTable = _sqlBulkHelperModelMapper.ConvertEntitiesToDataTable(entityList, identityColumnDefinition);
            return dataTable;
        }

        protected virtual SqlBulkCopy CreateSqlBulkCopyHelper(DataTable dataTable, SqlBulkHelpersTableDefinition tableDefinition, SqlTransaction transaction)
        {
            var factory = new SqlBulkCopyFactory(); //Load with all Defaults from our Factory.
            var sqlBulkCopy = factory.CreateSqlBulkCopy(dataTable, tableDefinition, transaction);
            return sqlBulkCopy;
        }

        //TODO: BBernard - If beneficial, we can Add Caching here at this point to cache the fully formed Merge Queries!
        protected virtual SqlMergeScriptResults BuildSqlMergeScriptsHelper(SqlBulkHelpersTableDefinition tableDefinition, SqlBulkHelpersMergeAction mergeAction)
        {
            var mergeScriptBuilder = new SqlBulkHelpersMergeScriptBuilder();
            var sqlScripts = mergeScriptBuilder.BuildSqlMergeScripts(tableDefinition, mergeAction);
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
            var identityPropInfo = identityPropDef.PropInfo;

            foreach (var mergeResult in mergeResultsList.Where(r => r.MergeAction.HasFlag(SqlBulkHelpersMergeAction.Insert)))
            {
                //NOTE: List is 0 (zero) based, but our RowNumber is 1 (one) based.
                var entity = entityList[mergeResult.RowNumber - 1];
                
                //BBernard
                //GENERICALLY Set the Identity Value to the Int value returned, this eliminates any dependency on a Base Class!
                //TODO: If needed we can optimize this with a Delegate for faster property access (vs pure Reflection).
                //(entity as Debug.ConsoleApp.TestElement).Id = mergeResult.IdentityId;
                identityPropInfo.SetValue(entity, mergeResult.IdentityId);
            }

            //Return the Updated Entities List (for chainability) and easier to read code
            //NOTE: even though we have actually mutated the original list by reference this helps with code readability.
            return entityList;
        }

    }
}
