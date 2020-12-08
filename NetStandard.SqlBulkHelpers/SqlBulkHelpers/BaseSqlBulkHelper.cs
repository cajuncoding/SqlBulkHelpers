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

        protected virtual List<T> PostProcessEntitiesWithMergeResults(
            List<T> entityList, 
            List<MergeResult> mergeResultsList, 
            SqlBulkHelpersColumnDefinition identityColumnDefinition, 
            SqlMergeMatchQualifierExpression sqlMatchQualifierExpression
        )
        {
            var propDefs = SqlBulkHelpersObjectReflectionFactory.GetPropertyDefinitions<T>(identityColumnDefinition);
            var identityPropDef = propDefs.FirstOrDefault(pi => pi.IsIdentityProperty);
            var identityPropInfo = identityPropDef?.PropInfo;

            bool uniqueMatchValidationEnabled =
                sqlMatchQualifierExpression?.ThrowExceptionIfNonUniqueMatchesOccur == true;

            //Get all Items Inserted or Updated....
            var itemsInsertedOrUpdated = mergeResultsList.Where(r =>
                r.MergeAction.HasFlag(SqlBulkHelpersMergeAction.Insert) 
                || r.MergeAction.HasFlag(SqlBulkHelpersMergeAction.Update)
            );

            if (!uniqueMatchValidationEnabled)
            {
                //BBernard - 12/08/2020
                //If Unique Match validation is Disabled, we must take additional steps to properly synchronize with 
                //  the risk of multiple update matches....
                //NOTE: It is CRITICAL to sort by RowNumber & then by Identity value to handle edge cases where
                //      special Match Qualifier Fields are specified that are non-unique and result in multiple update
                //      matches; this ensures that at least correct data is matched/synced by the latest/last values ordered
                //      Ascending, when the validation is disabled.
                itemsInsertedOrUpdated = itemsInsertedOrUpdated.OrderBy(r => r.RowNumber).ThenBy(r => r.IdentityId);
            }

            var uniqueMatchesHashSet = new HashSet<int>();

            //foreach (var mergeResult in mergeResultsList.Where(r => r.MergeAction.HasFlag(SqlBulkHelpersMergeAction.Insert)))
            foreach (var mergeResult in itemsInsertedOrUpdated)
            {
                //ONLY Process uniqueness validation if necessary... otherwise skip the logic altogether.
                if (uniqueMatchValidationEnabled)
                {
                    if (uniqueMatchesHashSet.Contains(mergeResult.RowNumber))
                    {
                        throw new ArgumentOutOfRangeException(
                            nameof(mergeResultsList), 
                            "The bulk action has resulted in multiple matches for the the specified Match Qualifiers"
                            + $" [{sqlMatchQualifierExpression}] and the original Entities List cannot be safely updated."
                            + "Verify that the Match Qualifier fields result in unique matches or, if intentional, then "
                            + "this validation check may be disabled on the SqlMergeMatchQualifierExpression parameter."
                        );
                    }
                    else
                    {
                        uniqueMatchesHashSet.Add(mergeResult.RowNumber);
                    }
                }

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
