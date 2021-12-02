using System;
using System.Collections.Generic;
using System.Data;
using Microsoft.Data.SqlClient;
using System.Linq;
using System.Reflection;
using SqlBulkHelpers.SqlBulkHelpers.Interfaces;

namespace SqlBulkHelpers
{
    //BBernard - Base Class for future flexibility...
    public abstract class BaseSqlBulkHelper<T> where T: class
    {
        public const int DefaultBulkOperationTimeoutSeconds = 30;

        public ISqlBulkHelpersDBSchemaLoader SqlDbSchemaLoader { get; protected set; }

        public int BulkOperationTimeoutSeconds { get; set; }
        
        #region Constructors

        /// <summary>
        /// Constructor that support passing in a customized Sql DB Schema Loader implementation.
        /// NOTE: This is usually a shared/cached/static class (such as SqlBulkHelpersDBSchemaStaticLoader) because it may 
        ///         cache the Sql DB Schema for maximum performance of all Bulk insert/update activities within an application; 
        ///         because Schemas usually do not change during the lifetime of an application restart.
        /// NOTE: With this overload there is no internal caching done, as it assumes that the instance provided is already
        ///         being managed for performance (e.g. is static in the consuming code logic).
        /// </summary>
        /// <param name="sqlDbSchemaLoader"></param>
        /// <param name="timeoutSeconds"></param>
        protected BaseSqlBulkHelper(ISqlBulkHelpersDBSchemaLoader sqlDbSchemaLoader, int timeoutSeconds = DefaultBulkOperationTimeoutSeconds)
        {
            this.SqlDbSchemaLoader = sqlDbSchemaLoader;
            this.BulkOperationTimeoutSeconds = timeoutSeconds;
        }

        /// <summary>
        /// Constructor that support passing in an SqlConnection Provider which will enable deferred (lazy) initialization of the
        /// Sql DB Schema Loader and Schema Definitions internally. the Sql DB Schema Loader will be resolved internally using
        /// the SqlBulkHelpersSchemaLoaderCache manager for performance.
        /// NOTE: With this overload the resolve ISqlBulkHelpersDBSchemaLoader will be resolved for this unique Connection,
        ///         as an internally managed cached resource for performance.
        /// </summary>
        /// <param name="sqlBulkHelpersConnectionProvider"></param>
        /// <param name="timeoutSeconds"></param>
        protected BaseSqlBulkHelper(ISqlBulkHelpersConnectionProvider sqlBulkHelpersConnectionProvider, int timeoutSeconds = DefaultBulkOperationTimeoutSeconds)
        {
            this.SqlDbSchemaLoader = SqlBulkHelpersSchemaLoaderCache.GetSchemaLoader(sqlBulkHelpersConnectionProvider);
            this.BulkOperationTimeoutSeconds = timeoutSeconds;
        }

        /// <summary>
        /// Convenience constructor that support passing in an existing Transaction for an open Connection; whereby
        /// the Sql DB Schema Loader will be resolved internally using the SqlBulkHelpersSchemaLoaderCache manager.
        /// NOTE: With this overload the resolve ISqlBulkHelpersDBSchemaLoader will be resolved for this unique Connection,
        ///         as an internally managed cached resource for performance.
        /// </summary>
        /// <param name="sqlTransaction"></param>
        /// <param name="timeoutSeconds"></param>
        protected BaseSqlBulkHelper(SqlTransaction sqlTransaction, int timeoutSeconds = DefaultBulkOperationTimeoutSeconds)
            : this(sqlTransaction?.Connection, sqlTransaction, timeoutSeconds)
        {
        }

        /// <summary>
        /// Convenience constructor that support passing in an existing Transaction for an open Connection; whereby
        /// the Sql DB Schema Loader will be resolved internally using the SqlBulkHelpersSchemaLoaderCache manager.
        /// NOTE: With this overload the resolve ISqlBulkHelpersDBSchemaLoader will be resolved for this unique Connection,
        ///         as an internally managed cached resource for performance.
        /// </summary>
        /// <param name="sqlConnection"></param>
        /// <param name="sqlTransaction"></param>
        /// <param name="timeoutSeconds"></param>
        protected BaseSqlBulkHelper(SqlConnection sqlConnection, SqlTransaction sqlTransaction = null, int timeoutSeconds = DefaultBulkOperationTimeoutSeconds)
        {
            //For safety since a Connection was passed in then we generally should immediately initialize the Schema Loader,
            //      because this connection or transaction may no longer be valid later.
            this.SqlDbSchemaLoader = SqlBulkHelpersSchemaLoaderCache.GetSchemaLoader(sqlConnection, sqlTransaction, true);
            this.BulkOperationTimeoutSeconds = timeoutSeconds;
        }

        #endregion

        public virtual SqlBulkHelpersTableDefinition GetTableSchemaDefinition(string tableName)
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
            SqlTransaction transaction,
            int timeoutSeconds
        )
        {
            //Initialize the BulkCopy Factory class with parameters...
            var factory = new SqlBulkCopyFactory()
            {
                BulkCopyTimeoutSeconds = timeoutSeconds
            };

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
            //BBernard - 12/01/2021
            //Added Optimization to support interface based Identity Setter which may be optionally implemented
            //  directly on the models...
            //However, if the Generic Type doesn't implement our Interface ISqlBulkHelperIdentitySetter then
            //  we attempt to use Reflection to set the value...
            PropertyInfo identityPropInfo = null;
            if (!typeof(ISqlBulkHelperIdentitySetter).IsAssignableFrom(typeof(T)))
            {
                var propDefs = SqlBulkHelpersObjectReflectionFactory.GetPropertyDefinitions<T>(identityColumnDefinition);
                var identityPropDef = propDefs.FirstOrDefault(pi => pi.IsIdentityProperty);
                identityPropInfo = identityPropDef?.PropInfo;

                //If there is no Identity Column (e.g. no Identity Column Definition and/or no PropInfo can be found)
                //  then we can short circuit.
                if (identityPropInfo == null)
                    return entityList;
            }

            bool uniqueMatchValidationEnabled = sqlMatchQualifierExpression?.ThrowExceptionIfNonUniqueMatchesOccur == true;

            ////Get all Items Inserted or Updated....
            //NOTE: With the support for Custom Match Qualifiers we really need to handle Inserts & Updates,
            //      so there's no reason to filter the merge results anymore; this is more performant.
            var itemsInsertedOrUpdated = mergeResultsList;
            //var itemsInsertedOrUpdated = mergeResultsList.Where(r =>
            //    r.MergeAction.HasFlag(SqlBulkHelpersMergeAction.Insert) 
            //    || r.MergeAction.HasFlag(SqlBulkHelpersMergeAction.Update)
            //);

            //BBernard this isn't needed since we updated the SQL Merge Script to sort correctly before returning
            //  data.... but leaving it here for future reference in case it's needed.
            //if (!uniqueMatchValidationEnabled)
            //{
            //    //BBernard - 12/08/2020
            //    //If Unique Match validation is Disabled, we must take additional steps to properly synchronize with 
            //    //  the risk of multiple update matches....
            //    //NOTE: It is CRITICAL to sort by RowNumber & then by Identity value to handle edge cases where
            //    //      special Match Qualifier Fields are specified that are non-unique and result in multiple update
            //    //      matches; this ensures that at least correct data is matched/synced by the latest/last values ordered
            //    //      Ascending, when the validation is disabled.
            //    itemsInsertedOrUpdated = itemsInsertedOrUpdated.OrderBy(r => r.RowNumber).ThenBy(r => r.IdentityId);
            //}

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
                //If the entity supports our interface we can set the value with native performance via the Interface!
                if (entity is ISqlBulkHelperIdentitySetter identitySetterEntity)
                {
                    identitySetterEntity.SetIdentityId(mergeResult.IdentityId);
                }
                else
                {
                    //GENERICALLY Set the Identity Value to the Int value returned, this eliminates any dependency on a Base Class!
                    //TODO: If needed we can optimize this with a Delegate for faster property access (vs pure Reflection).
                    identityPropInfo?.SetValue(entity, mergeResult.IdentityId);
                }
            }

            //Return the Updated Entities List (for fluent chain-ability) and easier to read code
            //NOTE: even though we have actually mutated the original list by reference this is very intuitive and helps with code readability.
            return entityList;
        }

    }
}
