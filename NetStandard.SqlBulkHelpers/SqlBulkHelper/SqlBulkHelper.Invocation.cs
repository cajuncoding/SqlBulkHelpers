using System;
using System.Collections.Generic;
using Microsoft.Data.SqlClient;
using System.Threading.Tasks;

namespace SqlBulkHelpers
{
    internal partial class SqlBulkHelper<T> : BaseSqlBulkHelper<T> where T : class
    {
        #region Constructors

        /// <inheritdoc/>
        public SqlBulkHelper(ISqlBulkHelpersConfig bulkHelpersConfig = null)
            : base(bulkHelpersConfig)
        {
        }

        #endregion

        #region Async ISqlBulkHelper<T> implementations

        public virtual async Task<IEnumerable<T>> BulkInsertAsync(
            IEnumerable<T> entityList,
            SqlTransaction sqlTransaction,
            string tableNameParam = null,
            SqlMergeMatchQualifierExpression matchQualifierExpression = null
        )
        {
            return await BulkInsertOrUpdateInternalAsync(
                entityList,
                SqlBulkHelpersMergeAction.Insert,
                sqlTransaction,
                tableNameParam: tableNameParam,
                matchQualifierExpressionParam: matchQualifierExpression
            ).ConfigureAwait(false);
        }

        public virtual async Task<IEnumerable<T>> BulkUpdateAsync(
            IEnumerable<T> entityList,
            SqlTransaction sqlTransaction,
            string tableNameParam = null,
            SqlMergeMatchQualifierExpression matchQualifierExpression = null
        )
        {
            return await BulkInsertOrUpdateInternalAsync(
                entityList,
                SqlBulkHelpersMergeAction.Update,
                sqlTransaction,
                tableNameParam: tableNameParam,
                matchQualifierExpressionParam: matchQualifierExpression
            ).ConfigureAwait(false);
        }

        public virtual async Task<IEnumerable<T>> BulkInsertOrUpdateAsync(
            IEnumerable<T> entityList,
            SqlTransaction sqlTransaction,
            string tableNameParam = null,
            SqlMergeMatchQualifierExpression matchQualifierExpression = null
        )
        {
            return await BulkInsertOrUpdateInternalAsync(
                entityList,
                SqlBulkHelpersMergeAction.InsertOrUpdate,
                sqlTransaction,
                tableNameParam: tableNameParam,
                matchQualifierExpressionParam: matchQualifierExpression
            ).ConfigureAwait(false);
        }

        /// <summary>
        /// Retrieve the Schema Definition for the specified Table using either an SqlConnection or with an existing SqlTransaction.
        /// </summary>
        /// <param name="sqlConnection"></param>
        /// <param name="sqlTransaction"></param>
        /// <param name="tableNameOverride"></param>
        /// <param name="detailLevel"></param>
        /// <param name="forceCacheReload"></param>
        /// <returns></returns>
        public async Task<SqlBulkHelpersTableDefinition> GetTableSchemaDefinitionAsync(
            SqlConnection sqlConnection,
            SqlTransaction sqlTransaction = null,
            string tableNameOverride = null,
            TableSchemaDetailLevel detailLevel = TableSchemaDetailLevel.ExtendedDetails,
            bool forceCacheReload = false
        )
        {
            var tableDefinition = await this.GetTableSchemaDefinitionInternalAsync(
                detailLevel,
                sqlConnection,
                sqlTransaction,
                tableNameOverride,
                forceCacheReload
            ).ConfigureAwait(false);

            return tableDefinition;
        }

        #endregion

        #region Sync ISqlBulkHelper<T> implementations

        public virtual IEnumerable<T> BulkInsert(
            IEnumerable<T> entityList,
            SqlTransaction sqlTransaction,
            string tableNameParam = null,
            SqlMergeMatchQualifierExpression matchQualifierExpression = null
        )
        {
            return BulkInsertOrUpdateInternal(
                entityList,
                SqlBulkHelpersMergeAction.Insert,
                sqlTransaction,
                tableNameParam: tableNameParam,
                matchQualifierExpressionParam: matchQualifierExpression
            );
        }

        public virtual IEnumerable<T> BulkUpdate(
            IEnumerable<T> entityList,
            SqlTransaction sqlTransaction,
            string tableNameParam = null,
            SqlMergeMatchQualifierExpression matchQualifierExpression = null
        )
        {
            return BulkInsertOrUpdateInternal(
                entityList,
                SqlBulkHelpersMergeAction.Update,
                sqlTransaction,
                tableNameParam: tableNameParam,
                matchQualifierExpressionParam: matchQualifierExpression
            );
        }

        public virtual IEnumerable<T> BulkInsertOrUpdate(
            IEnumerable<T> entityList,
            SqlTransaction sqlTransaction,
            string tableNameParam = null,
            SqlMergeMatchQualifierExpression matchQualifierExpression = null
        )
        {
            return BulkInsertOrUpdateInternal(
                entityList,
                SqlBulkHelpersMergeAction.InsertOrUpdate,
                sqlTransaction,
                tableNameParam: tableNameParam,
                matchQualifierExpressionParam: matchQualifierExpression
            );
        }

        /// <summary>
        /// Retrieve the Schema Definition for the specified Table using either an SqlConnection or with an existing SqlTransaction.
        /// </summary>
        /// <param name="sqlConnection"></param>
        /// <param name="sqlTransaction"></param>
        /// <param name="tableNameOverride"></param>
        /// <param name="detailLevel"></param>
        /// <param name="forceCacheReload"></param>
        /// <returns></returns>
        public SqlBulkHelpersTableDefinition GetTableSchemaDefinition(
            SqlConnection sqlConnection, 
            SqlTransaction sqlTransaction = null, 
            string tableNameOverride = null, 
            TableSchemaDetailLevel detailLevel = TableSchemaDetailLevel.ExtendedDetails,
            bool forceCacheReload = false
        )
        {
            var tableDefinition = this.GetTableSchemaDefinitionInternal(
                detailLevel, 
                sqlConnection, 
                sqlTransaction, 
                tableNameOverride, 
                forceCacheReload
            );
            return tableDefinition;
        }

        #endregion
    }
}
