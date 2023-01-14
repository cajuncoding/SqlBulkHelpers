using System;
using System.Collections.Generic;
using Microsoft.Data.SqlClient;
using System.Threading.Tasks;

namespace SqlBulkHelpers
{
    internal partial class SqlBulkHelper<T> : BaseSqlBulkHelper<T> where T : class
    {
        #region Deprecated V1 API Overloads

        [Obsolete("This method is from v1 API and is deprecated, it will be removed eventually as it is replaced by the overload with optional parameters.")]
        public virtual async Task<IEnumerable<T>> BulkInsertAsync(
            IEnumerable<T> entityList,
            string tableName,
            SqlTransaction transaction,
            SqlMergeMatchQualifierExpression matchQualifierExpression = null
)
        {
            return await BulkInsertOrUpdateInternalAsync(
                entityList,
                SqlBulkHelpersMergeAction.Insert,
                transaction,
                tableNameParam: tableName,
                matchQualifierExpressionParam: matchQualifierExpression
            ).ConfigureAwait(false);
        }

        [Obsolete("This method is from v1 API and is deprecated, it will be removed eventually as it is replaced by the overload with optional parameters.")]
        public virtual async Task<IEnumerable<T>> BulkUpdateAsync(
            IEnumerable<T> entityList,
            string tableName,
            SqlTransaction transaction,
            SqlMergeMatchQualifierExpression matchQualifierExpression = null
        )
        {
            return await BulkInsertOrUpdateInternalAsync(
                entityList,
                SqlBulkHelpersMergeAction.Update,
                transaction,
                tableNameParam: tableName,
                matchQualifierExpressionParam: matchQualifierExpression
            ).ConfigureAwait(false);
        }

        [Obsolete("This method is from v1 API and is deprecated, it will be removed eventually as it is replaced by the overload with optional parameters.")]
        public virtual async Task<IEnumerable<T>> BulkInsertOrUpdateAsync(
            IEnumerable<T> entityList,
            string tableName,
            SqlTransaction transaction,
            SqlMergeMatchQualifierExpression matchQualifierExpression = null
        )
        {
            return await BulkInsertOrUpdateInternalAsync(
                entityList,
                SqlBulkHelpersMergeAction.InsertOrUpdate,
                transaction,
                tableNameParam: tableName,
                matchQualifierExpressionParam: matchQualifierExpression
            ).ConfigureAwait(false);
        }

        [Obsolete("This method is from v1 API and is deprecated, it will be removed eventually as it is replaced by the overload with optional parameters.")]
        public virtual IEnumerable<T> BulkInsert(
            IEnumerable<T> entityList,
            string tableName,
            SqlTransaction transaction,
            SqlMergeMatchQualifierExpression matchQualifierExpression = null
        )
        {
            return BulkInsertOrUpdateInternal(
                entityList,
                SqlBulkHelpersMergeAction.Insert,
                transaction,
                tableNameParam: tableName,
                matchQualifierExpressionParam: matchQualifierExpression
            );
        }

        [Obsolete("This method is from v1 API and is deprecated, it will be removed eventually as it is replaced by the overload with optional parameters.")]
        public virtual IEnumerable<T> BulkUpdate(
            IEnumerable<T> entityList,
            string tableName,
            SqlTransaction transaction,
            SqlMergeMatchQualifierExpression matchQualifierExpression = null
        )
        {
            return BulkInsertOrUpdateInternal(
                entityList,
                SqlBulkHelpersMergeAction.Update,
                transaction,
                tableNameParam: tableName,
                matchQualifierExpressionParam: matchQualifierExpression
            );
        }

        [Obsolete("This method is from v1 API and is deprecated, it will be removed eventually as it is replaced by the overload with optional parameters.")]
        public virtual IEnumerable<T> BulkInsertOrUpdate(
            IEnumerable<T> entityList,
            string tableName,
            SqlTransaction transaction,
            SqlMergeMatchQualifierExpression matchQualifierExpression = null
        )
        {
            return BulkInsertOrUpdateInternal(
                entityList,
                SqlBulkHelpersMergeAction.InsertOrUpdate,
                transaction,
                tableNameParam: tableName,
                matchQualifierExpressionParam: matchQualifierExpression
            );
        }

        #endregion
    }
}
