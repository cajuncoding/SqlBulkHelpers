using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using Microsoft.Data.SqlClient;
using System.Threading.Tasks;

namespace SqlBulkHelpers
{
    public partial class SqlBulkHelper<T> : BaseSqlBulkHelper<T>, ISqlBulkHelper<T> where T : class
    {
        #region Constructors

        /// <inheritdoc/>
        public SqlBulkHelper(ISqlBulkHelpersDBSchemaLoader sqlDbSchemaLoader, int perBatchTimeoutSeconds = SqlBulkHelpersConstants.DefaultBulkOperationPerBatchTimeoutSeconds)
            : base(sqlDbSchemaLoader, perBatchTimeoutSeconds)
        {
        }

        /// <inheritdoc/>
        public SqlBulkHelper(ISqlBulkHelpersConnectionProvider sqlBulkHelpersConnectionProvider, int perBatchTimeoutSeconds = SqlBulkHelpersConstants.DefaultBulkOperationPerBatchTimeoutSeconds)
            : base(sqlBulkHelpersConnectionProvider, perBatchTimeoutSeconds)
        {
        }

        /// <inheritdoc/>
        public SqlBulkHelper(SqlTransaction sqlTransaction, int perBatchTimeoutSeconds = SqlBulkHelpersConstants.DefaultBulkOperationPerBatchTimeoutSeconds)
            : base(sqlTransaction, perBatchTimeoutSeconds)
        {
        }

        /// <inheritdoc/>
        public SqlBulkHelper(SqlConnection sqlConnection, SqlTransaction sqlTransaction = null, int perBatchTimeoutSeconds = SqlBulkHelpersConstants.DefaultBulkOperationPerBatchTimeoutSeconds)
            : base(sqlConnection, sqlTransaction, perBatchTimeoutSeconds)
        {
        }

        #endregion

        #region ISqlBulkHelper<T> implementations

        public virtual async Task<IEnumerable<T>> BulkInsertAsync(
            IEnumerable<T> entityList,
            SqlTransaction sqlTransaction,
            string tableNameParam = null,
            SqlMergeMatchQualifierExpression matchQualifierExpression = null,
            bool? enableSqlBulkCopyTableLockParam = null
        )
        {
            return await BulkInsertOrUpdateInternalAsync(
                entityList,
                SqlBulkHelpersMergeAction.Insert,
                sqlTransaction,
                tableNameParam: tableNameParam,
                matchQualifierExpressionParam: matchQualifierExpression,
                enableSqlBulkCopyTableLockParam: enableSqlBulkCopyTableLockParam
            ).ConfigureAwait(false);
        }

        public virtual async Task<IEnumerable<T>> BulkUpdateAsync(
            IEnumerable<T> entityList,
            SqlTransaction sqlTransaction,
            string tableNameParam = null,
            SqlMergeMatchQualifierExpression matchQualifierExpression = null,
            bool? enableSqlBulkCopyTableLockParam = null
        )
        {
            return await BulkInsertOrUpdateInternalAsync(
                entityList,
                SqlBulkHelpersMergeAction.Update,
                sqlTransaction,
                tableNameParam: tableNameParam,
                matchQualifierExpressionParam: matchQualifierExpression,
                enableSqlBulkCopyTableLockParam: enableSqlBulkCopyTableLockParam
            ).ConfigureAwait(false);
        }

        public virtual async Task<IEnumerable<T>> BulkInsertOrUpdateAsync(
            IEnumerable<T> entityList,
            SqlTransaction sqlTransaction,
            string tableNameParam = null,
            SqlMergeMatchQualifierExpression matchQualifierExpression = null,
            bool? enableSqlBulkCopyTableLockParam = null
        )
        {
            return await BulkInsertOrUpdateInternalAsync(
                entityList,
                SqlBulkHelpersMergeAction.InsertOrUpdate,
                sqlTransaction,
                tableNameParam: tableNameParam,
                matchQualifierExpressionParam: matchQualifierExpression,
                enableSqlBulkCopyTableLockParam: enableSqlBulkCopyTableLockParam
            ).ConfigureAwait(false);
        }


        public virtual IEnumerable<T> BulkInsert(
            IEnumerable<T> entityList,
            SqlTransaction sqlTransaction,
            string tableNameParam = null,
            SqlMergeMatchQualifierExpression matchQualifierExpression = null,
            bool? enableSqlBulkCopyTableLockParam = null
        )
        {
            return BulkInsertOrUpdateInternal(
                entityList,
                SqlBulkHelpersMergeAction.Insert,
                sqlTransaction,
                tableNameParam: tableNameParam,
                matchQualifierExpressionParam: matchQualifierExpression,
                enableSqlBulkCopyTableLockParam: enableSqlBulkCopyTableLockParam
            );
        }

        public virtual IEnumerable<T> BulkUpdate(
            IEnumerable<T> entityList,
            SqlTransaction sqlTransaction,
            string tableNameParam = null,
            SqlMergeMatchQualifierExpression matchQualifierExpression = null,
            bool? enableSqlBulkCopyTableLockParam = null
        )
        {
            return BulkInsertOrUpdateInternal(
                entityList,
                SqlBulkHelpersMergeAction.Update,
                sqlTransaction,
                tableNameParam: tableNameParam,
                matchQualifierExpressionParam: matchQualifierExpression,
                enableSqlBulkCopyTableLockParam: enableSqlBulkCopyTableLockParam
            );
        }

        public virtual IEnumerable<T> BulkInsertOrUpdate(
            IEnumerable<T> entityList,
            SqlTransaction sqlTransaction,
            string tableNameParam = null,
            SqlMergeMatchQualifierExpression matchQualifierExpression = null,
            bool? enableSqlBulkCopyTableLockParam = null
        )
        {
            return BulkInsertOrUpdateInternal(
                entityList,
                SqlBulkHelpersMergeAction.InsertOrUpdate,
                sqlTransaction,
                tableNameParam: tableNameParam,
                matchQualifierExpressionParam: matchQualifierExpression,
                enableSqlBulkCopyTableLockParam: enableSqlBulkCopyTableLockParam
            );
        }

        #endregion

    }
}
