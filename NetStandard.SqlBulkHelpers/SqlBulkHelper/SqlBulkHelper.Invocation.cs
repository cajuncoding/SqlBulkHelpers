using System;
using System.Collections.Generic;
using Microsoft.Data.SqlClient;
using System.Threading.Tasks;

namespace SqlBulkHelpers
{
    public partial class SqlBulkHelper<T> : BaseSqlBulkHelper<T>, ISqlBulkHelper<T> where T : class
    {
        #region Constructors

        /// <inheritdoc/>
        public SqlBulkHelper(ISqlBulkHelpersDBSchemaLoader sqlDbSchemaLoader, ISqlBulkHelpersConfig bulkHelpersConfig = null)
            : base(sqlDbSchemaLoader, bulkHelpersConfig)
        {
        }

        /// <inheritdoc/>
        public SqlBulkHelper(ISqlBulkHelpersConnectionProvider sqlBulkHelpersConnectionProvider, ISqlBulkHelpersConfig bulkHelpersConfig = null)
            : base(sqlBulkHelpersConnectionProvider, bulkHelpersConfig)
        {
        }

        /// <inheritdoc/>
        public SqlBulkHelper(SqlTransaction sqlTransaction, ISqlBulkHelpersConfig bulkHelpersConfig = null)
            : base(sqlTransaction, bulkHelpersConfig)
        {
        }

        #endregion

        #region ISqlBulkHelper<T> implementations

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

        public SqlBulkHelpersTableDefinition GetTableSchemaDefinition(SqlTransaction sqlTransaction, string tableName = null)
        {
            var definitions = this.GetTableSchemaAndProcessingDefinitions(sqlTransaction, tableName);
            return definitions.TableDefinition;
        }

        #endregion
    }
}
