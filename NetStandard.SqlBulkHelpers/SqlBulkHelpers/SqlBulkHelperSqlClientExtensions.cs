using Microsoft.Data.SqlClient;
using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;

namespace SqlBulkHelpers.SqlBulkHelpers
{
    public static class SqlBulkHelperSqlClientExtensions
    {
        #region ISqlBulkHelper<T> implementations

        public static async Task<IEnumerable<T>> BulkInsertAsync<T>(
            this SqlTransaction sqlTransaction,
            IEnumerable<T> entities,
            string tableName = null,
            SqlMergeMatchQualifierExpression matchQualifierExpressionParam = null,
            bool? enableSqlBulkCopyTableLockParam = null
        ) where T : class
        {

            var results = await new SqlBulkHelper<T>(sqlTransaction)
                .BulkInsertAsync(
                    entities, 
                    sqlTransaction, 
                    tableNameParam: tableName, 
                    matchQualifierExpression: matchQualifierExpressionParam, 
                    enableSqlBulkCopyTableLockParam: enableSqlBulkCopyTableLockParam
                )
                .ConfigureAwait(false);

            return results;
        }

        public static async Task<IEnumerable<T>> BulkUpdateAsync<T>(
            this SqlTransaction sqlTransaction,
            IEnumerable<T> entities,
            string tableName = null,
            SqlMergeMatchQualifierExpression matchQualifierExpressionParam = null,
            bool? enableSqlBulkCopyTableLockParam = null
        ) where T : class
        {
            var results = await new SqlBulkHelper<T>(sqlTransaction)
                .BulkUpdateAsync(entities,
                    sqlTransaction,
                    tableNameParam: tableName,
                    matchQualifierExpression: matchQualifierExpressionParam,
                    enableSqlBulkCopyTableLockParam: enableSqlBulkCopyTableLockParam
                )
                .ConfigureAwait(false);

            return results;
        }

        public static async Task<IEnumerable<T>> BulkInsertOrUpdateAsync<T>(
            this SqlTransaction sqlTransaction,
            IEnumerable<T> entities,
            string tableName = null,
            SqlMergeMatchQualifierExpression matchQualifierExpressionParam = null,
            bool? enableSqlBulkCopyTableLockParam = null
        ) where T : class
        {
            var results = await new SqlBulkHelper<T>(sqlTransaction)
                .BulkInsertOrUpdateAsync(entities,
                    sqlTransaction,
                    tableNameParam: tableName,
                    matchQualifierExpression: matchQualifierExpressionParam,
                    enableSqlBulkCopyTableLockParam: enableSqlBulkCopyTableLockParam
                )
                .ConfigureAwait(false);

            return results;
        }

        public static IEnumerable<T> BulkInsert<T>(
            this SqlTransaction sqlTransaction,
            IEnumerable<T> entities,
            string tableName = null,
            SqlMergeMatchQualifierExpression matchQualifierExpressionParam = null,
            bool? enableSqlBulkCopyTableLockParam = null
        ) where T : class
        {
            var results = new SqlBulkHelper<T>(sqlTransaction).BulkInsert(entities,
                sqlTransaction,
                tableNameParam: tableName,
                matchQualifierExpression: matchQualifierExpressionParam,
                enableSqlBulkCopyTableLockParam: enableSqlBulkCopyTableLockParam
            );
            return results;
        }

        public static IEnumerable<T> BulkUpdate<T>(
            this SqlTransaction sqlTransaction,
            IEnumerable<T> entities,
            string tableName = null,
            SqlMergeMatchQualifierExpression matchQualifierExpressionParam = null,
            bool? enableSqlBulkCopyTableLockParam = null
        ) where T : class
        {
            var results = new SqlBulkHelper<T>(sqlTransaction).BulkUpdate(entities,
                sqlTransaction,
                tableNameParam: tableName,
                matchQualifierExpression: matchQualifierExpressionParam,
                enableSqlBulkCopyTableLockParam: enableSqlBulkCopyTableLockParam
            );
            return results;
        }

        public static IEnumerable<T> BulkInsertOrUpdate<T>(
            this SqlTransaction sqlTransaction,
            IEnumerable<T> entities,
            string tableName = null,
            SqlMergeMatchQualifierExpression matchQualifierExpressionParam = null,
            bool? enableSqlBulkCopyTableLockParam = null
        ) where T : class
        {
            var results = new SqlBulkHelper<T>(sqlTransaction).BulkInsertOrUpdate(entities,
                sqlTransaction,
                tableNameParam: tableName,
                matchQualifierExpression: matchQualifierExpressionParam,
                enableSqlBulkCopyTableLockParam: enableSqlBulkCopyTableLockParam
            );
            return results;
        }

        #endregion
    }
}
