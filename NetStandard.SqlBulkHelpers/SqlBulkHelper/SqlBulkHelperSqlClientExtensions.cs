using Microsoft.Data.SqlClient;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace SqlBulkHelpers
{
    public static class SqlBulkHelperSqlClientExtensions
    {
        #region ISqlBulkHelper<T> implementations

        public static async Task<IEnumerable<T>> BulkInsertAsync<T>(
            this SqlTransaction sqlTransaction,
            IEnumerable<T> entities,
            string tableName = null,
            SqlMergeMatchQualifierExpression matchQualifierExpressionParam = null,
            ISqlBulkHelpersConfig bulkHelpersConfig = null
        ) where T : class
        {

            var results = await new SqlBulkHelper<T>(sqlTransaction, bulkHelpersConfig)
                .BulkInsertAsync(
                    entities, 
                    sqlTransaction, 
                    tableNameParam: tableName, 
                    matchQualifierExpression: matchQualifierExpressionParam
                )
                .ConfigureAwait(false);

            return results;
        }

        public static async Task<IEnumerable<T>> BulkUpdateAsync<T>(
            this SqlTransaction sqlTransaction,
            IEnumerable<T> entities,
            string tableName = null,
            SqlMergeMatchQualifierExpression matchQualifierExpressionParam = null,
            ISqlBulkHelpersConfig bulkHelpersConfig = null
        ) where T : class
        {
            var results = await new SqlBulkHelper<T>(sqlTransaction, bulkHelpersConfig)
                .BulkUpdateAsync(entities,
                    sqlTransaction,
                    tableNameParam: tableName,
                    matchQualifierExpression: matchQualifierExpressionParam
                )
                .ConfigureAwait(false);

            return results;
        }

        public static async Task<IEnumerable<T>> BulkInsertOrUpdateAsync<T>(
            this SqlTransaction sqlTransaction,
            IEnumerable<T> entities,
            string tableName = null,
            SqlMergeMatchQualifierExpression matchQualifierExpressionParam = null,
            ISqlBulkHelpersConfig bulkHelpersConfig = null
        ) where T : class
        {
            var results = await new SqlBulkHelper<T>(sqlTransaction, bulkHelpersConfig)
                .BulkInsertOrUpdateAsync(entities,
                    sqlTransaction,
                    tableNameParam: tableName,
                    matchQualifierExpression: matchQualifierExpressionParam
                )
                .ConfigureAwait(false);

            return results;
        }

        public static IEnumerable<T> BulkInsert<T>(
            this SqlTransaction sqlTransaction,
            IEnumerable<T> entities,
            string tableName = null,
            SqlMergeMatchQualifierExpression matchQualifierExpressionParam = null,
            ISqlBulkHelpersConfig bulkHelpersConfig = null
        ) where T : class
        {
            var results = new SqlBulkHelper<T>(sqlTransaction, bulkHelpersConfig)
                .BulkInsert(
                    entities,
                    sqlTransaction,
                    tableNameParam: tableName,
                    matchQualifierExpression: matchQualifierExpressionParam
                );

            return results;
        }

        public static IEnumerable<T> BulkUpdate<T>(
            this SqlTransaction sqlTransaction,
            IEnumerable<T> entities,
            string tableName = null,
            SqlMergeMatchQualifierExpression matchQualifierExpressionParam = null,
            ISqlBulkHelpersConfig bulkHelpersConfig = null
        ) where T : class
        {
            var results = new SqlBulkHelper<T>(sqlTransaction, bulkHelpersConfig)
                .BulkUpdate(
                    entities,
                    sqlTransaction,
                    tableNameParam: tableName,
                    matchQualifierExpression: matchQualifierExpressionParam
                );
    
            return results;
        }

        public static IEnumerable<T> BulkInsertOrUpdate<T>(
            this SqlTransaction sqlTransaction,
            IEnumerable<T> entities,
            string tableName = null,
            SqlMergeMatchQualifierExpression matchQualifierExpressionParam = null,
            ISqlBulkHelpersConfig bulkHelpersConfig = null
        ) where T : class
        {
            var results = new SqlBulkHelper<T>(sqlTransaction, bulkHelpersConfig)
                .BulkInsertOrUpdate(
                    entities,
                    sqlTransaction,
                    tableNameParam: tableName,
                    matchQualifierExpression: matchQualifierExpressionParam
                );

            return results;
        }

        #endregion

        #region Other Convenience Methods

        public static SqlBulkHelpersTableDefinition GetTableSchemaDefinition(this SqlConnection sqlConn, string tableName)
            => GetTableSchemaDefinition<ISkipMappingLookup>(sqlConn, tableName);

        public static SqlBulkHelpersTableDefinition GetTableSchemaDefinition<T>(
            this SqlConnection sqlConn,
            string tableName = null
        ) where T: class
        {
            using (var sqlTransaction = sqlConn.BeginTransaction())
            {
                return GetTableSchemaDefinition<T>(sqlTransaction, tableName);
            }
        }

        public static SqlBulkHelpersTableDefinition GetTableSchemaDefinition(this SqlTransaction sqlTransaction, string tableName)
            => GetTableSchemaDefinition<ISkipMappingLookup>(sqlTransaction, tableName);

        public static SqlBulkHelpersTableDefinition GetTableSchemaDefinition<T>(
            this SqlTransaction sqlTransaction,
            string tableName = null
        ) where T : class
        {
            var sqlBulkHelper = new SqlBulkHelper<T>(sqlTransaction);
            var tableDefinition = sqlBulkHelper.GetTableSchemaDefinition(sqlTransaction, tableName);
            return tableDefinition;
        }

        #endregion

    }
}
