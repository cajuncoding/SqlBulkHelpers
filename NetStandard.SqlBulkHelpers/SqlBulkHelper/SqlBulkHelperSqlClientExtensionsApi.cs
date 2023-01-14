using Microsoft.Data.SqlClient;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace SqlBulkHelpers
{
    public static class SqlBulkHelperSqlClientExtensionsApi
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

            var results = await new SqlBulkHelper<T>(bulkHelpersConfig)
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
            var results = await new SqlBulkHelper<T>(bulkHelpersConfig)
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
            var results = await new SqlBulkHelper<T>(bulkHelpersConfig)
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
            var results = new SqlBulkHelper<T>(bulkHelpersConfig)
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
            var results = new SqlBulkHelper<T>(bulkHelpersConfig)
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
            var results = new SqlBulkHelper<T>(bulkHelpersConfig)
                .BulkInsertOrUpdate(
                    entities,
                    sqlTransaction,
                    tableNameParam: tableName,
                    matchQualifierExpression: matchQualifierExpressionParam
                );

            return results;
        }

        #endregion

        #region Table Schema Definition Methods

        public static Task<SqlBulkHelpersTableDefinition> GetTableSchemaDefinitionAsync(
            this SqlTransaction sqlTransaction, 
            string tableName, 
            TableSchemaDetailLevel detailLevel = TableSchemaDetailLevel.ExtendedDetails, 
            bool forceCacheReload = false
        ) => GetTableSchemaDefinitionAsync<ISkipMappingLookup>(sqlTransaction, tableName, detailLevel, forceCacheReload);

        public static Task<SqlBulkHelpersTableDefinition> GetTableSchemaDefinitionAsync<T>(
            this SqlTransaction sqlTransaction, 
            string tableNameOverride = null,
            TableSchemaDetailLevel detailLevel = TableSchemaDetailLevel.ExtendedDetails,
            bool forceCacheReload = false
        ) where T : class
        {
            sqlTransaction.AssertArgumentIsNotNull(nameof(sqlTransaction));
            return GetTableSchemaDefinitionInternalAsync<T>(sqlTransaction.Connection, tableNameOverride, detailLevel, sqlTransaction, forceCacheReload);
        }

        public static Task<SqlBulkHelpersTableDefinition> GetTableSchemaDefinitionAsync(
            this SqlConnection sqlConnection, 
            string tableName,
            TableSchemaDetailLevel detailLevel = TableSchemaDetailLevel.ExtendedDetails,
            bool forceCacheReload = false
        ) => GetTableSchemaDefinitionAsync<ISkipMappingLookup>(sqlConnection, tableName, detailLevel, forceCacheReload);

        public static Task<SqlBulkHelpersTableDefinition> GetTableSchemaDefinitionAsync<T>(
            this SqlConnection sqlConnection,
            string tableNameOverride = null,
            TableSchemaDetailLevel detailLevel = TableSchemaDetailLevel.ExtendedDetails,
            bool forceCacheReload = false
        ) where T : class
        {
            sqlConnection.AssertArgumentIsNotNull(nameof(sqlConnection));
            return GetTableSchemaDefinitionInternalAsync<T>(sqlConnection, tableNameOverride, detailLevel, forceCacheReload: forceCacheReload);
        }

        private static async Task<SqlBulkHelpersTableDefinition> GetTableSchemaDefinitionInternalAsync<T>(
            this SqlConnection sqlConnection, 
            string tableNameOverride = null,
            TableSchemaDetailLevel detailLevel = TableSchemaDetailLevel.ExtendedDetails,
            SqlTransaction sqlTransaction = null, 
            bool forceCacheReload = false
        ) where T : class
        {
            var sqlBulkHelper = new SqlBulkHelper<T>();
            var tableDefinition = await sqlBulkHelper
                .GetTableSchemaDefinitionAsync(sqlConnection, sqlTransaction, tableNameOverride, detailLevel, forceCacheReload)
                .ConfigureAwait(false);

            return tableDefinition;
        }

        public static SqlBulkHelpersTableDefinition GetTableSchemaDefinition(
            this SqlTransaction sqlTransaction,
            string tableName,
            TableSchemaDetailLevel detailLevel = TableSchemaDetailLevel.ExtendedDetails,
            bool forceCacheReload = false
        ) => GetTableSchemaDefinition<ISkipMappingLookup>(sqlTransaction, tableName, detailLevel, forceCacheReload);

        public static SqlBulkHelpersTableDefinition GetTableSchemaDefinition<T>(
            this SqlTransaction sqlTransaction,
            string tableNameOverride = null,
            TableSchemaDetailLevel detailLevel = TableSchemaDetailLevel.ExtendedDetails,
            bool forceCacheReload = false
        ) where T : class
        {
            sqlTransaction.AssertArgumentIsNotNull(nameof(sqlTransaction));
            return GetTableSchemaDefinitionInternal<T>(sqlTransaction.Connection, tableNameOverride, detailLevel, sqlTransaction, forceCacheReload);
        }

        public static SqlBulkHelpersTableDefinition GetTableSchemaDefinition(
            this SqlConnection sqlConnection,
            string tableName,
            TableSchemaDetailLevel detailLevel = TableSchemaDetailLevel.ExtendedDetails,
            bool forceCacheReload = false
        ) => GetTableSchemaDefinition<ISkipMappingLookup>(sqlConnection, tableName, detailLevel, forceCacheReload);

        public static SqlBulkHelpersTableDefinition GetTableSchemaDefinition<T>(
            this SqlConnection sqlConnection,
            string tableNameOverride = null,
            TableSchemaDetailLevel detailLevel = TableSchemaDetailLevel.ExtendedDetails,
            bool forceCacheReload = false
        ) where T : class
        {
            sqlConnection.AssertArgumentIsNotNull(nameof(sqlConnection));
            return GetTableSchemaDefinitionInternal<T>(sqlConnection, tableNameOverride, detailLevel, forceCacheReload: forceCacheReload);
        }

        private static SqlBulkHelpersTableDefinition GetTableSchemaDefinitionInternal<T>(
            this SqlConnection sqlConnection,
            string tableNameOverride = null,
            TableSchemaDetailLevel detailLevel = TableSchemaDetailLevel.ExtendedDetails,
            SqlTransaction sqlTransaction = null,
            bool forceCacheReload = false
        ) where T : class
        {
            var sqlBulkHelper = new SqlBulkHelper<T>();
            var tableDefinition = sqlBulkHelper.GetTableSchemaDefinition(sqlConnection, sqlTransaction, tableNameOverride, detailLevel, forceCacheReload);
            return tableDefinition;
        }

        #endregion

    }
}
