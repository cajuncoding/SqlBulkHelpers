using Microsoft.Data.SqlClient;
using System;
using System.Threading.Tasks;

namespace SqlBulkHelpers
{
    public interface ISqlBulkHelpersDBSchemaLoader
    {
        Task<SqlBulkHelpersTableDefinition> GetTableSchemaDefinitionAsync(
            string tableName,
            TableSchemaDetailLevel detailLevel,
            SqlConnection sqlConnection,
            SqlTransaction sqlTransaction = null,
            bool forceCacheReload = false
        );

        Task<SqlBulkHelpersTableDefinition> GetTableSchemaDefinitionAsync(
            string tableName,
            TableSchemaDetailLevel detailLevel,
            Func<Task<SqlConnection>> sqlConnectionAsyncFactory,
            bool forceCacheReload = false
        );

        SqlBulkHelpersTableDefinition GetTableSchemaDefinition(
            string tableName,
            TableSchemaDetailLevel detailLevel,
            SqlConnection sqlConnection,
            SqlTransaction sqlTransaction = null,
            bool forceCacheReload = false
        );

        SqlBulkHelpersTableDefinition GetTableSchemaDefinition(
            string tableName,
            TableSchemaDetailLevel detailLevel,
            Func<SqlConnection> sqlConnectionFactory,
            bool forceCacheReload = false
        );

        ValueTask ClearCacheAsync();

        void ClearCache();
    }
}