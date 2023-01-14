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

        ValueTask ClearCacheAsync();

        SqlBulkHelpersTableDefinition GetTableSchemaDefinition(
            string tableName, 
            TableSchemaDetailLevel detailLevel,
            SqlConnection sqlConnection,
            SqlTransaction sqlTransaction = null,
            bool forceCacheReload = false
        );

        void ClearCache();
    }
}