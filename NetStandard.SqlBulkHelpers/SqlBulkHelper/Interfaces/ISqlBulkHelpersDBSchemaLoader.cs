using Microsoft.Data.SqlClient;
using System;
using System.Linq;

namespace SqlBulkHelpers
{
    public interface ISqlBulkHelpersDBSchemaLoader
    {
        SqlBulkHelpersTableDefinition GetTableSchemaDefinition(string tableName, ISqlBulkHelpersConnectionProvider sqlConnectionProvider);
        SqlBulkHelpersTableDefinition GetTableSchemaDefinition(string tableName, Func<SqlConnection> sqlConnectionFactory);
        SqlBulkHelpersTableDefinition GetTableSchemaDefinition(string tableName, SqlTransaction sqlTransaction);

        void Reload();
    }
}