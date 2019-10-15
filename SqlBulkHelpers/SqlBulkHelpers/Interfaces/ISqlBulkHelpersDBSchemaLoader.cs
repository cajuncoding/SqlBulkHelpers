using System;

namespace SqlBulkHelpers
{
    public interface ISqlBulkHelpersDBSchemaLoader
    {
        SqlBulkHelpersTableDefinition GetTableSchemaDefinition(String tableName);
    }
}