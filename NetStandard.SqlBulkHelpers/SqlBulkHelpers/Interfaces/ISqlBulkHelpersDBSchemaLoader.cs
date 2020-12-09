using System;
using System.Linq;

namespace SqlBulkHelpers
{
    public interface ISqlBulkHelpersDBSchemaLoader
    {
        ILookup<string, SqlBulkHelpersTableDefinition> InitializeSchemaDefinitions();

        SqlBulkHelpersTableDefinition GetTableSchemaDefinition(string tableName);
    }
}