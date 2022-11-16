using System;
using System.Linq;

namespace SqlBulkHelpers
{
    public interface ISqlBulkHelpersDBSchemaLoader
    {
        ILookup<string, SqlBulkHelpersTableDefinition> GetTableSchemaDefinitionsLowercaseLookupFromLazyCache();

        SqlBulkHelpersTableDefinition GetTableSchemaDefinition(string tableName);

        void Reload();
    }
}