using System;

namespace SqlBulkHelpers
{
    public readonly struct TableNameTerm
    {
        public const string DefaultSchemaName = "dbo";
        public const char TermSeparator = '.';

        public TableNameTerm(string schemaName, string tableName)
        {
            SchemaName = schemaName.AssertArgumentIsNotNullOrWhiteSpace(nameof(schemaName));
            TableName = tableName.AssertArgumentIsNotNullOrWhiteSpace(nameof(tableName));
            FullyQualifiedTableName = $"[{SchemaName}].[{TableName}]";
        }

        public string SchemaName { get; }
        public string TableName { get; }
        public string FullyQualifiedTableName { get; }
        public override string ToString() => FullyQualifiedTableName;

        public static TableNameTerm From(string tableNameOverride)
            => From<ISkipMappingLookup>(tableNameOverride);

        public static TableNameTerm From<T>(string tableNameOverride = null)
        {
            TableNameTerm tableNameTerm;
            if (tableNameOverride != null)
            {
                tableNameTerm = tableNameOverride.ParseAsTableNameTerm();
            }
            else
            {
                var processingDef = SqlBulkHelpersProcessingDefinition.GetProcessingDefinition<T>();
                tableNameTerm = processingDef.MappedDbTableName.ParseAsTableNameTerm();
            }

            return tableNameTerm;
        }
    }
}
