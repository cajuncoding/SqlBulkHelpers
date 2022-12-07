using System;

namespace SqlBulkHelpers
{
    public readonly struct TableNameTerm
    {
        public const string DefaultSchemaName = "dbo";
        public const char TermSeparator = '.';

        public TableNameTerm(string schemaName, string tableName)
        {
            SchemaName = schemaName.AssertArgumentIsNotNullOrWhiteSpace(nameof(schemaName)).TrimTableNameTerm();
            TableName = tableName.AssertArgumentIsNotNullOrWhiteSpace(nameof(tableName)).TrimTableNameTerm();
            //NOTE: We don't use QualifySqlTerm() here to prevent unnecessary additional trimming (that is done above).
            FullyQualifiedTableName = $"[{SchemaName}].[{TableName}]";
        }

        public string SchemaName { get; }
        public string TableName { get; }
        public string FullyQualifiedTableName { get; }

        public override string ToString() => FullyQualifiedTableName;
        public bool Equals(TableNameTerm other) => FullyQualifiedTableName.Equals(other.FullyQualifiedTableName);
        public bool EqualsIgnoreCase(TableNameTerm other) => FullyQualifiedTableName.Equals(other.FullyQualifiedTableName, StringComparison.OrdinalIgnoreCase);
        public TableNameTerm SwitchSchema(string newSchema) => new TableNameTerm(newSchema, TableName);
        
        //Handle Automatic String conversions for simplified APIs...
        public static implicit operator string(TableNameTerm t) => t.ToString();

        public static TableNameTerm From(string schemaName, string tableName)
            => new TableNameTerm(schemaName, tableName);

        public static TableNameTerm From(string tableName)
            => From<ISkipMappingLookup>(tableName);

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
