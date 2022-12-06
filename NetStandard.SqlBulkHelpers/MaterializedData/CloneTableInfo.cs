using System;

namespace SqlBulkHelpers.MaterializedData
{
    public readonly struct CloneTableInfo
    {
        public TableNameTerm SourceTable { get; }
        public TableNameTerm TargetTable { get; }

        public CloneTableInfo(TableNameTerm sourceTable, TableNameTerm? targetTable = null)
        {
            sourceTable.AssertArgumentIsNotNull(nameof(sourceTable));

            //If both Source & Target are the same (e.g. Target was not explicitly specified) then we adjust
            //  the Target to ensure we create a copy and append a unique Copy Id...
            var validTargetTable = targetTable == null || targetTable.Value.FullyQualifiedTableName.Equals(sourceTable.FullyQualifiedTableName, StringComparison.OrdinalIgnoreCase)
                ? TableNameTerm.From(sourceTable.SchemaName, $"{sourceTable.TableName}_Copy_{IdGenerator.NewId(10)}")
                : targetTable.Value;

            SourceTable = sourceTable;
            TargetTable = validTargetTable;
        }

        public static CloneTableInfo From<TSource, TTarget>(string sourceTableName = null, string targetTableName = null)
        {
            //If the generic type is ISkipMappingLookup then we have a valid sourceTableName specified...
            if (SqlBulkHelpersProcessingDefinition.SkipMappingLookupType.IsAssignableFrom(typeof(TSource)))
                sourceTableName.AssertArgumentIsNotNullOrWhiteSpace(nameof(sourceTableName));

            var sourceTable = TableNameTerm.From<TSource>(sourceTableName);

            //For Target Table name we support falling back to original Source Table Name to automatically create a clone
            //  with unique 'Copy' name...
            var validTargetTableName = string.IsNullOrWhiteSpace(targetTableName)
                ? sourceTableName
                : targetTableName;

            //If the generic type is ISkipMappingLookup then we have a valid sourceTableName specified...
            if (SqlBulkHelpersProcessingDefinition.SkipMappingLookupType.IsAssignableFrom(typeof(TTarget)))
            {
                //We validate the valid target table name but if it's still blank then we throw an Argument
                //  exception because no 'targetTableName' could be resolved...
                validTargetTableName.AssertArgumentIsNotNullOrWhiteSpace(nameof(targetTableName));
            }

            var targetTable = TableNameTerm.From<TTarget>(targetTableName ?? sourceTableName);
            return new CloneTableInfo(sourceTable, targetTable);
        }

        public static CloneTableInfo From(string sourceTableName, string targetTableName)
        {
            var sourceTable = TableNameTerm.From<ISkipMappingLookup>(sourceTableName.AssertArgumentIsNotNullOrWhiteSpace(nameof(sourceTableName)));
            var targetTable = TableNameTerm.From<ISkipMappingLookup>(targetTableName.AssertArgumentIsNotNullOrWhiteSpace(nameof(targetTableName)));
            return new CloneTableInfo(sourceTable, targetTable);
        }

        public static CloneTableInfo ForNewSchema(TableNameTerm sourceTable, string targetSchemaName)
            => new CloneTableInfo(sourceTable, sourceTable.SwitchSchema(targetSchemaName));
    }
}
