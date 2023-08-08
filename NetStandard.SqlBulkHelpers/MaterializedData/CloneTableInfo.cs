using System;
using SqlBulkHelpers.CustomExtensions;

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
            var validTargetTable = targetTable == null || targetTable.Value.EqualsIgnoreCase(sourceTable)
                ? MakeTableNameUniqueInternal(sourceTable)
                : targetTable.Value;

            SourceTable = sourceTable;
            TargetTable = validTargetTable;
        }

        /// <summary>
        /// Ensures that the Target Table Name is truly unique (and not simply scoped to a different Schema.
        /// </summary>
        /// <returns></returns>
        public CloneTableInfo MakeTargetTableNameUnique()
            => new CloneTableInfo(SourceTable, MakeTableNameUniqueInternal(TargetTable));

        private static TableNameTerm MakeTableNameUniqueInternal(TableNameTerm tableNameTerm)
            => TableNameTerm.From(tableNameTerm.SchemaName, string.Concat(tableNameTerm.TableName, "_", IdGenerator.NewId(10)));

        public static CloneTableInfo From<TSource, TTarget>(string sourceTableName = null, string targetTableName = null, string targetPrefix = null, string targetSuffix = null)
        {
            //If the generic type is ISkipMappingLookup then we must have a valid sourceTableName specified as a param...
            if (SqlBulkHelpersProcessingDefinition.SkipMappingLookupType.IsAssignableFrom(typeof(TSource)))
                sourceTableName.AssertArgumentIsNotNullOrWhiteSpace(nameof(sourceTableName));

            var sourceTable = TableNameTerm.From<TSource>(sourceTableName);

            //For Target Table name we support falling back to original Source Table Name to automatically create a clone
            //  with unique 'Copy' name...
            var validTargetTableName = string.IsNullOrWhiteSpace(targetTableName)
                ? sourceTableName
                : targetTableName;

            //If the generic type is ISkipMappingLookup then we must have a valid validTargetTableName specified as a param...
            if (SqlBulkHelpersProcessingDefinition.SkipMappingLookupType.IsAssignableFrom(typeof(TTarget)))
            {
                //We validate the valid target table name but if it's still blank then we throw an Argument
                //  exception because no 'targetTableName' could be resolved...
                validTargetTableName.AssertArgumentIsNotNullOrWhiteSpace(nameof(targetTableName));
            }

            var targetTable = TableNameTerm.From<TTarget>(targetTableName ?? sourceTableName).ApplyNamePrefixOrSuffix(targetPrefix, targetSuffix);
            return new CloneTableInfo(sourceTable, targetTable);
        }

        public static CloneTableInfo From(string sourceTableName, string targetTableName = null, string targetPrefix = null, string targetSuffix = null)
            => From<ISkipMappingLookup, ISkipMappingLookup>(sourceTableName, targetTableName, targetPrefix, targetSuffix);

        public static CloneTableInfo ForNewSchema(TableNameTerm sourceTable, string targetSchemaName, string targetTablePrefix = null, string targetTableSuffix = null)
            => new CloneTableInfo(sourceTable, sourceTable.SwitchSchema(targetSchemaName).ApplyNamePrefixOrSuffix(targetTablePrefix, targetTableSuffix));
    }
}
