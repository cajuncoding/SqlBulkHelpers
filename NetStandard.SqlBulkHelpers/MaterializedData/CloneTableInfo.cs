using System;
using System.Security.Cryptography;
using Microsoft.Identity.Client;

namespace SqlBulkHelpers.MaterializedData
{
    public struct CloneTableInfo
    {
        public TableNameTerm SourceTable { get; }
        public TableNameTerm TargetTable { get; }

        public CloneTableInfo(TableNameTerm sourceTable, TableNameTerm targetTable)
        {
            SourceTable = sourceTable.AssertArgumentIsNotNull(nameof(sourceTable));
            TargetTable = targetTable.AssertArgumentIsNotNull(nameof(targetTable));
        }

        public static CloneTableInfo From<TSource, TTarget>(string sourceTableName, string targetTableName)
        {
            var sourceTable = TableNameTerm.From<TSource>(sourceTableName);
            var targetTable = TableNameTerm.From<TTarget>(targetTableName);
            return new CloneTableInfo(sourceTable, targetTable);
        }

        public static CloneTableInfo From(string sourceTableName, string targetTableName)
        {
            var sourceTable = TableNameTerm.From<ISkipMappingLookup>(sourceTableName);
            var targetTable = TableNameTerm.From<ISkipMappingLookup>(targetTableName);
            return new CloneTableInfo(sourceTable, targetTable);
        }

        public static CloneTableInfo ForNewSchema(TableNameTerm sourceTable, string targetSchemaName)
            => new CloneTableInfo(sourceTable, sourceTable.SwitchSchema(targetSchemaName));
    }
}
