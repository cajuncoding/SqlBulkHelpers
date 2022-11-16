using System;

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
    }
}
