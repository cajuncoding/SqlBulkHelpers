using System;

namespace SqlBulkHelpers.MaterializedData
{
    public struct MaterializationTableInfo
    {
        public TableNameTerm LiveTable { get; }

        public SqlBulkHelpersTableDefinition LiveTableDefinition { get; }

        public TableNameTerm LoadingTable { get; }

        public TableNameTerm TempHoldingTable { get; }

        public MaterializationTableInfo(SqlBulkHelpersTableDefinition originalTableDef, TableNameTerm loadingTableTerm, TableNameTerm tempHoldingTableTerm)
        {
            LiveTableDefinition = originalTableDef;
            LiveTable = originalTableDef.TableNameTerm;
            LoadingTable = loadingTableTerm.AssertArgumentIsNotNull(nameof(loadingTableTerm));
            TempHoldingTable = tempHoldingTableTerm.AssertArgumentIsNotNull(nameof(tempHoldingTableTerm));
        }
    }
}
