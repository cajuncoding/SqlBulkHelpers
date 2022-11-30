using System;

namespace SqlBulkHelpers.MaterializedData
{
    public struct MaterializationTableInfo
    {
        public TableNameTerm OriginalTable { get; }

        public TableNameTerm LoadingTable { get; }

        public TableNameTerm TempHoldingTable { get; }

        public MaterializationTableInfo(TableNameTerm originalTableTerm, TableNameTerm loadingTableTerm, TableNameTerm tempHoldingTableTerm)
        {
            OriginalTable = originalTableTerm.AssertArgumentIsNotNull(nameof(originalTableTerm));
            LoadingTable = loadingTableTerm.AssertArgumentIsNotNull(nameof(loadingTableTerm));
            TempHoldingTable = tempHoldingTableTerm.AssertArgumentIsNotNull(nameof(tempHoldingTableTerm));
        }
    }
}
