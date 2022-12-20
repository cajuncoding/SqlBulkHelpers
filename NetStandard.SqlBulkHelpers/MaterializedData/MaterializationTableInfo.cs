using System;

namespace SqlBulkHelpers.MaterializedData
{
    public class MaterializationTableInfo
    {
        public TableNameTerm LiveTable { get; }

        public SqlBulkHelpersTableDefinition LiveTableDefinition { get; }

        public TableNameTerm LoadingTable { get; }

        public TableNameTerm DiscardingTable { get; }

        public MaterializationTableInfo(SqlBulkHelpersTableDefinition originalTableDef, TableNameTerm loadingTableTerm, TableNameTerm discardingTableNameTerm)
        {
            LiveTableDefinition = originalTableDef;
            LiveTable = originalTableDef.TableNameTerm;
            LoadingTable = loadingTableTerm.AssertArgumentIsNotNull(nameof(loadingTableTerm));
            DiscardingTable = discardingTableNameTerm.AssertArgumentIsNotNull(nameof(discardingTableNameTerm));
        }
    }
}
