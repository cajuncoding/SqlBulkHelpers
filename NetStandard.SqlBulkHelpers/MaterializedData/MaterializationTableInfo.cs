using System;
using SqlBulkHelpers.CustomExtensions;

namespace SqlBulkHelpers.MaterializedData
{
    public class MaterializationTableInfo
    {
        internal string OriginalTableName { get; }

        public TableNameTerm LiveTable { get; }

        public SqlBulkHelpersTableDefinition LiveTableDefinition { get; }

        public TableNameTerm LoadingTable { get; }

        public TableNameTerm DiscardingTable { get; }

        public MaterializationTableInfo(string originalTableName, SqlBulkHelpersTableDefinition originalTableDef, TableNameTerm loadingTableTerm, TableNameTerm discardingTableNameTerm)
        {
            OriginalTableName = originalTableName;
            LiveTableDefinition = originalTableDef;
            LiveTable = originalTableDef.TableNameTerm;
            LoadingTable = loadingTableTerm.AssertArgumentIsNotNull(nameof(loadingTableTerm));
            DiscardingTable = discardingTableNameTerm.AssertArgumentIsNotNull(nameof(discardingTableNameTerm));
        }
    }
}
