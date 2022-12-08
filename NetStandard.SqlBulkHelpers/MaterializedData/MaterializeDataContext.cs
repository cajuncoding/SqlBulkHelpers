using System;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Data.SqlClient;

namespace SqlBulkHelpers.MaterializedData
{
    public class MaterializeDataContext : IAsyncDisposable
    {
        protected SqlTransaction SqlTransaction { get; }
        protected ILookup<TableNameTerm, MaterializationTableInfo> TableLookup { get; }
        protected ISqlBulkHelpersConfig BulkHelpersConfig { get; }
        protected bool IsDisposed { get; set; } = false;

        public MaterializationTableInfo[] Tables { get; }

        public MaterializationTableInfo this[int index] => Tables[index];

        public MaterializationTableInfo this[string tableName] => TableLookup[TableNameTerm.From(tableName)].FirstOrDefault();

        /// <summary>
        /// Allows disabling of data validation during materialization, but may put data integrity at risk.
        /// This will improve performance for large data loads, but if disabled then the implementor is responsible
        /// for ensuring all data integrity of the data populated into the tables!
        /// NOTE: The downside of this is that larger tables will take longer to Switch over but Data Integrity is maintained therefore this
        ///         is the default and normal behavior that should be used.
        /// NOTE: In addition, Disabling this poses other implications in SQL Server as the Constraints then become Untrusted which affects
        ///         the Query Optimizer and may may adversely impact Query performance.
        /// </summary>
        public bool EnableDataConstraintChecksOnCompletion { get; set; } = true;

        public MaterializeDataContext(SqlTransaction sqlTransaction, MaterializationTableInfo[] materializationTables, ISqlBulkHelpersConfig bulkHelpersConfig)
        {
            SqlTransaction = sqlTransaction.AssertArgumentIsNotNull(nameof(sqlTransaction));
            Tables = materializationTables.AssertArgumentIsNotNull(nameof(materializationTables));
            BulkHelpersConfig = bulkHelpersConfig.AssertArgumentIsNotNull(nameof(bulkHelpersConfig));
            TableLookup = Tables.ToLookup(t => t.LiveTable);
        }

        public async Task FinishMaterializationProcessAsync()
        {
            var materializationTables = this.Tables;
            var switchScriptBuilder = MaterializedDataScriptBuilder.NewSqlScript();

            //NOTE: We update ALL tables at each step together so that any/all constraints, relationships, etc.
            //      are valid based on newly materialized data populated in the respective loading tables (for each original table)!
            //1) First disable all referencing FKey constraints that will prevent us from being able to switch -- this is still safe within our Transaction!
            foreach (var materializationTableInfo in materializationTables)
                switchScriptBuilder.DisableReferencingForeignKeyChecks(materializationTableInfo.LiveTableDefinition.ReferencingForeignKeyConstraints.AsArray());

            //2) Then we can switch all existing Live tables to the Discarding Schema -- this Frees the Live table up to be updated in the next step!
            foreach (var materializationTableInfo in materializationTables)
                switchScriptBuilder.SwitchTables(materializationTableInfo.LiveTable, materializationTableInfo.DiscardingTable);

            //3) Now we are able to switch all existing Loading tables to Live (which were freed up above) -- this will update the Live Data!
            foreach (var materializationTableInfo in materializationTables)
                switchScriptBuilder.SwitchTables(materializationTableInfo.LoadingTable, materializationTableInfo.LiveTable);

            //4) Third we re-enable all Foreign Key Checks that were disabled!
            //5) Finally we explicitly clean up all Loading/Discarding Tables (contains old data) to free resources -- this leaves us with only the (new) Live Table in place!
            foreach (var materializationTableInfo in materializationTables)
            {
                var originalTableDef = materializationTableInfo.LiveTableDefinition;
                switchScriptBuilder
                    .EnableReferencingForeignKeyChecks(this.EnableDataConstraintChecksOnCompletion, originalTableDef.ReferencingForeignKeyConstraints.AsArray())
                    .DropTable(materializationTableInfo.LoadingTable)
                    .DropTable(materializationTableInfo.DiscardingTable);
            }

            await SqlTransaction.ExecuteMaterializedDataSqlScriptAsync(
                switchScriptBuilder,
                BulkHelpersConfig.MaterializeDataStructureProcessingTimeoutSeconds
            ).ConfigureAwait(false);
        }

        public ValueTask DisposeAsync()
        {
            if (!IsDisposed)
            {
                IsDisposed = true;
            }

            return new ValueTask();
        }
    }
}
