using System;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Data.SqlClient;

namespace SqlBulkHelpers.MaterializedData
{
    public struct MaterializeDataContext : IAsyncDisposable
    {
        private readonly ILookup<TableNameTerm, MaterializationTableInfo> _tableLookup;
        private readonly SqlTransaction _sqlTrans;
        private readonly ISqlBulkHelpersConfig _bulkHelpersConfig;

        public MaterializationTableInfo[] Tables { get; }

        public MaterializationTableInfo this[int index] => Tables[index];

        public MaterializationTableInfo this[string tableName] => _tableLookup[TableNameTerm.From(tableName)].FirstOrDefault();

        /// <summary>
        /// Allows disabling of data validation during materialization, but may put data integrity at risk.
        /// This will improve performance for large data loads, but if disabled then the implementor is responsible
        /// for ensuring all data integrity of the data populated into the tables!
        /// </summary>
        public bool EnableDataConstraintChecksOnCompletion { get; set; }

        public MaterializeDataContext(SqlTransaction sqlTransaction, MaterializationTableInfo[] materializationTables, ISqlBulkHelpersConfig bulkHelpersConfig)
        {
            _sqlTrans = sqlTransaction.AssertArgumentIsNotNull(nameof(sqlTransaction));
            Tables = materializationTables.AssertArgumentIsNotNull(nameof(materializationTables));
            _bulkHelpersConfig = bulkHelpersConfig.AssertArgumentIsNotNull(nameof(bulkHelpersConfig));

            _isDisposed = false;
            EnableDataConstraintChecksOnCompletion = true;
            _tableLookup = Tables.ToLookup(t => t.LiveTable);
        }

        public async Task FinishMaterializationProcessAsync()
        {
            var materializationTables = this.Tables;
            var switchScriptBuilder = MaterializedDataScriptBuilder.NewSqlScript();

            //NOTE: We update ALL tables at each step together so that any/all constraints, relationships, etc.
            //      are valid based on newly materialized data populated in the respective loading tables (for each original table)!
            //1) First disable all referencing FKey constraints that will prevent us from being able to switch -- this is still safe within our Transaction!
            //2) Then we can switch all existing Live tables to Temp/Holding -- this Frees the Live table up to be updated in the next step!
            foreach (var materializationTableInfo in materializationTables)
            {
                var originalTableDef = materializationTableInfo.LiveTableDefinition;
                switchScriptBuilder.DisableReferencingForeignKeyChecks(originalTableDef.ReferencingForeignKeyConstraints.AsArray());
                switchScriptBuilder.SwitchTables(materializationTableInfo.LiveTable, materializationTableInfo.TempHoldingTable);
            }

            //3) Now we are able to switch all existing Loading/Staging tables to Live (which were freed up above) -- this will update the Live Data!
            foreach (var materializationTableInfo in materializationTables)
            {
                switchScriptBuilder.SwitchTables(materializationTableInfo.LoadingTable, materializationTableInfo.LiveTable);
            }

            //4) Third we re-enable all Foreign Key Checks that were disabled!
            //5) Finally we explicitly clean up all Temp/Holding/Loading Tables (contains old data) to free resources -- this leaves us with only the (new) Live Table in place!
            foreach (var materializationTableInfo in materializationTables)
            {
                var originalTableDef = materializationTableInfo.LiveTableDefinition;
                switchScriptBuilder.EnableReferencingForeignKeyChecks(this.EnableDataConstraintChecksOnCompletion, originalTableDef.ReferencingForeignKeyConstraints.AsArray());
                switchScriptBuilder.DropTable(materializationTableInfo.LoadingTable);
                switchScriptBuilder.DropTable(materializationTableInfo.TempHoldingTable);
            }

            await _sqlTrans.ExecuteMaterializedDataSqlScriptAsync(
                switchScriptBuilder,
                _bulkHelpersConfig.MaterializeDataStructureProcessingTimeoutSeconds
            ).ConfigureAwait(false);
        }

        private bool _isDisposed;
        public async ValueTask DisposeAsync()
        {
            if (!_isDisposed)
            {
                _isDisposed = true;
            }
        }
    }
}
