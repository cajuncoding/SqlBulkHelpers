﻿using System;
using System.Collections.Immutable;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Data.SqlClient;

namespace SqlBulkHelpers.MaterializedData
{
    public class MaterializeDataContext : IAsyncDisposable
    {
        protected SqlTransaction SqlTransaction { get; }
        protected ILookup<string, MaterializationTableInfo> TableLookup { get; }
        protected ISqlBulkHelpersConfig BulkHelpersConfig { get; }
        protected bool IsDisposed { get; set; } = false;

        public MaterializationTableInfo[] Tables { get; }

        public MaterializationTableInfo this[int index] => Tables[index];

        public MaterializationTableInfo this[string fullyQualifiedTableName] => TableLookup[fullyQualifiedTableName].FirstOrDefault();

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
            TableLookup = Tables.ToLookup(t => t.LiveTable.FullyQualifiedTableName);
        }

        public async Task FinishMaterializationProcessAsync()
        {
            var materializationTables = this.Tables;
            var switchScriptBuilder = MaterializedDataScriptBuilder.NewSqlScript();

            var allLiveTableFKeyConstraintLookup = Tables
                .SelectMany(t => t.LiveTableDefinition.ForeignKeyConstraints)
                .ToLookup(fkey => fkey.ToString());

            //NOTE: We update ALL tables at each step together so that any/all constraints, relationships, etc.
            //      are valid based on newly materialized data populated in the respective loading tables (for each Live table)!
            //1) First Add all missing FKey constraints (not added during initial cloning) that will prevent us from being able to switch -- this is still safe within our Transaction!
            //   NOTE: We could not add FKey constraints at initial load because the links will result in Transaction locks on the Live Tables which we must avoid!!!
            foreach (var materializationTableInfo in materializationTables)
            {
                var fkeyConstraints = materializationTableInfo.LiveTableDefinition.ForeignKeyConstraints.AsArray();
                //var inContextFKeyConstraints = fkeyConstraints.Where(fkey => TableLookup.Contains(fkey.ReferenceTableFullyQualifiedName)).AsArray();

                switchScriptBuilder
                    //We must disable FKeys that reference the Live Table we are switching...
                    .DisableReferencingForeignKeyChecks(materializationTableInfo.LiveTableDefinition.ReferencingForeignKeyConstraints.AsArray())
                    //We must add the missing FKeys to the Loading/Discarding tables so that we can now safely switch to/from them!
                    //NOTE: We have to explicitly Disable any FKey that References a Table in context (being switched) newly added FKeys are still enabled,
                    //      and the FKey status must match the currently disabled Live Table FKey status for switching to work; but we should NOT disable
                    //      any FKeys to tables not being switched...
                    //NOTE: We also have to disable the in context FKey constraints for both Loading and Discarding tables.
                    .AddForeignKeyConstraints(materializationTableInfo.LoadingTable, executeConstraintValidation: false, fkeyConstraints)
                    .DisableAllTableConstraintChecks(materializationTableInfo.LoadingTable)
                    //.DisableForeignKeyChecks(materializationTableInfo.LoadingTable, inContextFKeyConstraints)
                    .AddForeignKeyConstraints(materializationTableInfo.DiscardingTable, executeConstraintValidation: false, fkeyConstraints)
                    .DisableAllTableConstraintChecks(materializationTableInfo.DiscardingTable);
                    //.DisableForeignKeyChecks(materializationTableInfo.DiscardingTable, inContextFKeyConstraints);
            }

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
                var liveTableDefinition = materializationTableInfo.LiveTableDefinition;
                var otherReferencingFKeyConstraints = liveTableDefinition.ReferencingForeignKeyConstraints.Where(rc => !allLiveTableFKeyConstraintLookup.Contains(rc.ToString()));

                switchScriptBuilder
                    //HERE We enable all FKey Constraints and Trigger a Re-check to ensure Data Integrity!
                    //NOTE: This is critical because the FKeys were added with NOCHECK status above so that we could safely switch
                    .EnableAllTableConstraintChecks(materializationTableInfo.LiveTable, true)
                    //NOTE: FKeys must be explicitly re-enabled to ensure they are restored to Trusted state; they aren't included in the ALL Constraint Check.
                    .EnableForeignKeyChecks(liveTableDefinition.ForeignKeyConstraints.AsArray())
                    //Re-enable All other Referencing FKey Checks that were disable above to allow the switching above...
                    .EnableReferencingForeignKeyChecks(this.EnableDataConstraintChecksOnCompletion, otherReferencingFKeyConstraints.AsArray())
                    //Finally cleanup the Loading and Discarding tables...
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
