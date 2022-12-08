using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Data.SqlClient;

namespace SqlBulkHelpers.MaterializedData
{
    public class MaterializeDataHelper<T> : BaseHelper<T> where T : class
    {
        #region Constructors

        /// <inheritdoc/>
        public MaterializeDataHelper(ISqlBulkHelpersDBSchemaLoader sqlDbSchemaLoader, ISqlBulkHelpersConfig bulkHelpersConfig = null)
            : base(sqlDbSchemaLoader, bulkHelpersConfig)
        {
        }

        /// <inheritdoc/>
        public MaterializeDataHelper(ISqlBulkHelpersConnectionProvider sqlBulkHelpersConnectionProvider, ISqlBulkHelpersConfig bulkHelpersConfig = null)
            : base(sqlBulkHelpersConnectionProvider, bulkHelpersConfig)
        {
        }

        /// <inheritdoc/>
        public MaterializeDataHelper(SqlTransaction sqlTransaction, ISqlBulkHelpersConfig bulkHelpersConfig = null)
            : base(sqlTransaction, bulkHelpersConfig)
        {
        }

        #endregion

        #region Materialize Data API Methods (& Cloning for Materialization Process)

        public Task<MaterializeDataContext> MaterializeDataIntoAsync(SqlTransaction sqlTransaction, params string[] tableNames)
            => MaterializeDataIntoAsync(sqlTransaction, BulkHelpersConfig.MaterializedDataLoadingSchema, BulkHelpersConfig.MaterializedDataDiscardingSchema, tableNames);

        public async Task<MaterializeDataContext> MaterializeDataIntoAsync(SqlTransaction sqlTransaction, string loadingSchemaName, string discardingSchemaName, params string[] tableNames)
        {
            //This will clone each of the Live tables into one Temp and one Loading table (each in different Schema as defined in the BulkHelperSettings)..
            var cloneMaterializationTables = await CloneTableStructuresForMaterializationAsync(
                sqlTransaction,
                tableNames,
                loadingSchemaName,
                discardingSchemaName
            ).ConfigureAwait(false);

            return new MaterializeDataContext(sqlTransaction, cloneMaterializationTables, this.BulkHelpersConfig);
        }

        public Task<MaterializationTableInfo[]> CloneTableStructuresForMaterializationAsync(
            SqlTransaction sqlTransaction,
            params string[] tableNames
        ) => CloneTableStructuresForMaterializationAsync(
            sqlTransaction, 
            tableNames, 
            BulkHelpersConfig.MaterializedDataLoadingSchema, 
            BulkHelpersConfig.MaterializedDataDiscardingSchema
        );

        public async Task<MaterializationTableInfo[]> CloneTableStructuresForMaterializationAsync(
            SqlTransaction sqlTransaction, 
            IEnumerable<string> tableNames, 
            string loadingSchemaName = null, 
            string discardingSchemaName = null
        )
        {
            sqlTransaction.AssertArgumentIsNotNull(nameof(sqlTransaction));

            var materializationTableInfoList = new List<MaterializationTableInfo>();
            var cloneInfoToExecuteList = new List<CloneTableInfo>();

            var tableNameTermsList = tableNames.Select(n => n.ParseAsTableNameTerm()).ToList();
            if (!tableNameTermsList.HasAny())
                return Array.Empty<MaterializationTableInfo>();

            var loadingTablesSchema = loadingSchemaName.TrimTableNameTerm() ?? BulkHelpersConfig.MaterializedDataLoadingSchema;
            var discardingTablesSchema = discardingSchemaName.TrimTableNameTerm() ?? BulkHelpersConfig.MaterializedDataDiscardingSchema;
            
            //This Lookup is to determine what tables are in context so that all referencing FKey Constraints can be resolved and disabled...
            var tablesInContextLookup = tableNameTermsList.ToLookup(t => t, StringComparer.OrdinalIgnoreCase);

            //1) First compute all table cloning instructions, and Materialization table info./details to generate the Loading Tables and the Discard Tables for every table to be cloned...
            foreach (var originalTableNameTerm in tableNameTermsList)
            {
                //Add Clones for Loading tables...
                //NOTE: It is important that we DISABLE constraints on the Loading Tables so that FKey Checks are not enforced until ALL tables are switched to LIVE;
                //      otherwise the bulk loading may fail if the data doesn't exist in other related tables which should be part of the Materialization context also being loaded...
                var loadingCloneInfo = CloneTableInfo.ForNewSchema(originalTableNameTerm, loadingTablesSchema);
                cloneInfoToExecuteList.Add(loadingCloneInfo);

                //Add Clones for Discarding tables (used for switching Live OUT for later cleanup)...
                var discardingCloneInfo = CloneTableInfo.ForNewSchema(originalTableNameTerm, discardingTablesSchema);
                cloneInfoToExecuteList.Add(discardingCloneInfo);

                //Finally aggregate the Live/Original, Loading, and Discarding tables into the MaterializationTableInfo
                var originalTableDef = GetTableSchemaDefinitionInternal(sqlTransaction, originalTableNameTerm);

                var materializationTableInfo = new MaterializationTableInfo(originalTableDef, loadingCloneInfo.TargetTable, discardingCloneInfo.TargetTable);
                materializationTableInfoList.Add(materializationTableInfo);
            }

            //2) Now we can clone all tables efficiently creating all Loading and Discard tables!
            //   NOTE: We always Recreate if it Exists, with no data, but without FKey Constraints so that there is no links resulting in Transaction locks on Live tables!
            await CloneTablesInternalAsync(
                sqlTransaction, 
                cloneInfoToExecuteList, 
                recreateIfExists: true, 
                copyDataFromSource: false,
                includeFKeyConstraints: false
            ).ConfigureAwait(false);

            //Finally we return the complete Materialization Table Info details...
            return materializationTableInfoList.AsArray();
        }

        #endregion
        
        #region Clone Table API Methods

        public async Task<CloneTableInfo> CloneTableAsync(
            SqlTransaction sqlTransaction,
            string sourceTableName = null,
            string targetTableName = null,
            bool recreateIfExists = false,
            bool copyDataFromSource = false
        ) => (await CloneTablesAsync(sqlTransaction, tablesToClone: new[] { CloneTableInfo.From<T, T>(sourceTableName, targetTableName) }, recreateIfExists).ConfigureAwait(false)).FirstOrDefault();

        public Task<CloneTableInfo[]> CloneTablesAsync(
            SqlTransaction sqlTransaction,
            bool recreateIfExists,
            bool copyDataFromSource,
            params CloneTableInfo[] tablesToClone
        ) => CloneTablesAsync(sqlTransaction, tablesToClone, recreateIfExists, copyDataFromSource);

        public Task<CloneTableInfo[]> CloneTablesAsync(
            SqlTransaction sqlTransaction,
            IEnumerable<CloneTableInfo> tablesToClone,
            bool recreateIfExists = false,
            bool copyDataFromSource = false
        ) => CloneTablesInternalAsync(sqlTransaction, tablesToClone, recreateIfExists, copyDataFromSource);

        //Internal method with additional flags for normal cloning & materialized data cloning
        //NOTE: Materialization process requires special handling such as No FKeys being added to Temp/Loading Tables until ready to Switch
        protected async Task<CloneTableInfo[]> CloneTablesInternalAsync(
            SqlTransaction sqlTransaction,
            IEnumerable<CloneTableInfo> tablesToClone,
            bool recreateIfExists = false,
            bool copyDataFromSource = false,
            bool includeFKeyConstraints = false
        )
        {
            sqlTransaction.AssertArgumentIsNotNull(nameof(sqlTransaction));
            var cloneInfoList = tablesToClone.ToList();

            if (cloneInfoList.IsNullOrEmpty())
                throw new ArgumentException("At least one source & target table pair must be specified.");

            var cloneTableStructureSqlScriptBuilder = MaterializedDataScriptBuilder.NewSqlScript();
            var cloneInfoResults = new List<CloneTableInfo>();
            foreach (var cloneInfo in cloneInfoList)
            {
                var sourceTable = cloneInfo.SourceTable;
                var targetTable = cloneInfo.TargetTable;

                //If both Source & Target are the same (e.g. Target was not explicitly specified) then we adjust
                //  the Target to ensure we create a copy and append a unique Copy Id...
                if (targetTable.EqualsIgnoreCase(sourceTable))
                    throw new InvalidOperationException($"The source table name {sourceTable.FullyQualifiedTableName} and target table name {targetTable.FullyQualifiedTableName} must be unique.");

                var sourceTableSchemaDefinition = GetTableSchemaDefinitionInternal(sqlTransaction, sourceTable);
                if (sourceTableSchemaDefinition == null)
                    throw new ArgumentException($"Could not resolve the source table schema for {sourceTable.FullyQualifiedTableName} on the provided connection.");

                //TODO: Implement Support to copy Table Data...
                cloneTableStructureSqlScriptBuilder.CloneTableWithAllElements(
                    sourceTableSchemaDefinition,
                    targetTable,
                    recreateIfExists ? IfExists.Recreate : IfExists.StopProcessingWithException,
                    cloneIdentitySeedValue: BulkHelpersConfig.IsCloningIdentitySeedValueEnabled,
                    includeFKeyConstraints: includeFKeyConstraints
                );

                ////TODO: Might (potentially if it doesn't impede performance too much) implement support for re-mapping FKey constraints to Materialization Context tables so data integrity issues will be caught sooner
                ////      in the process, but for now they are caught when FKey constraints are re-enabled after Switching tables...
                //if (includeFKeyConstraints)
                //    cloneTableStructureSqlScriptBuilder.DisableAllTableConstraintChecks(targetTable);

                cloneInfoResults.Add(new CloneTableInfo(sourceTable, targetTable));
            }

            //Execute the Script!
            await sqlTransaction
                .ExecuteMaterializedDataSqlScriptAsync(cloneTableStructureSqlScriptBuilder, BulkHelpersConfig.MaterializeDataStructureProcessingTimeoutSeconds)
                .ConfigureAwait(false);

            //If everything was successful then we can simply return the input values as they were all cloned...
            return cloneInfoResults.AsArray();
        }

        #endregion

        #region Drop Table API Methods

        public async Task<TableNameTerm> DropTableAsync(SqlTransaction sqlTransaction, string tableNameOverride = null)
            => (await DropTablesAsync(sqlTransaction, GetMappedTableNameTerm(tableNameOverride).FullyQualifiedTableName).ConfigureAwait(false)).FirstOrDefault();

        public async Task<TableNameTerm[]> DropTablesAsync(SqlTransaction sqlTransaction, params string[] tableNames)
        {
            if (!tableNames.HasAny())
                return Array.Empty<TableNameTerm>();

            var tableNameTermsList = tableNames.Distinct().Select(TableNameTerm.From).ToList();
            var dropTableSqlScriptBuilder = MaterializedDataScriptBuilder.NewSqlScript();

            foreach (var tableNameTerm in tableNameTermsList)
                dropTableSqlScriptBuilder.DropTable(tableNameTerm);

            //Execute the Script!
            await sqlTransaction
                .ExecuteMaterializedDataSqlScriptAsync(dropTableSqlScriptBuilder, BulkHelpersConfig.MaterializeDataStructureProcessingTimeoutSeconds)
                .ConfigureAwait(false);

            return tableNameTermsList.AsArray();
        }

        #endregion
       
        #region Clear Table API Methods

        public async Task<TableNameTerm> ClearTableAsync(SqlTransaction sqlTransaction, string tableNameOverride = null, bool forceOverrideOfConstraints = false)
            => (await ClearTablesAsync(sqlTransaction, forceOverrideOfConstraints, GetMappedTableNameTerm(tableNameOverride).FullyQualifiedTableName).ConfigureAwait(false)).FirstOrDefault();

        public async Task<TableNameTerm[]> ClearTablesAsync(SqlTransaction sqlTransaction, bool forceOverrideOfConstraints, params string[] tableNames)
        {
            if (!tableNames.HasAny())
                return Array.Empty<TableNameTerm>();

            var truncateTableSqlScriptBuilder = MaterializedDataScriptBuilder.NewSqlScript();
            var tableNameTermsList = tableNames.Distinct().Select(TableNameTerm.From).ToList();

            foreach (var tableNameTerm in tableNameTermsList)
            {
                var tableDef = GetTableSchemaDefinitionInternal(sqlTransaction, tableNameTerm);
                if (tableDef.ReferencingForeignKeyConstraints.HasAny() && forceOverrideOfConstraints)
                {
                    var referencingFKeyConstraints = tableDef.ReferencingForeignKeyConstraints.AsArray();

                    //Cloning without a target table name result in unique target name being generated based on the source...
                    var emptyCloneTableInfo = new CloneTableInfo(tableNameTerm);
                    var discardCloneTableInfo = new CloneTableInfo(tableNameTerm);

                    //Use Materialized Data Helpers to efficiently SWAP out with EMPTY table -- effectively clearing the original Table
                    //  without the need to remove FKey constraints from related tables that reference this one...
                    truncateTableSqlScriptBuilder
                        .CloneTableWithAllElements(tableDef, emptyCloneTableInfo.TargetTable, IfExists.Recreate, true, true)
                        .CloneTableWithAllElements(tableDef, discardCloneTableInfo.TargetTable, IfExists.Recreate, true, true)
                        .DisableReferencingForeignKeyChecks(referencingFKeyConstraints)
                        .SwitchTables(tableNameTerm, discardCloneTableInfo.TargetTable)
                        .SwitchTables(emptyCloneTableInfo.TargetTable, tableNameTerm)
                        .DropTable(discardCloneTableInfo.TargetTable)
                        .DropTable(emptyCloneTableInfo.TargetTable)
                        //NOTE: Despite the fact that we are forcing the override of Table Constraints we still need to ensure overall data integrity so
                        //      FKey references will be validated -- this means that Tables need to be cleared in the proper Order of FKey referencing chain
                        //      to maintain data integrity.
                        .EnableReferencingForeignKeyChecks(true, referencingFKeyConstraints);
                }
                else
                {
                    truncateTableSqlScriptBuilder.TruncateTable(tableNameTerm);
                }
            }

            //Execute the Script!
            await sqlTransaction
                .ExecuteMaterializedDataSqlScriptAsync(truncateTableSqlScriptBuilder, BulkHelpersConfig.MaterializeDataStructureProcessingTimeoutSeconds)
                .ConfigureAwait(false);

            return tableNameTermsList.AsArray();
        }

        #endregion

    }
}
