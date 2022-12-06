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

        #region Materialize Data Methods (& Cloning for Materialization Process)

        public async Task<MaterializeDataContext> MaterializeData(SqlTransaction sqlTransaction, string loadSchemaName, string tempHoldingSchemaName, params string[] tableNames)
        {
            var cloneMaterializationTables = await CloneTableStructuresForMaterializationAsync(
                sqlTransaction,
                tableNames,
                loadSchemaName,
                tempHoldingSchemaName
            ).ConfigureAwait(false);

            return new MaterializeDataContext(cloneMaterializationTables, async (MaterializationTableInfo[] materializationTables) =>
            {
                var finishMaterializationSqlScriptBuilder = MaterializedDataScriptBuilder.NewSqlScript();

                //NOTE: We update ALL tables to each Stage together so that any/all constraints, relationships, etc.
                //      are valid based on new data that was likely populated in the full set of tables now materialized with new data!
                //1) First we switch all existing Live tables to Temp/Holding -- this Frees the Live table up to be updated!
                foreach (var materializationTableInfo in materializationTables)
                {
                    finishMaterializationSqlScriptBuilder.SwitchTables(materializationTableInfo.OriginalTable, materializationTableInfo.TempHoldingTable);
                }

                //2) Second we switch all existing Loading/Staging tables to Live -- this updates the Live Data!
                foreach (var materializationTableInfo in materializationTables)
                {
                    finishMaterializationSqlScriptBuilder.SwitchTables(materializationTableInfo.LoadingTable, materializationTableInfo.OriginalTable);
                }

                //3) Third we clean up all Temp/Holding data to free space, and remove the Loading Tables too -- this leaves only the (new) Live Table in place!
                foreach (var materializationTableInfo in materializationTables)
                {
                    finishMaterializationSqlScriptBuilder.DropTable(materializationTableInfo.LoadingTable);
                    finishMaterializationSqlScriptBuilder.DropTable(materializationTableInfo.TempHoldingTable);
                }

                await sqlTransaction.ExecuteMaterializedDataSqlScriptAsync(
                    finishMaterializationSqlScriptBuilder,
                    BulkHelpersConfig.MaterializeDataStructureProcessingTimeoutSeconds
                ).ConfigureAwait(false);
            });
        }

        public Task<MaterializationTableInfo[]> CloneTableStructuresForMaterializationAsync(
            SqlTransaction sqlTransaction, 
            string loadSchemaName, 
            string tempHoldSchemaName, 
            params string[] tableNames
        ) => CloneTableStructuresForMaterializationAsync(sqlTransaction, tableNames, loadSchemaName, tempHoldSchemaName);

        public async Task<MaterializationTableInfo[]> CloneTableStructuresForMaterializationAsync(
            SqlTransaction sqlTransaction, 
            IEnumerable<string> tableNames, 
            string loadingSchemaName = null, 
            string tempHoldingSchemaName = null
        )
        {
            var materializationTableInfoList = new List<MaterializationTableInfo>();
            var cloneInfoList = new List<CloneTableInfo>();

            var tableNamesList = tableNames.ToList();
            var loadingTablesSchema = loadingSchemaName.TrimTableNameTerm() ?? BulkHelpersConfig.MaterializedDataDefaultLoadingSchema;
            var tempHoldingTablesSchema = tempHoldingSchemaName.TrimTableNameTerm() ?? BulkHelpersConfig.MaterializedDataDefaultTempHoldingSchema;

            //1) First compute all table cloning instructions, and Materialization table info./details to generate the Loading Tables and the Hold Tables for every table to be cloned...
            foreach (var originalTable in tableNamesList.Select(n => n.ParseAsTableNameTerm()))
            {
                //  Add Clones for Loading tables...
                var loadingCloneInfo = CloneTableInfo.ForNewSchema(originalTable, loadingTablesSchema);
                cloneInfoList.Add(loadingCloneInfo);

                //  Add Clones for Temp/Holding tables (used for switching Live OUT for later cleanup)...
                var tempHoldingCloneInfo = CloneTableInfo.ForNewSchema(originalTable, tempHoldingTablesSchema);
                cloneInfoList.Add(tempHoldingCloneInfo);
                
                //Finally aggregate the original, loading, and temp/holding tables into the MaterializationTableInfo
                var materializationTableInfo = new MaterializationTableInfo(originalTable, loadingCloneInfo.TargetTable, tempHoldingCloneInfo.TargetTable);
                materializationTableInfoList.Add(materializationTableInfo);
            }

            //2) Now we can clone all tables efficiently creating all Loading and Temp/Holding tables!
            await CloneTableAsync(sqlTransaction, cloneInfoList).ConfigureAwait(false);
            
            //Finally we return the complete Materialization Table Info details...
            return materializationTableInfoList.ToArray();
        }

        #endregion
        
        #region Clone Table Methods

        public async Task<CloneTableInfo> CloneTableAsync(
            SqlTransaction sqlTransaction,
            string sourceTableName = null,
            string targetTableName = null,
            bool recreateIfExists = false,
            bool copyDataFromSource = false
        ) => (
            await CloneTableAsync(
                sqlTransaction,
                tablesToClone: new[] { CloneTableInfo.From<T, T>(sourceTableName, targetTableName) },
                recreateIfExists
            ).ConfigureAwait(false)
        ).FirstOrDefault();

        public async Task<CloneTableInfo[]> CloneTableAsync(
            SqlTransaction sqlTransaction,
            IEnumerable<CloneTableInfo> tablesToClone,
            bool recreateIfExists = false,
            bool copyDataFromSource = false
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
                if (targetTable.FullyQualifiedTableName.Equals(sourceTable.FullyQualifiedTableName, StringComparison.OrdinalIgnoreCase))
                    throw new InvalidOperationException($"The source table name {sourceTable.FullyQualifiedTableName} and target table name {targetTable.FullyQualifiedTableName} must be unique.");

                var sourceTableSchemaDefinition = SqlBulkHelpersSchemaLoaderCache
                    .GetSchemaLoader(sqlTransaction.Connection.ConnectionString)
                    ?.GetTableSchemaDefinition(sourceTable.FullyQualifiedTableName, sqlTransaction);

                if (sourceTableSchemaDefinition == null)
                    throw new ArgumentException($"Could not resolve the source table schema for {sourceTable.FullyQualifiedTableName} on the provided connection.");

                cloneTableStructureSqlScriptBuilder.CloneTableWithAllElements(
                    sourceTableSchemaDefinition,
                    targetTable,
                    recreateIfExists ? IfExists.Recreate : IfExists.StopProcessingWithException
                );

                cloneInfoResults.Add(new CloneTableInfo(sourceTable, targetTable));
            }

            //Execute the Script!
            await sqlTransaction
                .ExecuteMaterializedDataSqlScriptAsync(cloneTableStructureSqlScriptBuilder, BulkHelpersConfig.MaterializeDataStructureProcessingTimeoutSeconds)
                .ConfigureAwait(false);

            //If everything was successful then we can simply return the input values as they were all cloned...
            return cloneInfoResults.ToArray();
        }

        #endregion
        
        #region Drop Table Methods

        public Task<TableNameTerm[]> DropTableAsync(SqlTransaction sqlTransaction, string tableNameOverride = null)
            => DropTablesAsync(sqlTransaction, GetMappedTableNameTerm(tableNameOverride).FullyQualifiedTableName);

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

            return tableNameTermsList.ToArray();
        }

        #endregion
       
        #region Truncate Table Methods

        public Task<TableNameTerm[]> ClearTableAsync(SqlTransaction sqlTransaction, string tableNameOverride = null, bool forceOverrideOfConstraints = false)
            => ClearTablesAsync(sqlTransaction, forceOverrideOfConstraints, GetMappedTableNameTerm(tableNameOverride).FullyQualifiedTableName);

        public async Task<TableNameTerm[]> ClearTablesAsync(SqlTransaction sqlTransaction, bool forceOverrideOfConstraints, params string[] tableNames)
        {
            if (!tableNames.HasAny())
                return Array.Empty<TableNameTerm>();

            var truncateTableSqlScriptBuilder = MaterializedDataScriptBuilder.NewSqlScript();
            var tableNameTermsList = tableNames.Distinct().Select(TableNameTerm.From).ToList();

            foreach (var tableNameTerm in tableNameTermsList)
            {
                if (forceOverrideOfConstraints)
                {
                    var tableDef = GetTableSchemaDefinitionInternal(sqlTransaction, tableNameTerm);
                    var fkeyConstraintsArray = tableDef.ForeignKeyConstraints.ToArray();

                    //Cloning without a target table name result in unique target name being generated based on the source...
                    var emptyCloneTableInfo = new CloneTableInfo(tableNameTerm);
                    var holdCloneTableInfo = new CloneTableInfo(tableNameTerm);

                    //Use Materialized Data Helpers to efficiently SWAP out with EMPTY table -- effectively clearing the original Table
                    //  without the need to remove FKey constraints from related tables that reference this one...
                    truncateTableSqlScriptBuilder
                        .CloneTableWithAllElements(tableDef, emptyCloneTableInfo.TargetTable, IfExists.Recreate)
                        .CloneTableWithAllElements(tableDef, holdCloneTableInfo.TargetTable)
                        .DisableReferencingForeignKeyChecks(tableDef.ReferencingForeignKeyConstraints.ToArray())
                        .SwitchTables(tableNameTerm, holdCloneTableInfo.TargetTable)
                        .SwitchTables(emptyCloneTableInfo.TargetTable, tableNameTerm)
                        .DropTable(holdCloneTableInfo.TargetTable)
                        .DropTable(emptyCloneTableInfo.TargetTable)
                        .EnableReferencingForeignKeyChecks(tableDef.ReferencingForeignKeyConstraints.ToArray());
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

            return tableNameTermsList.ToArray();
        }

        #endregion

    }
}
