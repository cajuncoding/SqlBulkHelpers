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

        public async Task<CloneTableInfo> CloneTableStructureAsync(
            SqlTransaction sqlTransaction,
            string sourceTableName = null,
            string targetTableName = null,
            bool recreateIfExists = true
        ) => (
            await CloneTableStructuresAsync(
                sqlTransaction, 
                tablesToClone: new [] { CloneTableInfo.From<T,T>(sourceTableName, targetTableName) }, 
                recreateIfExists
            ).ConfigureAwait(false)
        ).FirstOrDefault();

        public async Task<CloneTableInfo[]> CloneTableStructuresAsync(
            SqlTransaction sqlTransaction,
            IEnumerable<CloneTableInfo> tablesToClone,
            bool recreateIfExists = true
        )
        {
            sqlTransaction.AssertArgumentIsNotNull(nameof(sqlTransaction));
            var cloneInfoList = tablesToClone.ToList();

            if (cloneInfoList.IsNullOrEmpty())
                throw new ArgumentException("At least one source & target table pair must be specified.");

            var cloneTableStructureSqlScriptBuilder = MaterializedDataScriptBuilder.NewSqlScript();

            foreach (var cloneInfo in cloneInfoList)
            {
                var sourceTable = cloneInfo.SourceTable;
                var targetTable = cloneInfo.TargetTable;

                //NOTE: If the Target Table was not specified and the Schemas are still he same we have to Target a different schema
                //  so we use the globally configured default Loading Schema...
                if (targetTable.SchemaName == sourceTable.SchemaName)
                    targetTable = targetTable.SwitchSchema(BulkHelpersConfig.MaterializedDataDefaultLoadingSchema);

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
            }

            //Execute the Script!
            await sqlTransaction
                .ExecuteMaterializedDataSqlScriptAsync(cloneTableStructureSqlScriptBuilder, BulkHelpersConfig.MaterializeDataStructureProcessingTimeoutSeconds)
                .ConfigureAwait(false);

            //If everything was successful then we can simply return the input values as they were all cloned...
            return cloneInfoList.ToArray();
        }

        public Task<CloneTableInfo[]> CloneTableStructuresForMaterializationAsync(SqlTransaction sqlTransaction, string loadSchemaName, string holdSchemaName, params string[] tableNames)
            => CloneTableStructuresForMaterializationAsync(sqlTransaction, tableNames, loadSchemaName, holdSchemaName);

        public async Task<CloneTableInfo[]> CloneTableStructuresForMaterializationAsync(SqlTransaction sqlTransaction, IEnumerable<string> tableNames, string loadingSchemaName = null, string tempHoldingSchemaName = null)
        {
            var materializationCloneInfoList = new List<CloneTableInfo>();
            var tableNamesList = tableNames.ToList();

            //1) First compute all table cloning instructions to generate the Loading Tables and the Hold Tables for every table to be cloned...
            //  Add Clones for Loading tables...
            var loadingTablesSchema = loadingSchemaName.TrimTableNameTerm() ?? BulkHelpersConfig.MaterializedDataDefaultLoadingSchema;
            materializationCloneInfoList.AddRange(tableNamesList.Select(n => CloneTableInfo.ForNewSchema(n, loadingTablesSchema)));
            //  Add Clones for Temp/Holding tables (used for switching Live OUT for later cleanup)...
            var tempHoldingTablesSchema = tempHoldingSchemaName.TrimTableNameTerm() ?? BulkHelpersConfig.MaterializedDataDefaultTempHoldingSchema;
            materializationCloneInfoList.AddRange(tableNamesList.Select(n => CloneTableInfo.ForNewSchema(n, tempHoldingTablesSchema)));
            
            //2) Now we can clone all tables efficiently!
            var cloneResults = await CloneTableStructuresAsync(sqlTransaction, materializationCloneInfoList).ConfigureAwait(false);
            return cloneResults;
        }

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

        public Task<TableNameTerm[]> TruncateTableAsync(SqlTransaction sqlTransaction, string tableNameOverride = null, bool forceOverrideOfConstraints = false)
            => TruncateTablesAsync(sqlTransaction, forceOverrideOfConstraints, GetMappedTableNameTerm(tableNameOverride).FullyQualifiedTableName);

        public async Task<TableNameTerm[]> TruncateTablesAsync(SqlTransaction sqlTransaction, bool forceOverrideOfConstraints, params string[] tableNames)
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

                    //Use Materialized Data Helpers to efficiently SWAP out with EMPTY table -- effectively clearing the original Table!
                    truncateTableSqlScriptBuilder
                        .DropForeignKeyConstraints(tableNameTerm, fkeyConstraintsArray)
                        .TruncateTable(tableNameTerm)
                        .AddForeignKeyConstraints(tableNameTerm, fkeyConstraintsArray);
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
        
    }
}
