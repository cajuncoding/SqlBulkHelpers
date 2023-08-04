using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlTypes;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Data.SqlClient;
using SqlBulkHelpers.CustomExtensions;

namespace SqlBulkHelpers.MaterializedData
{
    internal class MaterializeDataHelper<T> : BaseHelper<T> where T : class
    {
        #region Constructors

        /// <inheritdoc/>
        public MaterializeDataHelper(ISqlBulkHelpersConfig bulkHelpersConfig = null)
            : base(bulkHelpersConfig)
        {
        }

        #endregion

        #region Materialize Data API Methods (& Cloning for Materialization Process)

        public Task<MaterializeDataContext> StartMaterializeDataProcessAsync(SqlTransaction sqlTransaction, params string[] tableNames)
            => StartMaterializeDataProcessAsync(sqlTransaction, BulkHelpersConfig.MaterializedDataLoadingSchema, BulkHelpersConfig.MaterializedDataDiscardingSchema, tableNames);

        public async Task<MaterializeDataContext> StartMaterializeDataProcessAsync(SqlTransaction sqlTransaction, string loadingSchemaName, string discardingSchemaName, params string[] tableNames)
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

            //NOTE: The ParseAsTableNameTerm() method will validate that the value could be parsed...
            var tableNameTermsList = tableNames.Select(n => n.ParseAsTableNameTerm()).ToList();
            if (!tableNameTermsList.HasAny())
                throw new ArgumentOutOfRangeException(nameof(tableNames), "No valid table names were specified.");

            var loadingTablesSchema = loadingSchemaName.TrimTableNameTerm() ?? BulkHelpersConfig.MaterializedDataLoadingSchema;
            loadingTablesSchema.AssertArgumentIsNotNullOrWhiteSpace(nameof(loadingTablesSchema));

            var discardingTablesSchema = discardingSchemaName.TrimTableNameTerm() ?? BulkHelpersConfig.MaterializedDataDiscardingSchema;
            discardingTablesSchema.AssertArgumentIsNotNullOrWhiteSpace(nameof(discardingTablesSchema));

            //Optional Perf. Optimization: If ConcurrentConnections are enabled we can optimize performance by asynchronously pre-loading Table Schemas with concurrent Sql Connections...
            if (BulkHelpersConfig.IsConcurrentConnectionProcessingEnabled)
                await PreCacheTableSchemaDefinitionsForMaterialization(tableNameTermsList).ConfigureAwait(false);

            //1) First compute all table cloning instructions, and Materialization table info./details to generate the Loading Tables and the Discard Tables for every table to be cloned...
            foreach (var originalTableNameTerm in tableNameTermsList)
            {
                //Add Clones for Loading tables...
                //NOTE: We make the Loading table name highly unique just in case multiple processes run at the same time they will have less risk of impacting each other;
                //  though such a conflict would be a flawed design and should be eliminated via an SQL lock or Distributed Mutex lock (aka SqlAppLockHelper library).
                var loadingCloneInfo = CloneTableInfo.ForNewSchema(originalTableNameTerm, loadingTablesSchema).MakeTargetTableNameUnique();
                cloneInfoToExecuteList.Add(loadingCloneInfo);

                // ReSharper disable once PossibleNullReferenceException
                bool isDiscardingSchemaDifferentFromLoadingSchema = !loadingSchemaName.Equals(discardingTablesSchema, StringComparison.OrdinalIgnoreCase);

                //Add Clones for Discarding tables (used for switching Live OUT for later cleanup)...
                //NOTE: We try to keep our Loading and Discarding table names in sync if possible but enforce their uniqueness if the Schema names are not different...
                var discardingCloneInfo = isDiscardingSchemaDifferentFromLoadingSchema
                    //Try to keep the Table Names highly unique but in-sync between Loading and Discarding schemas (for debugging purposes mainly).
                    ? CloneTableInfo.From(originalTableNameTerm, loadingCloneInfo.TargetTable.SwitchSchema(discardingTablesSchema))
                    //Otherwise enforce uniqueness...
                    : CloneTableInfo.ForNewSchema(originalTableNameTerm, discardingTablesSchema).MakeTargetTableNameUnique();
                    
                cloneInfoToExecuteList.Add(discardingCloneInfo);

                //Finally aggregate the Live/Original, Loading, and Discarding tables into the MaterializationTableInfo
                var originalTableDef = await GetTableSchemaDefinitionInternalAsync(
                    TableSchemaDetailLevel.ExtendedDetails, 
                    sqlTransaction.Connection, 
                    sqlTransaction, 
                    originalTableNameTerm
                ).ConfigureAwait(false);

                //VALIDATE That FullTextIndex Processing can be handled if enabled, and throw a helpful error if not
                //NOTE: This only applies IF a table has a Full Text Index, otherwise we can process all other scenarios as expected (within our Transaction)...
                AssertTableSchemaDefinitionAndConfigurationIsValidForMaterialization(originalTableDef);

                var materializationTableInfo = new MaterializationTableInfo(originalTableNameTerm, originalTableDef, loadingCloneInfo.TargetTable, discardingCloneInfo.TargetTable);
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

        /// <summary>
        /// VALIDATE that various aspects of the Table and the Configuration are set correctly for Materialization processing.
        /// For Example: If any table has a FullTextIndex then we must also have concurrent connection factory enabled so that it can be processed outside
        ///     of the current Transaction (this is a limitation of Sql Server for FullTextIndexes).
        /// </summary>
        /// <param name="tableDefinition"></param>
        protected void AssertTableSchemaDefinitionAndConfigurationIsValidForMaterialization(SqlBulkHelpersTableDefinition tableDefinition)
        {
            if (tableDefinition.FullTextIndex != null && BulkHelpersConfig.IsFullTextIndexHandlingEnabled && !BulkHelpersConfig.IsConcurrentConnectionProcessingEnabled)
            {
                throw new InvalidOperationException(
            $"The table {tableDefinition.TableFullyQualifiedName} contains a Full Text Index and which cannot be processed unless concurrent connections are enabled in the {nameof(SqlBulkHelpersConfig)}. " +
                    $"The the materialized data helpers are configured to automatically handle this (via [{nameof(SqlBulkHelpersConfig)}.{nameof(ISqlBulkHelpersConfig.IsFullTextIndexHandlingEnabled)}]) however " +
                    $"no Sql Connection Factory or {nameof(ISqlBulkHelpersConnectionProvider)} was specified. You need correctly configure these via " +
                    $"[{nameof(SqlBulkHelpersConfig)}.{nameof(SqlBulkHelpersConfig.EnableConcurrentSqlConnectionProcessing)}()] or [{nameof(SqlBulkHelpersConfig)}.{nameof(SqlBulkHelpersConfig.ConfigureDefaults)}()] " +
                    $"for the materialized data processing to proceed."
                );
            }
        }

        /// <summary>
        /// If ConcurrentConnections are enabled we can optimize performance by asynchronously pre-loading Table Schemas with concurrent Sql Connections...
        /// </summary>
        /// <param name="tableNameTerms"></param>
        /// <returns></returns>
        protected async Task<List<SqlBulkHelpersTableDefinition>> PreCacheTableSchemaDefinitionsForMaterialization(IEnumerable<TableNameTerm> tableNameTerms)
        {
            var tableDefinitionResults = new List<SqlBulkHelpersTableDefinition>();

            //OPTIMIZE the retrieval of Table Schema definitions for the Materialized Data processing...
            //NOTE: If the Concurrent Connection processing is enabled we can retrieve schemas via parallel Async connections,
            //      otherwise we fall-back to serially retrieving them all...
            if (BulkHelpersConfig.IsConcurrentConnectionProcessingEnabled)
            {
                await tableNameTerms.ForEachAsync(BulkHelpersConfig.MaxConcurrentConnections, async tableNameTerm =>
                {
                    using (var sqlConcurrentConnection = await BulkHelpersConfig.ConcurrentConnectionFactory.NewConnectionAsync().ConfigureAwait(false))
                    {
                        var tableDef = await GetTableSchemaDefinitionInternalAsync(
                            TableSchemaDetailLevel.ExtendedDetails, 
                            sqlConcurrentConnection, 
                            sqlTransaction: null, 
                            tableNameTerm
                        ).ConfigureAwait(false);

                        lock (tableDefinitionResults) tableDefinitionResults.Add(tableDef);
                    }
                }).ConfigureAwait(false);
            }

            return tableDefinitionResults;
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

            var sqlScriptBuilder = MaterializedDataScriptBuilder.NewSqlScript();
            var cloneInfoResults = new List<CloneTableInfo>();
            foreach (var cloneInfo in cloneInfoList)
            {
                var sourceTable = cloneInfo.SourceTable;
                var targetTable = cloneInfo.TargetTable;

                //If both Source & Target are the same (e.g. Target was not explicitly specified) then we adjust
                //  the Target to ensure we create a copy and append a unique Copy Id...
                if (targetTable.EqualsIgnoreCase(sourceTable))
                    throw new InvalidOperationException($"The source table name {sourceTable.FullyQualifiedTableName} and target table name {targetTable.FullyQualifiedTableName} must be unique.");

                var sourceTableSchemaDefinition = await GetTableSchemaDefinitionInternalAsync(TableSchemaDetailLevel.ExtendedDetails, sqlTransaction.Connection, sqlTransaction, sourceTable);
                if (sourceTableSchemaDefinition == null)
                    throw new ArgumentException($"Could not resolve the source table schema for {sourceTable.FullyQualifiedTableName} on the provided connection.");

                sqlScriptBuilder.CloneTableWithAllElements(
                    sourceTableSchemaDefinition,
                    targetTable,
                    recreateIfExists ? IfExists.Recreate : IfExists.StopProcessingWithException,
                    cloneIdentitySeedValue: BulkHelpersConfig.IsCloningIdentitySeedValueEnabled,
                    includeFKeyConstraints: includeFKeyConstraints,
                    copyDataFromSource: copyDataFromSource
                );

                ////TODO: Might (potentially if it doesn't impede performance too much) implement support for re-mapping FKey constraints to Materialization Context tables so data integrity issues will be caught sooner
                ////      in the process, but for now they are caught when FKey constraints are re-enabled after Switching tables...
                //if (includeFKeyConstraints)
                //    cloneTableStructureSqlScriptBuilder.DisableAllTableConstraintChecks(targetTable);

                cloneInfoResults.Add(new CloneTableInfo(sourceTable, targetTable));
            }

            //Execute the Script!
            await sqlTransaction
                .ExecuteMaterializedDataSqlScriptAsync(sqlScriptBuilder, BulkHelpersConfig.MaterializeDataStructureProcessingTimeoutSeconds)
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
            var sqlScriptBuilder = MaterializedDataScriptBuilder.NewSqlScript();

            foreach (var tableNameTerm in tableNameTermsList)
                sqlScriptBuilder.DropTable(tableNameTerm);

            //Execute the Script!
            await sqlTransaction
                .ExecuteMaterializedDataSqlScriptAsync(sqlScriptBuilder, BulkHelpersConfig.MaterializeDataStructureProcessingTimeoutSeconds)
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

            var distinctTableNames = tableNames.Distinct().ToList();
            var tableNameTermsList = distinctTableNames.Select(TableNameTerm.From).ToList();
            var tablesToProcessWithTruncation = new List<TableNameTerm>();

            if (forceOverrideOfConstraints)
            {
                 //NOTE: We use String here because the StartMaterializeDataProcessAsync takes in string names (not parsed names)...
                var tablesToMaterializeAsEmpty = new List<string>();
                foreach(var tableNameTerm in tableNameTermsList)
                {
                    var sqlConnection = sqlTransaction.Connection;
                    var tableDef = await GetTableSchemaDefinitionInternalAsync(TableSchemaDetailLevel.ExtendedDetails, sqlConnection, sqlTransaction: sqlTransaction, tableNameTerm).ConfigureAwait(false);
                    //Bucket our Table Definitions based on if they REQUIRE Materialization or if they can be handled by Truncate processing...
                    if (tableDef.ReferencingForeignKeyConstraints.HasAny() || tableDef.ForeignKeyConstraints.HasAny())
                        lock (tablesToMaterializeAsEmpty) tablesToMaterializeAsEmpty.Add(tableNameTerm);
                    else
                        lock (tablesToProcessWithTruncation) tablesToProcessWithTruncation.Add(tableNameTerm);
                }

                if (tablesToMaterializeAsEmpty.Any())
                {
                    //BBernard
                    //NOTE: To Clear the tables and ensure all Constraints, and FKeys are handled we re-use the Materialized Data Helpers that already do this
                    //          and we simply complete the process by materializing to EMPTY tables (newly cloned) with no data!
                    //START the Materialize Data Process... but we do NOT insert any new data to the Empty Tables!
                    var materializeDataContext = await sqlTransaction.StartMaterializeDataProcessAsync(tablesToMaterializeAsEmpty).ConfigureAwait(false);

                    //We finish the Clearing process by immediately switching out with the new/empty tables to Clear the Data!
                    await materializeDataContext.FinishMaterializeDataProcessAsync().ConfigureAwait(false);
                }
            }
            else
            {
                //Attempt to Process all as with Truncation...
                tablesToProcessWithTruncation.AddRange(tableNameTermsList);
            }

            if (tablesToProcessWithTruncation.Any())
            {
                var truncateTableSqlScriptBuilder = MaterializedDataScriptBuilder.NewSqlScript();

                foreach (var tableNameTerm in tablesToProcessWithTruncation)
                    truncateTableSqlScriptBuilder.TruncateTable(tableNameTerm);

                //Execute the Script!
                await sqlTransaction
                    .ExecuteMaterializedDataSqlScriptAsync(truncateTableSqlScriptBuilder, BulkHelpersConfig.MaterializeDataStructureProcessingTimeoutSeconds)
                    .ConfigureAwait(false);
            }

            return tableNameTermsList.AsArray();
        }

        #endregion

        #region Table Identity Column API Methods

        /// <summary>
        /// Retrieve the Current Identity Value for the specified Table
        /// </summary>
        /// <param name="sqlConnection"></param>
        /// <param name="sqlTransaction"></param>
        /// <param name="tableNameOverride"></param>
        /// <returns></returns>
        public async Task<long> GetTableCurrentIdentityValueAsync(SqlConnection sqlConnection, SqlTransaction sqlTransaction = null, string tableNameOverride = null)
        {
            using (var sqlCmd = CreateGetTableIdentityValueSqlCommand(sqlConnection, sqlTransaction, tableNameOverride))
            {
                var identityResult = await sqlCmd.ExecuteScalarAsync().ConfigureAwait(false);
                
                if (identityResult == null)
                    throw new ArgumentException($"The table specified [{GetMappedTableNameTerm(tableNameOverride).FullyQualifiedTableName}] does not contain an Identity column; current identity value is null.");

                var currentIdentityValue = Convert.ToInt64(identityResult);
                return currentIdentityValue;
            }
        }

        /// <summary>
        /// Retrieve the Current Identity Value for the specified Table
        /// </summary>
        /// <param name="sqlConnection"></param>
        /// <param name="sqlTransaction"></param>
        /// <param name="tableNameOverride"></param>
        /// <returns></returns>
        public long GetTableCurrentIdentityValue(SqlConnection sqlConnection, SqlTransaction sqlTransaction = null, string tableNameOverride = null)
        {
            using (var sqlCmd = CreateGetTableIdentityValueSqlCommand(sqlConnection, sqlTransaction, tableNameOverride))
            {
                var identityResult = sqlCmd.ExecuteScalar();

                if (identityResult == null)
                    throw new ArgumentException($"The table specified [{GetMappedTableNameTerm(tableNameOverride).FullyQualifiedTableName}] does not contain an Identity column; current identity value is null.");

                var currentIdentityValue = Convert.ToInt64(identityResult);
                return currentIdentityValue;
            }
        }

        private SqlCommand CreateGetTableIdentityValueSqlCommand(SqlConnection sqlConnection, SqlTransaction sqlTransaction = null, string tableNameOverride = null)
        {
            var fullyQualifiedTableName = GetMappedTableNameTerm(tableNameOverride).FullyQualifiedTableName;
            var sqlCmd = new SqlCommand("SELECT CURRENT_IDENTITY_VALUE = IDENT_CURRENT(@TableName);", sqlConnection, sqlTransaction);
            sqlCmd.CommandTimeout = BulkHelpersConfig.MaterializeDataStructureProcessingTimeoutSeconds;
            sqlCmd.Parameters.Add("@TableName", SqlDbType.NVarChar).Value = fullyQualifiedTableName;
            return sqlCmd;
        }

        /// <summary>
        /// Sets / Re-seeds the Current Identity Value for the specified Table.
        /// </summary>
        /// <param name="sqlConnection"></param>
        /// <param name="newIdentitySeedValue"></param>
        /// <param name="sqlTransaction"></param>
        /// <param name="tableNameOverride"></param>
        /// <returns></returns>
        public async Task ReSeedTableIdentityValueAsync(SqlConnection sqlConnection, long newIdentitySeedValue, SqlTransaction sqlTransaction = null, string tableNameOverride = null)
        {
            using (var sqlCmd = CreateReSeedTableIdentityValueSqlCommand(sqlConnection, newIdentitySeedValue, sqlTransaction, tableNameOverride))
            {
                await sqlCmd.ExecuteNonQueryAsync().ConfigureAwait(false);
            }
        }

        /// <summary>
        /// Sets / Re-seeds the Current Identity Value for the specified Table.
        /// </summary>
        /// <param name="sqlConnection"></param>
        /// <param name="newIdentitySeedValue"></param>
        /// <param name="sqlTransaction"></param>
        /// <param name="tableNameOverride"></param>
        /// <returns></returns>
        public void ReSeedTableIdentityValue(SqlConnection sqlConnection, long newIdentitySeedValue, SqlTransaction sqlTransaction = null, string tableNameOverride = null)
        {
            using (var sqlCmd = CreateReSeedTableIdentityValueSqlCommand(sqlConnection, newIdentitySeedValue, sqlTransaction, tableNameOverride))
            {
                sqlCmd.ExecuteNonQuery();
            }
        }

        private SqlCommand CreateReSeedTableIdentityValueSqlCommand(SqlConnection sqlConnection, long newIdentitySeedValue, SqlTransaction sqlTransaction = null, string tableNameOverride = null)
        {
            var fullyQualifiedTableName = GetMappedTableNameTerm(tableNameOverride).FullyQualifiedTableName;
            var sqlCmd = new SqlCommand("DBCC CHECKIDENT(@TableName, RESEED, @NewIdentitySeedValue);", sqlConnection, sqlTransaction);
            sqlCmd.CommandTimeout = BulkHelpersConfig.MaterializeDataStructureProcessingTimeoutSeconds;
            sqlCmd.Parameters.Add("@TableName", SqlDbType.NVarChar).Value = fullyQualifiedTableName;
            sqlCmd.Parameters.Add("@NewIdentitySeedValue", SqlDbType.BigInt).Value = newIdentitySeedValue;
            return sqlCmd;
        }

        /// <summary>
        /// Sets / Re-seeds the Current Identity Value with the current MAX() value of the Identity Column for the specified Table.
        /// </summary>
        /// <param name="sqlConnection"></param>
        /// <param name="sqlTransaction"></param>
        /// <param name="tableNameOverride"></param>
        /// <returns></returns>
        public async Task<long> ReSeedTableIdentityValueWithMaxIdAsync(SqlConnection sqlConnection, SqlTransaction sqlTransaction = null, string tableNameOverride = null)
        {
            var tableDef = await GetTableSchemaDefinitionInternalAsync(TableSchemaDetailLevel.BasicDetails, sqlConnection, sqlTransaction: sqlTransaction, tableNameOverride).ConfigureAwait(false);
            using (var sqlCmd = CreateReSeedTableIdentityValueToSyncWithMaxIdSqlCommand(sqlConnection, tableDef, sqlTransaction))
            {
                var result = await sqlCmd.ExecuteScalarAsync().ConfigureAwait(false);
                return Convert.ToInt64(result);
            }
        }

        /// <summary>
        /// Sets / Re-seeds the Current Identity Value with the current MAX() value of the Identity Column for the specified Table.
        /// </summary>
        /// <param name="sqlConnection"></param>
        /// <param name="sqlTransaction"></param>
        /// <param name="tableNameOverride"></param>
        /// <returns></returns>
        public long ReSeedTableIdentityWithMaxId(SqlConnection sqlConnection, SqlTransaction sqlTransaction = null, string tableNameOverride = null)
        {
            var tableDef = GetTableSchemaDefinitionInternal(TableSchemaDetailLevel.BasicDetails, sqlConnection, sqlTransaction: sqlTransaction, tableNameOverride);
            using (var sqlCmd = CreateReSeedTableIdentityValueToSyncWithMaxIdSqlCommand(sqlConnection, tableDef, sqlTransaction))
            {
                var result = sqlCmd.ExecuteScalar();
                return Convert.ToInt64(result);
            }
        }

        private SqlCommand CreateReSeedTableIdentityValueToSyncWithMaxIdSqlCommand(SqlConnection sqlConnection, SqlBulkHelpersTableDefinition tableDef, SqlTransaction sqlTransaction = null)
        {
            var tableNameTerm = tableDef.TableNameTerm;
            var maxIdVariable = $"@MaxId_{tableNameTerm.TableNameVariable}";

            var sqlCmd = new SqlCommand($@"
                DECLARE {maxIdVariable} BIGINT = (SELECT MAX({tableDef.IdentityColumn.ColumnName.QualifySqlTerm()}) FROM {tableNameTerm.FullyQualifiedTableName});
                DBCC CHECKIDENT(@TableName, RESEED, {maxIdVariable});
                SELECT CURRENT_IDENTITY_VALUE = IDENT_CURRENT(@TableName);
            ", sqlConnection, sqlTransaction);
            
            sqlCmd.CommandTimeout = BulkHelpersConfig.MaterializeDataStructureProcessingTimeoutSeconds;
            sqlCmd.Parameters.Add("@TableName", SqlDbType.NVarChar).Value = tableNameTerm.FullyQualifiedTableName;
            return sqlCmd;
        }

        #endregion

        #region Full Text Index API Methods

        /// <summary>
        /// Remove and Return the Details for the Full Text Index of specified mapped table model type.
        /// NOTE: THIS API is Unique in that this CANNOT be called within the context of a Transaction and therefore
        ///         must be executed on a Connection without a Transaction!!!
        /// </summary>
        /// <param name="sqlConnection"></param>
        /// <param name="tableNameOverride"></param>
        /// <returns></returns>
        public async Task<FullTextIndexDefinition> RemoveFullTextIndexAsync(
            SqlConnection sqlConnection,
            string tableNameOverride = null
        )
        {
            sqlConnection.AssertArgumentIsNotNull(nameof(sqlConnection));

            var tableSchemaDefinition = await this.GetTableSchemaDefinitionInternalAsync(
                TableSchemaDetailLevel.ExtendedDetails, 
                sqlConnection, 
                sqlTransaction: null, 
                tableNameOverride: tableNameOverride
            ).ConfigureAwait(false);

            if (tableSchemaDefinition.FullTextIndex != null)
            {
                var sqlScriptBuilder = MaterializedDataScriptBuilder
                    .NewSqlScript()
                    .DropFullTextIndex(tableSchemaDefinition.TableNameTerm);

                //Execute the Script!
                await sqlConnection
                    .ExecuteMaterializedDataSqlScriptAsync(sqlScriptBuilder, BulkHelpersConfig.MaterializeDataStructureProcessingTimeoutSeconds)
                    .ConfigureAwait(false);

                return tableSchemaDefinition.FullTextIndex;
            }

            return null;
        }

        /// <summary>
        /// Add the Full Text Index specified by the Definition to specified table.
        /// NOTE: THIS API is Unique in that this CANNOT be called within the context of a Transaction and therefore
        ///         must be executed on a Connection without a Transaction!!!
        /// NOTE: This is usually done when Materializing data into a table that has a Full Text Index, so the index must be removed and re-added outside
        ///         of the Transaction.
        /// </summary>
        /// <param name="sqlConnection"></param>
        /// <param name="fullTextIndex"></param>
        /// <param name="tableNameOverride"></param>
        /// <returns></returns>
        public async Task AddFullTextIndexAsync(
            SqlConnection sqlConnection,
            FullTextIndexDefinition fullTextIndex,
            string tableNameOverride = null
        )
        {
            sqlConnection.AssertArgumentIsNotNull(nameof(sqlConnection));
            fullTextIndex.AssertArgumentIsNotNull(nameof(fullTextIndex));

            var tableNameTerm = GetMappedTableNameTerm(tableNameOverride);

            var sqlScriptBuilder = MaterializedDataScriptBuilder
                .NewSqlScript()
                .AddFullTextIndex(tableNameTerm, fullTextIndex);

            //Execute the Script!
            await sqlConnection
                .ExecuteMaterializedDataSqlScriptAsync(sqlScriptBuilder, BulkHelpersConfig.MaterializeDataStructureProcessingTimeoutSeconds)
                .ConfigureAwait(false);
        }

        #endregion
    }
}
