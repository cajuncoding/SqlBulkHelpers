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
        )
        {
            var sourceTable = TableNameTerm.From<T>(sourceTableName);
            var targetTable = TableNameTerm.From<T>(targetTableName);
            //NOTE: If the Target Table was not specified and the Schemas are still he same we have to Target a different schema
            //  so we use the globally configured default Loading Schema...
            if (targetTable.SchemaName == sourceTable.SchemaName)
            {
                targetTable = new TableNameTerm(BulkHelpersConfig.MaterializedDataDefaultLoadingSchema, targetTable.TableName);
            }

            var sourceTableSchemaDefinition = SqlBulkHelpersSchemaLoaderCache
                .GetSchemaLoader(sqlTransaction.Connection.ConnectionString)
                ?.GetTableSchemaDefinition(sourceTable.FullyQualifiedTableName, sqlTransaction);

            if (sourceTableSchemaDefinition == null)
                throw new ArgumentException($"Could not resolve the source table schema for {sourceTable.FullyQualifiedTableName} on the provided connection.");


            var cloneTableStructureSql = MaterializedDataScriptBuilder
                .NewScript()
                .CloneTableWithAllElements(sourceTableSchemaDefinition, targetTable, recreateIfExists ? IfExists.Recreate : IfExists.StopProcessingWithException)
                .BuildSqlScript();

            using (var sqlCmd = new SqlCommand(cloneTableStructureSql, sqlTransaction.Connection, sqlTransaction))
            {
                sqlCmd.CommandTimeout = BulkHelpersConfig.MaterializeDataStructureProcessingTimeoutSeconds;

                using (var sqlReader = await sqlCmd.ExecuteReaderAsync())
                {
                    bool isSuccessful = false;
                    if ((await sqlReader.ReadAsync()) && sqlReader.FieldCount >= 1 && sqlReader.GetFieldType(0) == typeof(bool))
                    {
                        isSuccessful = await sqlReader.GetFieldValueAsync<bool>(0);
                        if (!isSuccessful && sqlReader.FieldCount >= 2 && sqlReader.GetFieldType(1) == typeof(string))
                        {
                            var errorMessage = await sqlReader.GetFieldValueAsync<string>(1);
                            throw new InvalidOperationException(errorMessage);
                        }
                    }

                    //This pretty-much will never happen as SQL Server will likely raise it's own exceptions/errors;
                    //  but at least if it does we cancel the process and raise an exception...
                    if (!isSuccessful)
                        throw new InvalidOperationException("An unknown error occurred while executing the SQL Script.");
                }
            }

            return new CloneTableInfo(sourceTable, targetTable);
        }

        public async Task<TableNameTerm[]> DropTablesAsync(SqlTransaction sqlTransaction, params string[] tableNames)
        {
            if (!tableNames.HasAny())
                return Array.Empty<TableNameTerm>();

            var tableNameTermsList = tableNames.Distinct().Select(TableNameTerm.From).ToList();
            var dropTableSqlScriptBuilder = MaterializedDataScriptBuilder.NewScript();

            foreach (var tableNameTerm in tableNameTermsList)
                dropTableSqlScriptBuilder.DropTable(tableNameTerm);

            var dropTablesSql = dropTableSqlScriptBuilder.BuildSqlScript();
            using (var sqlCmd = new SqlCommand(dropTablesSql, sqlTransaction.Connection, sqlTransaction))
            {
                sqlCmd.CommandTimeout = BulkHelpersConfig.MaterializeDataStructureProcessingTimeoutSeconds;
                await sqlCmd.ExecuteNonQueryAsync();
            }

            return tableNameTermsList.ToArray();
        }
    }
}
