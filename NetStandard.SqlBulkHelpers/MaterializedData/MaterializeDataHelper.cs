using System;
using System.Text;
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

            var cloneTableStructureSql = MaterializedDataScriptBuilder
                .NewScript()
                .CloneTableStructure(sourceTable, targetTable, recreateIfExists)
                .BuildSqlScript();

            using (var sqlCmd = new SqlCommand(cloneTableStructureSql, sqlTransaction.Connection, sqlTransaction))
            {
                sqlCmd.CommandTimeout = BulkHelpersConfig.MaterializeDataStructureProcessingTimeoutSeconds;

                var isSuccessful = (bool)await sqlCmd.ExecuteScalarAsync();

                //This pretty-much will never happen as SQL Server will likely raise it's own exceptions/errors;
                //  but at least if it does we cancel the process and raise an exception...
                if (!isSuccessful)
                    throw new InvalidOperationException("An unknown error occurred while executing the SQL Script.");
            }

            return new CloneTableInfo(sourceTable, targetTable);
        }

    }
}
