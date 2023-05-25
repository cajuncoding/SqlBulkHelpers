using System;
using System.Collections.Generic;
using Microsoft.Data.SqlClient;
using SqlBulkHelpers.CustomExtensions;

namespace SqlBulkHelpers
{
    public class SqlBulkCopyFactory
    {
        protected ISqlBulkHelpersConfig SqlBulkConfig { get; set; }

        public SqlBulkCopyFactory(ISqlBulkHelpersConfig bulkHelpersConfig = null)
        {
            SqlBulkConfig = bulkHelpersConfig ?? SqlBulkHelpersConfig.DefaultConfig;
        }

        public virtual SqlBulkCopy CreateSqlBulkCopy<T>(
            List<T> entities, 
            SqlBulkHelpersProcessingDefinition processingDefinition, 
            SqlBulkHelpersTableDefinition tableDefinition, 
            SqlTransaction transaction
        )
        {
            entities.AssertArgumentIsNotNull(nameof(entities));
            processingDefinition.AssertArgumentIsNotNull(nameof(processingDefinition));
            tableDefinition.AssertArgumentIsNotNull(nameof(tableDefinition));

            var sqlBulk = new SqlBulkCopy(transaction.Connection, SqlBulkConfig.SqlBulkCopyOptions, transaction)
            {
                //Always initialize a Batch size & Timeout
                BatchSize = SqlBulkConfig.SqlBulkBatchSize, 
                BulkCopyTimeout = SqlBulkConfig.SqlBulkPerBatchTimeoutSeconds,
            };

            //First initialize the Column Mappings for the SqlBulkCopy
            //NOTE: BBernard - We only map valid columns that exist in both the Model & the Table Schema!
            //NOTE: BBernard - We Map All valid columns (including Identity Key column) to support Insert or Updates!
            foreach (var fieldDefinition in processingDefinition.PropertyDefinitions)
            {
                var dbColumnDef = tableDefinition.FindColumnCaseInsensitive(fieldDefinition.MappedDbColumnName);
                if (dbColumnDef != null)
                    sqlBulk.ColumnMappings.Add(fieldDefinition.MappedDbColumnName, dbColumnDef.ColumnName);
            }

            //BBernard
            //Now that we know we have only valid columns from the ProcessingDefinition, we must manually add a mapping
            //      for the Row Number Column for Bulk Loading, but Only if appropriate...
            if (processingDefinition.IsRowNumberColumnNameEnabled)
            {
                sqlBulk.ColumnMappings.Add(SqlBulkHelpersConstants.ROWNUMBER_COLUMN_NAME, SqlBulkHelpersConstants.ROWNUMBER_COLUMN_NAME);
            }

            return sqlBulk;
        }

    }
}
