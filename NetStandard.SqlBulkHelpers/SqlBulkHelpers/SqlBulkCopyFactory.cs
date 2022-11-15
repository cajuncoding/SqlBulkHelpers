﻿using System;
using System.Collections.Generic;
using Microsoft.Data.SqlClient;

namespace SqlBulkHelpers
{
    public class SqlBulkCopyFactory
    {
        public virtual int BulkCopyBatchSize { get; set; } = 2000; //General guidance is that 2000-5000 is efficient enough.
        public virtual int BulkCopyPerBatchTimeoutSeconds { get; set; } = SqlBulkHelpersConstants.DefaultBulkOperationPerBatchTimeoutSeconds; //Microsoft's Default is only 30 seconds, but we intentionally wait a bit longer in case it is needed.

        public virtual SqlBulkCopyOptions BulkCopyOptions { get; set; } = SqlBulkCopyOptions.Default;

        public virtual SqlBulkCopy CreateSqlBulkCopy<T>(List<T> entities, SqlBulkHelpersProcessingDefinition processingDefinition, SqlBulkHelpersTableDefinition tableDefinition, SqlTransaction transaction)
        {
            entities.AssertArgumentIsNotNull(nameof(entities));
            processingDefinition.AssertArgumentIsNotNull(nameof(processingDefinition));
            tableDefinition.AssertArgumentIsNotNull(nameof(tableDefinition));

            var sqlBulk = new SqlBulkCopy(transaction.Connection, this.BulkCopyOptions, transaction)
            {
                //Always initialize a Batch size & Timeout
                BatchSize = this.BulkCopyBatchSize, 
                BulkCopyTimeout = this.BulkCopyPerBatchTimeoutSeconds,
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
