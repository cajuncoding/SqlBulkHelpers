using System.Data;
using System.Data.SqlClient;
using System.Linq;

namespace SqlBulkHelpers
{
    public class SqlBulkCopyFactory
    {
        public virtual int BulkCopyBatchSize { get; set; } = 2000; //General guidance is that 2000-5000 is efficient enough.
        public virtual int BulkCopyTimeoutSeconds { get; set; } = 60; //Default is only 30 seconds, but we can wait a bit longer if needed.

        public virtual SqlBulkCopyOptions BulkCopyOptions { get; set; } = SqlBulkCopyOptions.Default;

        public virtual SqlBulkCopy CreateSqlBulkCopy(DataTable dataTable, SqlBulkHelpersTableDefinition tableDefinition, SqlTransaction transaction)
        {
            var sqlBulk = new SqlBulkCopy(transaction.Connection, this.BulkCopyOptions, transaction)
            {
                //Always initialize a Batch size & Timeout
                BatchSize = this.BulkCopyBatchSize, 
                BulkCopyTimeout = this.BulkCopyTimeoutSeconds
            };

            //First initialize the Column Mappings for the SqlBulkCopy
            //NOTE: BBernard - We only map valid columns that exist in both the Model & the Table Schema!
            //NOTE: BBernard - We Map All valid columns (including Identity Key column) to support Insert or Updates!
            var dataTableColumnNames = dataTable.Columns.Cast<DataColumn>().Select(c => c.ColumnName);
            foreach (var dataTableColumnName in dataTableColumnNames)
            {
                var dbColumnDef = tableDefinition.FindColumnCaseInsensitive(dataTableColumnName);
                if (dbColumnDef != null)
                {
                    sqlBulk.ColumnMappings.Add(dataTableColumnName, dbColumnDef.ColumnName);
                }
            }

            //BBernard
            //Now that we know we have only valid columns from the Model/DataTable, we must manually add a mapping
            //      for the Row Number Column for Bulk Loading . . . but Only if the data table has a RowNumber column defined.
            if (dataTable.Columns.Contains(SqlBulkHelpersConstants.ROWNUMBER_COLUMN_NAME))
            {
                sqlBulk.ColumnMappings.Add(SqlBulkHelpersConstants.ROWNUMBER_COLUMN_NAME, SqlBulkHelpersConstants.ROWNUMBER_COLUMN_NAME);
            }

            return sqlBulk;
        }

    }
}
