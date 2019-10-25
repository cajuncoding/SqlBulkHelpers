using System;
using System.Linq;

namespace SqlBulkHelpers
{
    public class SqlBulkHelpersMergeScriptBuilder
    {
        public SqlMergeScriptResults BuildSqlMergeScripts(SqlBulkHelpersTableDefinition tableDefinition, SqlBulkHelpersMergeAction mergeAction)
        {
            //NOTE: BBernard - This temp table name MUST begin with 1 (and only 1) hash "#" to ensure it is a Transaction Scoped table!
            var tempStagingTableName = $"#SqlBulkHelpers_STAGING_TABLE_{Guid.NewGuid()}";
            var tempOutputIdentityTableName = $"#SqlBulkHelpers_OUTPUT_IDENTITY_TABLE_{Guid.NewGuid()}";
            var identityColumnName = tableDefinition.IdentityColumn?.ColumnName ?? String.Empty;

            var columnNamesListWithoutIdentity = tableDefinition.GetColumnNames(false);
            var columnNamesWithoutIdentityCSV = columnNamesListWithoutIdentity.Select(c => $"[{c}]").ToCSV();

            //Initialize/Create the Staging Table!
            String sqlScriptToInitializeTempTables = $@"
                SELECT TOP(0)
                    -1 as [{identityColumnName}],
                    {columnNamesWithoutIdentityCSV},
                    -1 as [{SqlBulkHelpersConstants.ROWNUMBER_COLUMN_NAME}] 
                INTO [{tempStagingTableName}] 
                FROM [{tableDefinition.TableName}];

                ALTER TABLE [{tempStagingTableName}] ADD PRIMARY KEY ([{identityColumnName}]);

                SELECT TOP(0)
                    CAST('' AS nvarchar(10)) as [MERGE_ACTION],
                    CAST(-1 AS int) as [IDENTITY_ID], 
                    CAST(-1 AS int) [{SqlBulkHelpersConstants.ROWNUMBER_COLUMN_NAME}] 
                INTO [{tempOutputIdentityTableName}];
            ";


            //NOTE: This is ALL now completed very efficiently on the Sql Server Database side with
            //          NO unnecessary round trips to the Database!
            var mergeInsertSql = String.Empty;
            if (mergeAction.HasFlag(SqlBulkHelpersMergeAction.Insert))
            {
                mergeInsertSql = $@"
                    WHEN NOT MATCHED BY TARGET THEN
                        INSERT ({columnNamesWithoutIdentityCSV}) 
                        VALUES ({columnNamesListWithoutIdentity.Select(c => $"source.[{c}]").ToCSV()})
                ";
            }

            var mergeUpdateSql = String.Empty;
            if (mergeAction.HasFlag(SqlBulkHelpersMergeAction.Update))
            {
                mergeUpdateSql = $@"
                    WHEN MATCHED THEN
                        UPDATE SET {columnNamesListWithoutIdentity.Select(c => $"target.[{c}] = source.[{c}]").ToCSV()} 
                ";
            }

            //Build the FULL Dynamic Merge Script here...
            //BBernard - 2019-08-07
            //NOTE: We now sort on the RowNumber column that we define; this FIXES issue with SqlBulkCopy.WriteToServer()
            //      where the order of data being written is NOT guaranteed, and there is still no support for the ORDER() hint.
            //      In general it results in inverting the order of data being sent in Bulk which then resulted in Identity
            //      values being incorrect based on the order of data specified.
            String sqlScriptToExecuteMergeProcess = $@"
                MERGE [{tableDefinition.TableName}] as target
				USING (
					SELECT TOP 100 PERCENT * 
					FROM [{tempStagingTableName}] 
					ORDER BY [{SqlBulkHelpersConstants.ROWNUMBER_COLUMN_NAME}] ASC
				) as source
                ON target.[{identityColumnName}] = source.[{identityColumnName}]
                {mergeUpdateSql}
                {mergeInsertSql}
                OUTPUT $action, INSERTED.[{identityColumnName}], source.[{SqlBulkHelpersConstants.ROWNUMBER_COLUMN_NAME}]
                    INTO [{tempOutputIdentityTableName}] ([MERGE_ACTION], [IDENTITY_ID], [{SqlBulkHelpersConstants.ROWNUMBER_COLUMN_NAME}]);

                SELECT 
                    [{SqlBulkHelpersConstants.ROWNUMBER_COLUMN_NAME}], 
                    [IDENTITY_ID], 
                    [MERGE_ACTION]
                FROM [{tempOutputIdentityTableName}]
                ORDER BY [{SqlBulkHelpersConstants.ROWNUMBER_COLUMN_NAME}] ASC;

                DROP TABLE [{tempStagingTableName}];
                DROP TABLE [{tempOutputIdentityTableName}];
            ";

            return new SqlMergeScriptResults(
                tempStagingTableName,
                tempOutputIdentityTableName,
                sqlScriptToInitializeTempTables,
                sqlScriptToExecuteMergeProcess
            );
        }
    }

    public class SqlMergeScriptResults
    {
        public SqlMergeScriptResults(String tempStagingTableName, String tempOutputTableName, String tempTableScript, String mergeProcessScript)
        {
            this.SqlScriptToInitializeTempTables = tempTableScript;
            this.SqlScriptToExecuteMergeProcess = mergeProcessScript;
            this.TempStagingTableName = tempStagingTableName;
            this.TempOutputTableName = tempOutputTableName;
        }

        public String TempOutputTableName { get; private set; }
        public String TempStagingTableName { get; private set; }
        public String SqlScriptToInitializeTempTables { get; private set; }
        public String SqlScriptToExecuteMergeProcess { get; private set; }
    }
}
