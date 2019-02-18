using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Data;
using System.Data.Sql;
using System.Data.SqlClient;
using System.Diagnostics;

namespace SqlBulkHelpers
{
    //TODO: REMOVE WHEN NOT NEEDED ANYMORE AFTER ENHANCING TO DYNAMICALLY RESOLVE THE CORRECT PROPERTY FOR THE Identity value!
    public class BaseIdentityIdModel
    {
        public int Id { get; set; }
    }

    public class SqlBulkIdentityHelper<T> : ISqlBulkHelper<T> where T : BaseIdentityIdModel
    {
        public async Task<List<T>> BulkInsertAsync(List<T> entityList, string tableName, SqlTransaction transaction)
        {
            return await BulkInsertOrUpdateWithIdentityColumnAsync(entityList, tableName, SqlBulkHelpersMergeAction.Insert, transaction);
        }

        public async Task<List<T>> BulkUpdateAsync(List<T> entityList, string tableName, SqlTransaction transaction)
        {
            return await BulkInsertOrUpdateWithIdentityColumnAsync(entityList, tableName, SqlBulkHelpersMergeAction.Update, transaction);
        }

        public async Task<List<T>> BulkInsertOrUpdateAsync(List<T> entityList, string tableName, SqlTransaction transaction)
        {
            return await BulkInsertOrUpdateWithIdentityColumnAsync(entityList, tableName, SqlBulkHelpersMergeAction.InsertOrUpdate, transaction);
        }

        /// <summary>
        /// BBernard
        /// This is the Primary method that supports Insert, Update, and InsertOrUpdate via the flexibility of the Sql MERGE query!
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="entityList"></param>
        /// <param name="tableName"></param>
        /// <param name="mergeAction"></param>
        /// <param name="transaction"></param>
        /// <returns></returns>
        private async Task<List<T>> BulkInsertOrUpdateWithIdentityColumnAsync(List<T> entityList, String tableName, SqlBulkHelpersMergeAction mergeAction, SqlTransaction transaction)
        {
            //***************************************************************************************
            //STEP #1: Load the Table Schema Definitions (cached after initial Load)!!!
            //***************************************************************************************
            //BBernard
            //NOTE: Prevent SqlInjection - by validating that the TableName must be a valid value (as retrieved from the DB Schema) 
            //      we eliminate risk of Sql Injection.
            var tableDefinition = SqlBulkHelpersDBSchemaLoader.GetTableSchemaDefinition(tableName);
            if (tableDefinition == null) throw new ArgumentOutOfRangeException(nameof(tableName), $"The specified argument [{tableName}] is invalid.");

            //FIRST Dynamically Convert All Entities to a DataTable for consumption by the SqlBulkCopy class...
            var SqlBulkHelpersMapper = new SqlBulkHelpersObjectMapper();
            var dataTable = SqlBulkHelpersMapper.ConvertEntitiesToDatatable(entityList);

            using (SqlCommand sqlCmd = new SqlCommand(String.Empty, transaction.Connection, transaction))
            using (SqlBulkCopy sqlBulk = new SqlBulkCopy(transaction.Connection, SqlBulkCopyOptions.Default, transaction))
            {
                var timer = Stopwatch.StartNew();

                //Always initialize a Batch size & Timeout
                sqlBulk.BatchSize = 1000; //General guidance is that 1000-5000 is efficient enough.
                sqlBulk.BulkCopyTimeout = 60; //Default is only 30 seconds, but we can wait a bit longer if needed.

                //First initilize the Column Mappings for the SqlBulkCopy
                //NOTE: BBernard - We EXCLUDE the Identity Column so that it is handled Completely by Sql Server!
                var dataTableColumnNames = dataTable.Columns.Cast<DataColumn>().Select(c => c.ColumnName);
                foreach (var dataTableColumnName in dataTableColumnNames)
                {
                    var dbColumnDef = tableDefinition.FindColumnCaseInsensitive(dataTableColumnName);
                    if (dbColumnDef != null)
                    {
                        sqlBulk.ColumnMappings.Add(dataTableColumnName, dbColumnDef.ColumnName);
                    }
                }

                //Now that we konw we have only valid columns from the Model/DataTable, we must manually add a mapping
                //      for the Row Number Column for Bulk Loading . . .
                sqlBulk.ColumnMappings.Add(SqlBulkHelpersConstants.ROWNUMBER_COLUMN_NAME, SqlBulkHelpersConstants.ROWNUMBER_COLUMN_NAME);

                //***************************************************************************************
                //STEP #1: Create Tables for Buffering Data & Storing Output values
                //***************************************************************************************
                //NOTE: BBernard - This temp table name MUST begin with 1 (and only 1) hash "#" to ensure it is a Transaction Scoped table!
                var tempStagingTableName = $"#SqlBulkHelpers_STAGING_TABLE_{Guid.NewGuid()}";
                var tempOutputIdentityTableName = $"#SqlBulkHelpers_OUTPUT_IDENTITY_TABLE_{Guid.NewGuid()}";
                var identityColumnName = tableDefinition.IdentityColumn?.ColumnName ?? String.Empty;

                var columnNamesListWithoutIdentity = tableDefinition.GetColumnNames(false);
                var columnNamesWithoutIdentityCSV = columnNamesListWithoutIdentity.Select(c => $"[{c}]").ToCSV();

                //Initialize/Create the Staging Table!
                sqlCmd.CommandText = $@"
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
                await sqlCmd.ExecuteNonQueryAsync();

                //***************************************************************************************
                //STEP #2: Write Data to the Staging/Buffer Table as fast as possible!
                //***************************************************************************************
                sqlBulk.DestinationTableName = $"[{tempStagingTableName}]";
                await sqlBulk.WriteToServerAsync(dataTable);

                //***************************************************************************************
                //STEP #3: Merge Data from the Staging Table into the Real Table
                //          and simultaneously Ouptut Identity Id values into Output Temp Table!
                //***************************************************************************************
                //NOTE: This is ALL now completed very efficiently on the Sql Server Database side with
                //          NO unnecessary round trips to the Datbase!
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

                sqlCmd.CommandText = $@"
                    MERGE [{tableDefinition.TableName}] as target
                    USING [{tempStagingTableName}] as source
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

                //***************************************************************************************
                //STEP #4: Now READ All Identity ID values (mapped from the MERGE Insert or Update)
                //***************************************************************************************
                List<MergeResult> mergeResultsList;
                using (SqlDataReader sqlReader = await sqlCmd.ExecuteReaderAsync())
                {
                    mergeResultsList = await ReadMergeResults(sqlReader);
                }

                //***************************************************************************************
                //STEP #5: FINALLY Update all of the original Entities with INSERTED/New Identity Values
                //***************************************************************************************
                foreach (var mergeResult in mergeResultsList.Where(r => r.MergeAction.HasFlag(SqlBulkHelpersMergeAction.Insert)))
                {
                    //NOTE: List is 0 (zero) based, but our RowNumber is 1 (one) based.
                    var entity = entityList[mergeResult.RowNumber - 1];
                    entity.Id = mergeResult.IdentityId;
                }

                System.Diagnostics.Debug.WriteLine($"Bulk Process Completed in [{timer.ElapsedMilliseconds} ms]!", "SqlBulkHelpers");

                //FINALLY Return the updated Entities with the Identity Id if it was Inserted!
                return entityList;
            }
        }

        //NOTE: This is Private Class because it is ONLY needed by the SqlBulkHelper implementations Merge Operation for organized code when post-processing results.
        private class MergeResult
        {
            public int RowNumber { get; set; }
            public int IdentityId { get; set; }
            public SqlBulkHelpersMergeAction MergeAction { get; set; }
        }

        private SqlBulkHelpersMergeAction ParseMergeActionString(String actionString)
        {
            SqlBulkHelpersMergeAction mergeAction;
            Enum.TryParse<SqlBulkHelpersMergeAction>(actionString, true, out mergeAction);
            return mergeAction;
        }

        private async Task<List<MergeResult>> ReadMergeResults(SqlDataReader sqlReader)
        {
            var mergeResultsList = new List<MergeResult>();
            while (await sqlReader.ReadAsync())
            {
                //So far all calls to SqlDataReader have been asynchronous, but since the data reader is in 
                //non -sequential mode and ReadAsync was used, the column data should be read synchronously.
                var rowNumberMap = new MergeResult()
                {
                    RowNumber = sqlReader.GetInt32(0),
                    IdentityId = sqlReader.GetInt32(1),
                    MergeAction = ParseMergeActionString(sqlReader.GetString(2))
                };
                mergeResultsList.Add(rowNumberMap);
            }

            return mergeResultsList;
        }

    }
}
