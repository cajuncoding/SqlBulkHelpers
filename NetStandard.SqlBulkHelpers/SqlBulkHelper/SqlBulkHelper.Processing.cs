using System;
using System.Collections.Generic;
using System.Data;
using Microsoft.Data.SqlClient;
using System.Linq;
using System.Threading.Tasks;

namespace SqlBulkHelpers
{
    internal partial class SqlBulkHelper<T> : BaseSqlBulkHelper<T> where T : class
    {
        #region Processing Methods (that do the real work)

        /// <summary>
        /// BBernard
        /// This is the Primary Async method that supports Insert, Update, and InsertOrUpdate via the flexibility of the Sql MERGE query!
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="entities"></param>
        /// <param name="tableNameParam"></param>
        /// <param name="mergeAction"></param>
        /// <param name="sqlTransaction"></param>
        /// <param name="matchQualifierExpressionParam"></param>
        /// <param name="enableIdentityInsert"></param>
        /// <returns></returns>
        protected virtual async Task<IEnumerable<T>> BulkInsertOrUpdateInternalAsync(
            IEnumerable<T> entities, 
            SqlBulkHelpersMergeAction mergeAction, 
            SqlTransaction sqlTransaction,
            string tableNameParam = null, //Optional / Nullable
            SqlMergeMatchQualifierExpression matchQualifierExpressionParam = null,
            bool enableIdentityInsert = false
        )
        {
            //For Performance we ensure the entities are only ever enumerated One Time!
            var entityList = entities.ToList();

            var tableDefinition = await this.GetTableSchemaDefinitionInternalAsync(
                TableSchemaDetailLevel.BasicDetails, 
                sqlTransaction.Connection, 
                sqlTransaction: sqlTransaction, 
                tableNameOverride: tableNameParam
            );

            //Initialize our Disposable ProcessHelper...
            //This will handle STEP #1, #2, & #3 (as shared steps between Async and Sync implementations)...
            using (var processHelper = this.CreateProcessHelper(
                entityList,
                mergeAction,
                tableDefinition,
                sqlTransaction,
                tableNameParam,
                matchQualifierExpressionParam,
                enableIdentityInsert
            ))
            {
                var sqlCmd = processHelper.SqlCommand;
                var sqlBulkCopy = processHelper.SqlBulkCopy;
                var sqlScripts = processHelper.SqlMergeScripts;

                //***STEP #4: Create Tables for Buffering Data & Storing Output values
                //            NOTE: THIS Step is Unique for Async processing...
                sqlCmd.CommandText = sqlScripts.SqlScriptToInitializeTempTables;
                await sqlCmd.ExecuteNonQueryAsync().ConfigureAwait(false);

                //***STEP #5: Write Data to the Staging/Buffer Table as fast as possible!
                //            NOTE: THIS Step is Unique for Async processing...
                //NOTE: The DataReader must be properly Disposed!
                sqlBulkCopy.DestinationTableName = sqlScripts.TempStagingTableName.QualifySqlTerm();
                using (var entityDataReader = processHelper.CreateEntityDataReader())
                {
                    await sqlBulkCopy.WriteToServerAsync(entityDataReader).ConfigureAwait(false);
                }

                //***STEP #6: Merge Data from the Staging Table into the Real Table
                //            and simultaneously Output Identity Id values into Output Temp Table!
                //            NOTE: THIS Step is Unique for Async processing...
                sqlCmd.CommandText = sqlScripts.SqlScriptToExecuteMergeProcess;

                //Execute this script and load the results....
                var mergeResultsList = new List<MergeResult>();
                bool hasIdentityColumn = processHelper.TableDefinition.IdentityColumn != null;
                using (var sqlReader = await sqlCmd.ExecuteReaderAsync().ConfigureAwait(false))
                {
                    while (await sqlReader.ReadAsync().ConfigureAwait(false))
                    {
                        //So far all calls to SqlDataReader have been asynchronous, but since the data reader is in 
                        //non -sequential mode and ReadAsync was used, the column data should be read synchronously...
                        //Note: see this StackOverflow thread for more details: https://stackoverflow.com/a/19895322/7293142
                        var mergeResult = ReadCurrentMergeResultInternal(sqlReader, hasIdentityColumn);
                        mergeResultsList.Add(mergeResult);
                    }
                }

                //***STEP #7: FINALLY Update all of the original Entities with INSERTED/New or UPDATED Identity Values
                //NOTE: IF MULTIPLE NON-UNIQUE items are updated then ONLY ONE Identity value can be returned, though multiple
                //      other items may have in-reality actually been updated within the DB.  This is a likely scenario
                //      IF a different non-unique Match Qualifier Field is specified.
                var updatedEntityList = this.PostProcessEntitiesWithMergeResults(
                    entityList,
                    mergeResultsList,
                    processHelper.TableDefinition.IdentityColumn,
                    processHelper.SqlMergeScripts.SqlMatchQualifierExpression
                );

                //FINALLY Return the updated Entities with the Identity Id if it was Inserted!
                return updatedEntityList;
            }
        }

        /// <summary>
        /// BBernard
        /// This is the Primary Synchronous method that supports Insert, Update, and InsertOrUpdate via the flexibility of the Sql MERGE query!
        /// </summary>
        /// <param name="entities"></param>
        /// <param name="tableNameParam"></param>
        /// <param name="mergeAction"></param>
        /// <param name="sqlTransaction"></param>
        /// <param name="matchQualifierExpressionParam"></param>
        /// <param name="bulkHelpersConfig"></param>
        /// <param name="enableIdentityInsert"></param>
        /// <returns></returns>
        protected virtual IEnumerable<T> BulkInsertOrUpdateInternal(
            IEnumerable<T> entities, 
            SqlBulkHelpersMergeAction mergeAction, 
            SqlTransaction sqlTransaction,
            string tableNameParam = null,   //Optional/Nullable! 
            SqlMergeMatchQualifierExpression matchQualifierExpressionParam = null,
            ISqlBulkHelpersConfig bulkHelpersConfig = null,
            bool enableIdentityInsert = false
        )
        {
            //For Performance we ensure the entities are only ever enumerated One Time!
            var entityList = entities.ToList();
            var bulkOperationTimeoutSeconds = this.BulkHelpersConfig;

            var tableDefinition = this.GetTableSchemaDefinitionInternal(
                TableSchemaDetailLevel.BasicDetails, 
                sqlTransaction.Connection, 
                sqlTransaction: sqlTransaction, 
                tableNameOverride: tableNameParam
            );

            using (ProcessHelper processHelper = this.CreateProcessHelper(
                entityList, mergeAction, tableDefinition, sqlTransaction, tableNameParam, matchQualifierExpressionParam
            ))
            {
                var sqlCmd = processHelper.SqlCommand;
                var sqlBulkCopy = processHelper.SqlBulkCopy;
                var sqlScripts = processHelper.SqlMergeScripts;

                //***STEP #4: Create Tables for Buffering Data & Storing Output values
                sqlCmd.CommandText = sqlScripts.SqlScriptToInitializeTempTables;
                sqlCmd.ExecuteNonQuery();

                //***STEP #5: Write Data to the Staging/Buffer Table as fast as possible!
                sqlBulkCopy.DestinationTableName = sqlScripts.TempStagingTableName.QualifySqlTerm();
                using (var entityDataReader = processHelper.CreateEntityDataReader())
                {
                    sqlBulkCopy.WriteToServer(entityDataReader);
                }

                //***STEP #6: Merge Data from the Staging Table into the Real Table
                //          and simultaneously Output Identity Id values into Output Temp Table!
                sqlCmd.CommandText = sqlScripts.SqlScriptToExecuteMergeProcess;

                //Execute this script and load the results....
                var mergeResultsList = new List<MergeResult>();
                using (SqlDataReader sqlReader = sqlCmd.ExecuteReader())
                {
                    while (sqlReader.Read())
                    {
                        var mergeResult = ReadCurrentMergeResultInternal(sqlReader, processHelper.HasIdentityColumn);
                        mergeResultsList.Add(mergeResult);
                    }
                }

                //***STEP #7: FINALLY Update all of the original Entities with INSERTED/New Identity Values
                //NOTE: IF MULTIPLE NON-UNIQUE items are updated then ONLY ONE Identity value can be returned, though multiple
                //      other items may have in-reality actually been updated within the DB.  This is a likely scenario
                //      IF a different non-unique Match Qualifier Field is specified.
                var updatedEntityList = this.PostProcessEntitiesWithMergeResults(
                    entityList,
                    mergeResultsList,
                    processHelper.TableDefinition.IdentityColumn,
                    processHelper.SqlMergeScripts.SqlMatchQualifierExpression
                );

                //FINALLY Return the updated Entities with the Identity Id if it was Inserted!
                return updatedEntityList;
            }
        }

        protected virtual MergeResult ReadCurrentMergeResultInternal(SqlDataReader sqlReader,  bool hasIdentityColumn)
        {
            //So-far all of the calls to SqlDataReader have been asynchronous, but since the data reader is in 
            //non-sequential mode and ReadAsync was used, the column data should be read synchronously.
            var mergeResult = new MergeResult() { RowNumber = sqlReader.GetInt32(0) };

            if (hasIdentityColumn)
            {
                mergeResult.IdentityId = Convert.ToInt64(sqlReader.GetValue(1));
                //mergeResult.MergeAction = SqlBulkHelpersMerge.ParseMergeActionString(sqlReader.GetString(2))
            }
            else
            {
                mergeResult.IdentityId = -1;
                //mergeResult.MergeAction = SqlBulkHelpersMerge.ParseMergeActionString(sqlReader.GetString(1))
            }

            return mergeResult;
        }

        /// <summary>
        /// BBernard - Private process helper to wrap up and encapsulate the initialization logic that is shared across both Async and Sync methods...
        /// </summary>
        /// <param name="entityData"></param>
        /// <param name="mergeAction"></param>
        /// <param name="tableDefinition"></param>
        /// <param name="sqlTransaction"></param>
        /// <param name="tableNameOverride"></param>
        /// <param name="matchQualifierExpressionParam"></param>
        /// <param name="enableIdentityInsert"></param>
        /// <returns></returns>
        protected virtual ProcessHelper CreateProcessHelper(
            List<T> entityData, 
            SqlBulkHelpersMergeAction mergeAction,
            SqlBulkHelpersTableDefinition tableDefinition,
            SqlTransaction sqlTransaction,
            string tableNameOverride = null,
            SqlMergeMatchQualifierExpression matchQualifierExpressionParam = null,
            bool enableIdentityInsert = false
        )
        {
            //***STEP #1: Get the Table & Model Processing Definitions (cached after initial Load)!!!
            var processingDefinition = SqlBulkHelpersProcessingDefinition.GetProcessingDefinition<T>(tableDefinition.IdentityColumn);

            //***STEP #2: Build all of the Sql Scripts needed to Process the entities based on the specified Table definition.
            var sqlScripts = this.BuildSqlMergeScriptsInternal(
                tableDefinition,
                processingDefinition,
                mergeAction,
                matchQualifierExpressionParam,
                enableIdentityInsert
            );

            //***STEP #3: Dynamically Initialize the Bulk Copy Helper using our Table data and table Definition!
            var sqlBulkCopy = this.CreateSqlBulkCopyInternal(
                entityData, 
                tableDefinition, 
                processingDefinition, 
                sqlTransaction
            );

            return new ProcessHelper(
                tableDefinition,
                processingDefinition,
                entityData,
                sqlScripts,
                new SqlCommand(string.Empty, sqlTransaction.Connection, sqlTransaction)
                {
                    CommandTimeout = BulkHelpersConfig.SqlBulkPerBatchTimeoutSeconds
                },
                sqlBulkCopy
            );
        }

        /// <summary>
        /// BBernard - Private process helper to contain and encapsulate the initialization logic that is shared across both Asycn and Sync methods...
        /// </summary>
        protected class ProcessHelper : IDisposable
        {
            public ProcessHelper(
                SqlBulkHelpersTableDefinition tableDefinition, 
                SqlBulkHelpersProcessingDefinition processingDefinition, 
                List<T> entityData, 
                SqlMergeScriptResults sqlMergeScripts,
                SqlCommand sqlCommand,
                SqlBulkCopy sqlBulkCopy
            )
            {
                TableDefinition = tableDefinition;
                ProcessingDefinition = processingDefinition;
                EntityData = entityData;
                SqlMergeScripts = sqlMergeScripts;
                SqlCommand = sqlCommand;
                SqlBulkCopy = sqlBulkCopy;
            }

            public SqlBulkHelpersTableDefinition TableDefinition { get; }
            public bool HasIdentityColumn => TableDefinition.IdentityColumn != null;
            public SqlBulkHelpersProcessingDefinition ProcessingDefinition { get; }
            public List<T> EntityData { get; }
            public SqlMergeScriptResults SqlMergeScripts { get; }
            public SqlCommand SqlCommand { get; private set; }
            public SqlBulkCopy SqlBulkCopy { get; private set; }

            public IDataReader CreateEntityDataReader()
            {
                var entityDataReader = new SqlBulkHelpersDataReader<T>(this.EntityData, this.ProcessingDefinition, this.TableDefinition);
                return entityDataReader;
            }

            /// <summary>
            /// Implement IDisposable to ensure that we ALWAYS SAFELY CLEAN up our internal IDisposable resources.
            /// </summary>
            public void Dispose()
            {
                //Always clean up Sql Connection/Command objects as they can hold onto precious resources...
                (this.SqlCommand as IDisposable)?.Dispose();
                this.SqlCommand = null;

                //Always clean up SqlBulkCopy object (as it also can hold onto connections/commands)...
                (this.SqlBulkCopy as IDisposable)?.Dispose();
                this.SqlBulkCopy = null;
            }
        }

        #endregion
    }
}
