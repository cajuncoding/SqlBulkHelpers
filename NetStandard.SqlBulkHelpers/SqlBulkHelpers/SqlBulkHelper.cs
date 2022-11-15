using System;
using System.Collections.Generic;
using System.Data;
using Microsoft.Data.SqlClient;
using System.Linq;
using System.Threading.Tasks;
using SqlBulkHelpers.SqlBulkHelpers.QueryProcessing;

namespace SqlBulkHelpers
{
    public class SqlBulkHelper<T> : BaseSqlBulkHelper<T>, ISqlBulkHelper<T> where T : class
    {
        #region Constructors

        /// <inheritdoc/>
        public SqlBulkHelper(ISqlBulkHelpersDBSchemaLoader sqlDbSchemaLoader, int timeoutSeconds = DefaultBulkOperationTimeoutSeconds)
            : base(sqlDbSchemaLoader, timeoutSeconds)
        {
        }

        /// <inheritdoc/>
        public SqlBulkHelper(ISqlBulkHelpersConnectionProvider sqlBulkHelpersConnectionProvider, int timeoutSeconds = DefaultBulkOperationTimeoutSeconds)
            : base(sqlBulkHelpersConnectionProvider, timeoutSeconds)
        {
        }

        /// <inheritdoc/>
        public SqlBulkHelper(SqlTransaction sqlTransaction, int timeoutSeconds = DefaultBulkOperationTimeoutSeconds)
            : base(sqlTransaction, timeoutSeconds)
        {
        }

        /// <inheritdoc/>
        public SqlBulkHelper(SqlConnection sqlConnection, SqlTransaction sqlTransaction = null, int timeoutSeconds = DefaultBulkOperationTimeoutSeconds)
            : base(sqlConnection, sqlTransaction, timeoutSeconds)
        {
        }

        #endregion

        #region ISqlBulkHelper<T> implementations
        public virtual async Task<IEnumerable<T>> BulkInsertAsync(
            IEnumerable<T> entityList,
            SqlTransaction transaction,
            SqlMergeMatchQualifierExpression matchQualifierExpression = null
        )
        {
            return await BulkInsertOrUpdateInternalAsync(
                entityList,
                SqlBulkHelpersMergeAction.Insert,
                transaction,
                matchQualifierExpressionParam: matchQualifierExpression
            ).ConfigureAwait(false);
        }

        public virtual async Task<IEnumerable<T>> BulkInsertAsync(
            IEnumerable<T> entityList, 
            string tableName, 
            SqlTransaction transaction, 
            SqlMergeMatchQualifierExpression matchQualifierExpression = null
        )
        {
            return await BulkInsertOrUpdateInternalAsync(
                entityList,
                SqlBulkHelpersMergeAction.Insert,
                transaction,
                tableName: tableName,
                matchQualifierExpressionParam: matchQualifierExpression
            ).ConfigureAwait(false);
        }

        public virtual async Task<IEnumerable<T>> BulkUpdateAsync(
            IEnumerable<T> entityList,
            SqlTransaction transaction,
            SqlMergeMatchQualifierExpression matchQualifierExpression = null
        )
        {
            return await BulkInsertOrUpdateInternalAsync(
                entityList,
                SqlBulkHelpersMergeAction.Update,
                transaction,
                matchQualifierExpressionParam: matchQualifierExpression
            ).ConfigureAwait(false);
        }

        public virtual async Task<IEnumerable<T>> BulkUpdateAsync(
            IEnumerable<T> entityList,
            string tableName,
            SqlTransaction transaction,
            SqlMergeMatchQualifierExpression matchQualifierExpression = null
        )
        {
            return await BulkInsertOrUpdateInternalAsync(
                entityList,
                SqlBulkHelpersMergeAction.Update,
                transaction,
                tableName: tableName,
                matchQualifierExpressionParam: matchQualifierExpression
            ).ConfigureAwait(false);
        }

        public virtual async Task<IEnumerable<T>> BulkInsertOrUpdateAsync(
            IEnumerable<T> entityList,
            SqlTransaction transaction,
            SqlMergeMatchQualifierExpression matchQualifierExpression = null
        )
        {
            return await BulkInsertOrUpdateInternalAsync(
                entityList,
                SqlBulkHelpersMergeAction.InsertOrUpdate,
                transaction,
                matchQualifierExpressionParam: matchQualifierExpression
            ).ConfigureAwait(false);
        }

        public virtual async Task<IEnumerable<T>> BulkInsertOrUpdateAsync(
            IEnumerable<T> entityList,
            string tableName,
            SqlTransaction transaction,
            SqlMergeMatchQualifierExpression matchQualifierExpression = null
        )
        {
            return await BulkInsertOrUpdateInternalAsync(
                entityList, 
                SqlBulkHelpersMergeAction.InsertOrUpdate, 
                transaction,
                tableName: tableName,
                matchQualifierExpressionParam: matchQualifierExpression
            ).ConfigureAwait(false);
        }

        public virtual IEnumerable<T> BulkInsert(
            IEnumerable<T> entityList,
            SqlTransaction transaction,
            SqlMergeMatchQualifierExpression matchQualifierExpression = null
        )
        {
            return BulkInsertOrUpdateInternal(
                entityList,
                SqlBulkHelpersMergeAction.Insert,
                transaction,
                matchQualifierExpression: matchQualifierExpression
            );
        }


        public virtual IEnumerable<T> BulkInsert(
            IEnumerable<T> entityList,
            string tableName,
            SqlTransaction transaction,
            SqlMergeMatchQualifierExpression matchQualifierExpression = null
        )
        {
            return BulkInsertOrUpdateInternal(
                entityList,
                SqlBulkHelpersMergeAction.Insert,
                transaction,
                tableName: tableName,
                matchQualifierExpression: matchQualifierExpression
            );
        }

        public virtual IEnumerable<T> BulkUpdate(
            IEnumerable<T> entityList,
            SqlTransaction transaction,
            SqlMergeMatchQualifierExpression matchQualifierExpression = null
        )
        {
            return BulkInsertOrUpdateInternal(
                entityList,
                SqlBulkHelpersMergeAction.Update,
                transaction,
                matchQualifierExpression: matchQualifierExpression
            );
        }

        public virtual IEnumerable<T> BulkUpdate(
            IEnumerable<T> entityList,
            string tableName,
            SqlTransaction transaction,
            SqlMergeMatchQualifierExpression matchQualifierExpression = null
        )
        {
            return BulkInsertOrUpdateInternal(
                entityList,
                SqlBulkHelpersMergeAction.Update,
                transaction,
                tableName: tableName,
                matchQualifierExpression: matchQualifierExpression
            );
        }

        public virtual IEnumerable<T> BulkInsertOrUpdate(
            IEnumerable<T> entityList,
            SqlTransaction transaction,
            SqlMergeMatchQualifierExpression matchQualifierExpression = null
        )
        {
            return BulkInsertOrUpdateInternal(
                entityList,
                SqlBulkHelpersMergeAction.InsertOrUpdate,
                transaction,
                matchQualifierExpression: matchQualifierExpression
            );
        }

        public virtual IEnumerable<T> BulkInsertOrUpdate(
            IEnumerable<T> entityList,
            string tableName,
            SqlTransaction transaction,
            SqlMergeMatchQualifierExpression matchQualifierExpression = null
        )
        {
            return BulkInsertOrUpdateInternal(
                entityList, 
                SqlBulkHelpersMergeAction.InsertOrUpdate, 
                transaction,
                tableName: tableName,
                matchQualifierExpression: matchQualifierExpression
            );
        }
        #endregion

        #region Processing Methods (that do the real work)

        /// <summary>
        /// BBernard
        /// This is the Primary Async method that supports Insert, Update, and InsertOrUpdate via the flexibility of the Sql MERGE query!
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="entities"></param>
        /// <param name="tableName"></param>
        /// <param name="mergeAction"></param>
        /// <param name="transaction"></param>
        /// <param name="matchQualifierExpressionParam"></param>
        /// <returns></returns>
        protected virtual async Task<IEnumerable<T>> BulkInsertOrUpdateInternalAsync(
            IEnumerable<T> entities, 
            SqlBulkHelpersMergeAction mergeAction, 
            SqlTransaction transaction,
            string tableName = null, //Optional / Nullable
            SqlMergeMatchQualifierExpression matchQualifierExpressionParam = null
        )
        {
            //For Performance we ensure the entities are only ever enumerated One Time!
            var entityList = entities.ToList();
            var bulkOperationTimeoutSeconds = this.BulkOperationTimeoutSeconds;

            using (ProcessHelper processHelper = this.CreateProcessHelper(
                entityList, mergeAction, transaction, bulkOperationTimeoutSeconds, tableName, matchQualifierExpressionParam)
            ) {
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
                sqlBulkCopy.DestinationTableName = $"[{sqlScripts.TempStagingTableName}]";
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
                using (SqlDataReader sqlReader = await sqlCmd.ExecuteReaderAsync().ConfigureAwait(false))
                {
                    while (await sqlReader.ReadAsync().ConfigureAwait(false))
                    {
                        //So far all calls to SqlDataReader have been asynchronous, but since the data reader is in 
                        //non -sequential mode and ReadAsync was used, the column data should be read synchronously.
                        var mergeResult = ReadCurrentMergeResultHelper(sqlReader);
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
        /// <param name="tableName"></param>
        /// <param name="mergeAction"></param>
        /// <param name="transaction"></param>
        /// <param name="matchQualifierExpression"></param>
        /// <returns></returns>
        protected virtual IEnumerable<T> BulkInsertOrUpdateInternal(
            IEnumerable<T> entities, 
            SqlBulkHelpersMergeAction mergeAction, 
            SqlTransaction transaction,
            string tableName = null,   //Optional/Nullable! 
            SqlMergeMatchQualifierExpression matchQualifierExpression = null
        )
        {
            //For Performance we ensure the entities are only ever enumerated One Time!
            var entityList = entities.ToList();
            var bulkOperationTimeoutSeconds = this.BulkOperationTimeoutSeconds;

            using (ProcessHelper processHelper = this.CreateProcessHelper(
                entityList, mergeAction, transaction, bulkOperationTimeoutSeconds, tableName, matchQualifierExpression))
            {
                var sqlCmd = processHelper.SqlCommand;
                var sqlBulkCopy = processHelper.SqlBulkCopy;
                var sqlScripts = processHelper.SqlMergeScripts;

                //***STEP #4: Create Tables for Buffering Data & Storing Output values
                sqlCmd.CommandText = sqlScripts.SqlScriptToInitializeTempTables;
                sqlCmd.ExecuteNonQuery();

                //***STEP #5: Write Data to the Staging/Buffer Table as fast as possible!
                sqlBulkCopy.DestinationTableName = $"[{sqlScripts.TempStagingTableName}]";
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
                        var mergeResult = ReadCurrentMergeResultHelper(sqlReader);
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

        protected virtual MergeResult ReadCurrentMergeResultHelper(SqlDataReader sqlReader)
        {
            //So-far all of the calls to SqlDataReader have been asynchronous, but since the data reader is in 
            //non-sequential mode and ReadAsync was used, the column data should be read synchronously.
            var mergeResult = new MergeResult()
            {
                RowNumber = sqlReader.GetInt32(0),
                IdentityId = sqlReader.GetInt32(1),
                //MergeAction = SqlBulkHelpersMerge.ParseMergeActionString(sqlReader.GetString(2))
            };
            return mergeResult;
        }

        /// <summary>
        /// BBernard - Private process helper to wrap up and encapsulate the initialization logic that is shared across both Async and Sync methods...
        /// </summary>
        /// <param name="entityData"></param>
        /// <param name="mergeAction"></param>
        /// <param name="transaction"></param>
        /// <param name="timeoutSeconds"></param>
        /// <param name="tableNameOverride"></param>
        /// <param name="matchQualifierExpressionParam"></param>
        /// <param name="enableSqlBulkCopyTableLock"></param>
        /// <returns></returns>
        protected virtual ProcessHelper CreateProcessHelper(
            List<T> entityData, 
            SqlBulkHelpersMergeAction mergeAction, 
            SqlTransaction transaction,
            int timeoutSeconds,
            string tableNameOverride = null,
            SqlMergeMatchQualifierExpression matchQualifierExpressionParam = null,
            bool enableSqlBulkCopyTableLock = false
        )
        {
            //***STEP #1: Get the Processing Definition (cached after initial Load)!!!
            var processingDefinition = SqlBulkHelpersProcessingDefinition.GetProcessingDefinition<T>();
            
            //***STEP #2: Load the Table Schema Definitions (cached after initial Load)!!!
            //BBernard
            //NOTE: Prevent SqlInjection - by validating that the TableName must be a valid value (as retrieved from the DB Schema) 
            //      we eliminate risk of Sql Injection.
            //NOTE: All other parameters are Strongly typed (vs raw Strings) thus eliminating risk of Sql Injection
            var tableName = !string.IsNullOrWhiteSpace(tableNameOverride) ? tableNameOverride : processingDefinition.MappedDbTableName;
            var tableDefinition = this.GetTableSchemaDefinition(tableName);

            //***STEP #3: Build all of the Sql Scripts needed to Process the entities based on the specified Table definition.
            var sqlScripts = this.BuildSqlMergeScriptsHelper(
                tableDefinition, 
                mergeAction,
                //NOTE: We use the parameter argument for Match Qualifier if specified, otherwise we fall-back to to use what may
                //      have been configured on the Entity model via SqlMatchQualifier property attributes.
                matchQualifierExpressionParam ?? processingDefinition.MergeMatchQualifierExpressionFromEntityModel
            );

            //***STEP #4: Dynamically Initialize the Bulk Copy Helper using our Table data and table Definition!
            var sqlBulkCopyHelper = this.CreateSqlBulkCopyHelper(entityData, tableDefinition, processingDefinition, transaction, timeoutSeconds, enableSqlBulkCopyTableLock);

            return new ProcessHelper()
            {
                TableDefinition = tableDefinition,
                ProcessingDefinition = processingDefinition,
                EntityData = entityData,
                SqlMergeScripts = sqlScripts,
                SqlCommand = new SqlCommand(String.Empty, transaction.Connection, transaction)
                {
                    CommandTimeout = timeoutSeconds
                },
                SqlBulkCopy = sqlBulkCopyHelper
            };
        }

        /// <summary>
        /// BBernard - Private process helper to contain and encapsulate the initialization logic that is shared across both Asycn and Sync methods...
        /// </summary>
        protected class ProcessHelper : IDisposable
        {
            public SqlBulkHelpersTableDefinition TableDefinition { get; set; }
            public SqlBulkHelpersProcessingDefinition ProcessingDefinition { get; set; }
            public List<T> EntityData { get; set; }
            public SqlMergeScriptResults SqlMergeScripts { get; set; }
            public SqlCommand SqlCommand { get; set; }
            public SqlBulkCopy SqlBulkCopy { get; set; }

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
