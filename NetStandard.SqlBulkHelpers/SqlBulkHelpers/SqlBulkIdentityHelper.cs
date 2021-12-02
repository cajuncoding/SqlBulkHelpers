using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using System.Threading.Tasks;

namespace SqlBulkHelpers
{
    public class SqlBulkIdentityHelper<T> : BaseSqlBulkHelper<T>, ISqlBulkHelper<T> where T : class
    {
        #region Constructors

        /// <inheritdoc/>
        public SqlBulkIdentityHelper(ISqlBulkHelpersDBSchemaLoader sqlDbSchemaLoader, int timeoutSeconds = DefaultBulkOperationTimeoutSeconds)
            : base(sqlDbSchemaLoader, timeoutSeconds)
        {
        }

        /// <inheritdoc/>
        public SqlBulkIdentityHelper(ISqlBulkHelpersConnectionProvider sqlBulkHelpersConnectionProvider, int timeoutSeconds = DefaultBulkOperationTimeoutSeconds)
            : base(sqlBulkHelpersConnectionProvider, timeoutSeconds)
        {
        }

        /// <inheritdoc/>
        public SqlBulkIdentityHelper(SqlTransaction sqlTransaction, int timeoutSeconds = DefaultBulkOperationTimeoutSeconds)
            : base(sqlTransaction, timeoutSeconds)
        {
        }

        /// <inheritdoc/>
        public SqlBulkIdentityHelper(SqlConnection sqlConnection, SqlTransaction sqlTransaction = null, int timeoutSeconds = DefaultBulkOperationTimeoutSeconds)
            : base(sqlConnection, sqlTransaction, timeoutSeconds)
        {
        }

        #endregion

        #region ISqlBulkHelper<T> implemenetations
        public virtual async Task<IEnumerable<T>> BulkInsertAsync(
            IEnumerable<T> entityList, 
            string tableName, 
            SqlTransaction transaction, 
            SqlMergeMatchQualifierExpression matchQualifierExpression = null
        )
        {
            return await BulkInsertOrUpdateWithIdentityColumnAsync(
                entityList,
                tableName,
                SqlBulkHelpersMergeAction.Insert,
                transaction,
                matchQualifierExpression
            );
        }

        public virtual async Task<IEnumerable<T>> BulkUpdateAsync(
            IEnumerable<T> entityList,
            string tableName,
            SqlTransaction transaction,
            SqlMergeMatchQualifierExpression matchQualifierExpression = null
        )
        {
            return await BulkInsertOrUpdateWithIdentityColumnAsync(
                entityList,
                tableName,
                SqlBulkHelpersMergeAction.Update,
                transaction,
                matchQualifierExpression
            );
        }

        public virtual async Task<IEnumerable<T>> BulkInsertOrUpdateAsync(
            IEnumerable<T> entityList,
            string tableName,
            SqlTransaction transaction,
            SqlMergeMatchQualifierExpression matchQualifierExpression = null
        )
        {
            return await BulkInsertOrUpdateWithIdentityColumnAsync(
                entityList, 
                tableName, 
                SqlBulkHelpersMergeAction.InsertOrUpdate, 
                transaction, 
                matchQualifierExpression
            );
        }

        public virtual IEnumerable<T> BulkInsert(
            IEnumerable<T> entityList,
            string tableName,
            SqlTransaction transaction,
            SqlMergeMatchQualifierExpression matchQualifierExpression = null
        )
        {
            return BulkInsertOrUpdateWithIdentityColumn(
                entityList,
                tableName,
                SqlBulkHelpersMergeAction.Insert,
                transaction,
                matchQualifierExpression
            );
        }

        public virtual IEnumerable<T> BulkUpdate(
            IEnumerable<T> entityList,
            string tableName,
            SqlTransaction transaction,
            SqlMergeMatchQualifierExpression matchQualifierExpression = null
        )
        {
            return BulkInsertOrUpdateWithIdentityColumn(
                entityList,
                tableName,
                SqlBulkHelpersMergeAction.Update,
                transaction,
                matchQualifierExpression
            );
        }

        public virtual IEnumerable<T> BulkInsertOrUpdate(
            IEnumerable<T> entityList,
            string tableName,
            SqlTransaction transaction,
            SqlMergeMatchQualifierExpression matchQualifierExpression = null
        )
        {
            return BulkInsertOrUpdateWithIdentityColumn(
                entityList, 
                tableName, 
                SqlBulkHelpersMergeAction.InsertOrUpdate, 
                transaction, 
                matchQualifierExpression
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
        /// <param name="matchQualifierExpression"></param>
        /// <param name="timeoutSeconds"></param>
        /// <returns></returns>
        protected virtual async Task<IEnumerable<T>> BulkInsertOrUpdateWithIdentityColumnAsync(
            IEnumerable<T> entities, 
            String tableName, 
            SqlBulkHelpersMergeAction mergeAction, 
            SqlTransaction transaction,
            SqlMergeMatchQualifierExpression matchQualifierExpression = null
        )
        {
            //For Performance we ensure the entities are only ever enumerated One Time!
            var entityList = entities.ToList();
            var bulkOperationTimeoutSeconds = this.BulkOperationTimeoutSeconds;

            using (ProcessHelper processHelper = this.CreateProcessHelper(
                entityList, tableName, mergeAction, transaction, bulkOperationTimeoutSeconds, matchQualifierExpression))
            {
                var sqlCmd = processHelper.SqlCommand;
                var sqlBulkCopy = processHelper.SqlBulkCopy;
                var sqlScripts = processHelper.SqlMergeScripts;

                //***STEP #4: Create Tables for Buffering Data & Storing Output values
                //            NOTE: THIS Step is Unique for Async processing...
                sqlCmd.CommandText = sqlScripts.SqlScriptToInitializeTempTables;
                await sqlCmd.ExecuteNonQueryAsync();

                //***STEP #5: Write Data to the Staging/Buffer Table as fast as possible!
                //            NOTE: THIS Step is Unique for Async processing...
                sqlBulkCopy.DestinationTableName = $"[{sqlScripts.TempStagingTableName}]";
                await sqlBulkCopy.WriteToServerAsync(processHelper.DataTable);

                //***STEP #6: Merge Data from the Staging Table into the Real Table
                //            and simultaneously Output Identity Id values into Output Temp Table!
                //            NOTE: THIS Step is Unique for Async processing...
                sqlCmd.CommandText = sqlScripts.SqlScriptToExecuteMergeProcess;

                //Execute this script and load the results....
                var mergeResultsList = new List<MergeResult>();
                using (SqlDataReader sqlReader = await sqlCmd.ExecuteReaderAsync())
                {
                    while (await sqlReader.ReadAsync())
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
        /// <returns></returns>
        protected virtual IEnumerable<T> BulkInsertOrUpdateWithIdentityColumn(
            IEnumerable<T> entities, 
            String tableName, 
            SqlBulkHelpersMergeAction mergeAction, 
            SqlTransaction transaction,
            SqlMergeMatchQualifierExpression matchQualifierExpression = null
        )
        {
            //For Performance we ensure the entities are only ever enumerated One Time!
            var entityList = entities.ToList();
            var bulkOperationTimeoutSeconds = this.BulkOperationTimeoutSeconds;

            using (ProcessHelper processHelper = this.CreateProcessHelper(
                entityList, tableName, mergeAction, transaction, bulkOperationTimeoutSeconds, matchQualifierExpression))
            {
                var sqlCmd = processHelper.SqlCommand;
                var sqlBulkCopy = processHelper.SqlBulkCopy;
                var sqlScripts = processHelper.SqlMergeScripts;

                //***STEP #4: Create Tables for Buffering Data & Storing Output values
                sqlCmd.CommandText = sqlScripts.SqlScriptToInitializeTempTables;
                sqlCmd.ExecuteNonQuery();

                //***STEP #5: Write Data to the Staging/Buffer Table as fast as possible!
                sqlBulkCopy.DestinationTableName = $"[{sqlScripts.TempStagingTableName}]";
                sqlBulkCopy.WriteToServer(processHelper.DataTable);

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
            //So far all calls to SqlDataReader have been asynchronous, but since the data reader is in 
            //non -sequential mode and ReadAsync was used, the column data should be read synchronously.
            var mergeResult = new MergeResult()
            {
                RowNumber = sqlReader.GetInt32(0),
                IdentityId = sqlReader.GetInt32(1),
                MergeAction = SqlBulkHelpersMerge.ParseMergeActionString(sqlReader.GetString(2))
            };
            return mergeResult;
        }

        /// <summary>
        /// BBernard - Private process helper to wrap up and encapsulate the initialization logic that is shared across both Async and Sync methods...
        /// </summary>
        /// <param name="entityList"></param>
        /// <param name="tableName"></param>
        /// <param name="mergeAction"></param>
        /// <param name="transaction"></param>
        /// <param name="timeoutSeconds"></param>
        /// <param name="matchQualifierExpression"></param>
        /// <returns></returns>
        protected virtual ProcessHelper CreateProcessHelper(
            List<T> entityList, 
            String tableName, 
            SqlBulkHelpersMergeAction mergeAction, 
            SqlTransaction transaction,
            int timeoutSeconds,
            SqlMergeMatchQualifierExpression matchQualifierExpression = null
        )
        {
            //***STEP #1: Load the Table Schema Definitions (cached after initial Load)!!!
            //BBernard
            //NOTE: Prevent SqlInjection - by validating that the TableName must be a valid value (as retrieved from the DB Schema) 
            //      we eliminate risk of Sql Injection.
            //NOTE: ALl other parameters are Strongly typed (vs raw Strings) thus eliminating risk of Sql Injection
            SqlBulkHelpersTableDefinition tableDefinition = this.GetTableSchemaDefinition(tableName);

            //***STEP #2: Dynamically Convert All Entities to a DataTable for consumption by the SqlBulkCopy class...
            DataTable dataTable = this.ConvertEntitiesToDataTableHelper(entityList, tableDefinition.IdentityColumn);

            //***STEP #3: Build all of the Sql Scripts needed to Process the entities based on the specified Table definition.
            SqlMergeScriptResults sqlScripts = this.BuildSqlMergeScriptsHelper(tableDefinition, mergeAction, matchQualifierExpression);

            //***STEP #4: Dynamically Initialize the Bulk Copy Helper using our Table data and table Definition!
            var sqlBulkCopyHelper = this.CreateSqlBulkCopyHelper(dataTable, tableDefinition, transaction, timeoutSeconds);

            return new ProcessHelper()
            {
                TableDefinition = tableDefinition,
                DataTable = dataTable,
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
            public DataTable DataTable { get; set; }
            public SqlMergeScriptResults SqlMergeScripts { get; set; }
            public SqlCommand SqlCommand { get; set; }
            public SqlBulkCopy SqlBulkCopy { get; set; }

            /// <summary>
            /// Implement IDisposable to ensure that we ALWAYS SAFELY CLEAN up our internal IDisposable resources.
            /// </summary>
            public void Dispose()
            {
                //Always clean up DataTables (as they can be heavy on memory until Garbage collection runs)
                (this.DataTable as IDisposable)?.Dispose();
                this.DataTable = null;

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
