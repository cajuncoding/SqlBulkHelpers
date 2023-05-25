using System;
using Microsoft.Data.SqlClient;

namespace SqlBulkHelpers
{
	/// <summary>
	/// BBernard
	/// DB Schema Loader class to keep the responsibility for Loading the Schema Definitions of Sql Server tables in on only one class;
	/// but also supports custom implementations that would initialize the DB Schema Definitions in any other custom way.
	///
	/// The Default implementation will load the Database schema with Lazy/Deferred loading for performance, but it will use the Sql Connection Provider
	///     specified in the first instance that this class is initialized from because the Schema Definitions will be statically cached across
	///     all instances for high performance!
	///
	/// NOTE: The static caching of the DB Schema is great for performance, and this default implementation will work well for most users (e.g. single database use),
	///         however more advanced usage may require the consumer/author to implement & manage  their own ISqlBulkHelpersDBSchemaLoader.
	/// </summary>
	public partial class SqlBulkHelpersDBSchemaLoader : ISqlBulkHelpersDBSchemaLoader
	{
        public SqlBulkHelpersTableDefinition GetTableSchemaDefinition(
            string tableName, 
            TableSchemaDetailLevel detailLevel, 
            SqlConnection sqlConnection,
            SqlTransaction sqlTransaction = null,
            bool forceCacheReload = false
        )
        {
            sqlConnection.AssertArgumentIsNotNull(nameof(sqlConnection));

            var tableDefinition = GetTableSchemaDefinitionInternal(
                tableName,
                detailLevel,
                () => (sqlConnection, sqlTransaction),
                disposeOfConnection: false, //DO Not dispose of Existing Connection/Transaction...
                forceCacheReload
            );

            return tableDefinition;
        }

        public SqlBulkHelpersTableDefinition GetTableSchemaDefinition(
            string tableName,
            TableSchemaDetailLevel detailLevel,
            Func<SqlConnection> sqlConnectionFactory,
            bool forceCacheReload = false
        )
        {
            sqlConnectionFactory.AssertArgumentIsNotNull(nameof(sqlConnectionFactory));

            var tableDefinition = GetTableSchemaDefinitionInternal(
                tableName,
                detailLevel,
                () => (sqlConnectionFactory(), (SqlTransaction)null),
                disposeOfConnection: true, //Always DISPOSE of New Connections created by the Factory...
                forceCacheReload
            );

            return tableDefinition;
        }

        protected SqlBulkHelpersTableDefinition GetTableSchemaDefinitionInternal(
            string tableName,
            TableSchemaDetailLevel detailLevel,
            Func<(SqlConnection, SqlTransaction)> sqlConnectionAndTransactionFactory,
            bool disposeOfConnection,
            bool forceCacheReload = false
        )
        {
            sqlConnectionAndTransactionFactory.AssertArgumentIsNotNull(nameof(sqlConnectionAndTransactionFactory));

            if (string.IsNullOrWhiteSpace(tableName))
                return null;

            var tableNameTerm = tableName.ParseAsTableNameTerm();
            var cacheKey = CreateCacheKeyInternal(tableNameTerm, detailLevel);

            if (forceCacheReload)
                TableDefinitionsCaseInsensitiveLazyCache.TryRemove(cacheKey);

            var tableDefinitionResult = TableDefinitionsCaseInsensitiveLazyCache.GetOrAdd(
                key: cacheKey,
                cacheValueFactory: key =>
                {
                    var (sqlConnection, sqlTransaction) = sqlConnectionAndTransactionFactory();
                    try
                    {
                        //If we don't have a Transaction then offer lazy opening of the Connection,
                        //  but if we do have a Transaction we assume the Connection is open & valid for the Transaction...
                        if (sqlTransaction == null)
                            sqlConnection.EnsureSqlConnectionIsOpen();

                        using (var sqlCmd = CreateSchemaQuerySqlCommand(tableNameTerm, detailLevel, sqlConnection, sqlTransaction))
                        {
                            //Execute and load results from the Json...
                            var tableDef = sqlCmd.ExecuteForJson<SqlBulkHelpersTableDefinition>();
                            return tableDef;
                        }
                    }
                    finally
                    {
                        if(disposeOfConnection)
                            sqlConnection.Dispose();
                    }
                });

            return tableDefinitionResult;
        }

        public void ClearCache()
        {
            TableDefinitionsCaseInsensitiveLazyCache.ClearCache();
        }
    }
}
