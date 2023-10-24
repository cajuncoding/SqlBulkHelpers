using System;
using System.Threading.Tasks;
using Microsoft.Data.SqlClient;
using LazyCacheHelpers;
using SqlBulkHelpers.CustomExtensions;

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
        //Safely initialize the LazyStaticInMemoryCache<> loader for Table Definition Schemas.
        //NOTE: This provides a high performance self-populating/blocking cache with exception handling (e.g. Exceptions are not cached like they are with default Lazy<> behavior).
        //NOTE: This is not Static because this instance is Cached inside the SqlBulkHelpersSchemaLoaderCache which caches based on Database Connection;
        //          therefore this must be an instance cache.
        protected LazyStaticInMemoryCache<string, SqlBulkHelpersTableDefinition> TableDefinitionsCaseInsensitiveLazyCache { get; } = new LazyStaticInMemoryCache<string, SqlBulkHelpersTableDefinition>();

		//Load and statically cache the Database Query from our SQL File...
        protected static readonly Type SchemaLoaderCachedType = typeof(SqlBulkHelpersDBSchemaLoader);
        protected static string SqlServerTableSchemaExtendedDetailsQuerySql { get; } = SchemaLoaderCachedType.Assembly
            .LoadEmbeddedResourceDataAsString("Database/SqlQueries/QueryDBTableSchemaExtendedDetailsJson.sql");

        protected static string SqlServerTableSchemaBasicDetailsQuerySql { get; } = SchemaLoaderCachedType.Assembly
            .LoadEmbeddedResourceDataAsString("Database/SqlQueries/QueryDBTableSchemaBasicDetailsJson.sql");

        public virtual ValueTask ClearCacheAsync()
        {
            TableDefinitionsCaseInsensitiveLazyCache.ClearCache();
            return new ValueTask();
        }

        protected virtual string CreateCacheKeyInternal(TableNameTerm tableNameTerm, TableSchemaDetailLevel detailLevel)
            => string.Concat(tableNameTerm.FullyQualifiedTableName, "::", detailLevel.ToString());

        protected string GetTableSchemaSqlQuery(TableSchemaDetailLevel detailLevel)
        {
            return detailLevel == TableSchemaDetailLevel.ExtendedDetails
                ? SqlServerTableSchemaExtendedDetailsQuerySql
                : SqlServerTableSchemaBasicDetailsQuerySql;
        }

        protected SqlCommand CreateSchemaQuerySqlCommand(
            TableNameTerm tableNameTerm, 
            TableSchemaDetailLevel detailLevel, 
            SqlConnection sqlConnection, 
            SqlTransaction sqlTransaction = null
        )
        {
            var tableSchemaQuerySql = GetTableSchemaSqlQuery(detailLevel);

            //Enable Support for Temp Table Schema Loading...
            if (tableNameTerm.IsTempTableName)
            {
                //NOTE: For Temp Table support all references to INFORMATION_SCHEMA must be replaced with tempdb.INFORMATION_SCHEMA
                //          and DB_NAME() must be changed to 'tempdb', otherwise we dynamically resolve the true Temp Table Name in the Cte...
                tableSchemaQuerySql = tableSchemaQuerySql
                    .Replace("INFORMATION_SCHEMA.", "tempdb.INFORMATION_SCHEMA.")
                    .Replace("DB_NAME()", "'tempdb'");
            }

            var sqlCmd = new SqlCommand(tableSchemaQuerySql, sqlConnection, sqlTransaction);
            
            //Configure the timeout for retrieving the Schema details...
            sqlCmd.CommandTimeout = SqlBulkHelpersConfig.DefaultConfig.DbSchemaLoaderQueryTimeoutSeconds;

            //Add the Parameter to get only Details for the requested table...
            var sqlParams = sqlCmd.Parameters;
            sqlParams.Add(new SqlParameter("@TableSchema", tableNameTerm.SchemaName));
            sqlParams.Add(new SqlParameter("@TableName", tableNameTerm.TableName));
            sqlParams.Add(new SqlParameter("@IsTempTable", tableNameTerm.IsTempTableName));
            return sqlCmd;
        }

        public async Task<SqlBulkHelpersTableDefinition> GetTableSchemaDefinitionAsync(
            string tableName,
            TableSchemaDetailLevel detailLevel,
            SqlConnection sqlConnection,
            SqlTransaction sqlTransaction = null,
            bool forceCacheReload = false
        )
        {
            sqlConnection.AssertArgumentIsNotNull(nameof(sqlConnection));

            var tableDefinition = await GetTableSchemaDefinitionInternalAsync(
                tableName,
                detailLevel,
                () => Task.FromResult((sqlConnection, sqlTransaction)),
                disposeOfConnection: false, //DO Not dispose of Existing Connection/Transaction...
                forceCacheReload
            ).ConfigureAwait(false);

            return tableDefinition;
        }

        public async Task<SqlBulkHelpersTableDefinition> GetTableSchemaDefinitionAsync(
            string tableName,
            TableSchemaDetailLevel detailLevel,
            Func<Task<SqlConnection>> sqlConnectionAsyncFactory,
            bool forceCacheReload = false
        )
        {
            sqlConnectionAsyncFactory.AssertArgumentIsNotNull(nameof(sqlConnectionAsyncFactory));

            var tableDefinition = await GetTableSchemaDefinitionInternalAsync(
                tableName,
                detailLevel,
                async () =>
                {
                    var sqlConnection = await sqlConnectionAsyncFactory().ConfigureAwait(false);
                    return (sqlConnection, (SqlTransaction)null);
                },
                disposeOfConnection: true, //Always DISPOSE of New Connections created by the Factory...
                forceCacheReload
            ).ConfigureAwait(false);

            return tableDefinition;
        }

        protected async Task<SqlBulkHelpersTableDefinition> GetTableSchemaDefinitionInternalAsync(
            string tableName,
            TableSchemaDetailLevel detailLevel,
            Func<Task<(SqlConnection, SqlTransaction)>> sqlConnectionAndTransactionAsyncFactory,
            bool disposeOfConnection,
            bool forceCacheReload = false
        )
        {
            sqlConnectionAndTransactionAsyncFactory.AssertArgumentIsNotNull(nameof(sqlConnectionAndTransactionAsyncFactory));

            if (string.IsNullOrWhiteSpace(tableName))
                return null;

            var tableNameTerm = tableName.ParseAsTableNameTerm();
            var cacheKey = CreateCacheKeyInternal(tableNameTerm, detailLevel);

            //NOTE: We also prevent caching of Temp Table schemas that are by definition Transient!
            if (forceCacheReload || tableNameTerm.IsTempTableName)
                TableDefinitionsCaseInsensitiveLazyCache.TryRemoveAsyncValue(cacheKey);

            var tableDefinition = await TableDefinitionsCaseInsensitiveLazyCache.GetOrAddAsync(
                key: cacheKey,
                cacheValueFactoryAsync: async key =>
                {
                    var (sqlConnection, sqlTransaction) = await sqlConnectionAndTransactionAsyncFactory().ConfigureAwait(false);
                    try
                    {
                        //If we don't have a Transaction then offer lazy opening of the Connection,
                        //  but if we do have a Transaction we assume the Connection is open & valid for the Transaction...
                        if (sqlTransaction == null)
                            await sqlConnection.EnsureSqlConnectionIsOpenAsync().ConfigureAwait(false);

                        using (var sqlCmd = CreateSchemaQuerySqlCommand(tableNameTerm, detailLevel, sqlConnection, sqlTransaction))
                        {
                            //Execute and load results from the Json...
                            var tableDef = await sqlCmd.ExecuteForJsonAsync<SqlBulkHelpersTableDefinition>().ConfigureAwait(false);
                            return tableDef;
                        }

                    }
                    finally
                    {
                        #if NETSTANDARD2_1
                        if(disposeOfConnection)
                            await sqlConnection.DisposeAsync().ConfigureAwait(false);
                        #else
                        if(disposeOfConnection)
                            sqlConnection.Dispose();
                        #endif
                    }
                }
            ).ConfigureAwait(false);

            return tableDefinition;
        }

    }
}
