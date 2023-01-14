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
                    using (var sqlCmd = CreateSchemaQuerySqlCommand(tableNameTerm, detailLevel, sqlConnection, sqlTransaction))
                    {
                        //Execute and load results from the Json...
                        var tableDef = sqlCmd.ExecuteForJson<SqlBulkHelpersTableDefinition>();
                        return tableDef;
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
