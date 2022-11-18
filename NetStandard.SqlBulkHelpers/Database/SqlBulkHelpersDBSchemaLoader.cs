using System;
using Microsoft.Data.SqlClient;
using LazyCacheHelpers;
using SqlBulkHelpers.SqlBulkHelpers.Interfaces;
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
	public class SqlBulkHelpersDBSchemaLoader : ISqlBulkHelpersDBSchemaLoader
	{
        //Safely initialize the LazyStaticInMemoryCache<> loader for Table Definition Schemas.
        //NOTE: This provides a high performance self-populating/blocking cache with exception handling (e.g. Exceptions are not cached like they are with default Lazy<> behavior).
        protected LazyStaticInMemoryCache<string, SqlBulkHelpersTableDefinition> TableDefinitionsCaseInsensitiveLazyCache { get; } = new LazyStaticInMemoryCache<string, SqlBulkHelpersTableDefinition>();

		//Load and statically cache the Database Query from our SQL File...
		protected static string SqlServerTableSchemaQuerySql { get; } = typeof(SqlBulkHelpersDBSchemaLoader).Assembly.LoadEmbeddedResourceDataAsString("Database/SqlQueries/QueryDBTableSchemaJson.sql");

		public SqlBulkHelpersDBSchemaLoader()
		{
        }

        public virtual void Reload()
        {
            TableDefinitionsCaseInsensitiveLazyCache.ClearCache();
        }

        protected static SqlBulkHelpersTableDefinition LoadTableSchemaDefinitionInternal(TableNameTerm tableNameTerm, ISqlBulkHelpersConnectionProvider sqlConnectionProvider)
        {
			//Validate inputs and short circuit...
			SqlConnection sqlConn = sqlConnectionProvider.AssertArgumentIsNotNull(nameof(sqlConnectionProvider)).NewConnection();

			//Determine if our connection provider created a new Connection or if it is proxy-ing for an Existing Sql Connection.
			//NOTE: If we are proxy-ing an existing Connection then we also need to handle a potentially associated Transaction.
			bool isNewConnectionInitialized = true;
			SqlTransaction sqlTransaction = null;
			if (sqlConnectionProvider is ISqlBulkHelpersHasTransaction providerWithTransaction)
			{
				isNewConnectionInitialized = false;
				sqlTransaction = providerWithTransaction.GetTransaction();
			}

			try
			{
				using (SqlCommand sqlCmd = new SqlCommand(SqlServerTableSchemaQuerySql, sqlConn, sqlTransaction))
                {
					//Add the Parameter to get only Details for the requested table...
                    sqlCmd.Parameters.Add(new SqlParameter("@TableSchema", tableNameTerm.SchemaName));
                    sqlCmd.Parameters.Add(new SqlParameter("@TableName", tableNameTerm.TableName));

                    //Execute and load results from the Json...
                    var tableDefinition = sqlCmd.ExecuteForJson<SqlBulkHelpersTableDefinition>();
                    return tableDefinition;
				}
			}
			finally
			{
				//Cleanup the Sql Connection IF it was newly created it...
				if (isNewConnectionInitialized && sqlConn != null)
				{
					sqlConn.Close();
					sqlConn.Dispose();
                }
			}
		}

		public virtual SqlBulkHelpersTableDefinition GetTableSchemaDefinition(string tableName, ISqlBulkHelpersConnectionProvider sqlConnectionProvider)
		{
            sqlConnectionProvider.AssertArgumentIsNotNull(nameof(sqlConnectionProvider));

            if (string.IsNullOrWhiteSpace(tableName)) 
                return null;

			var tableNameTerm = tableName.ParseAsTableNameTerm();
			
            var tableDefinition = TableDefinitionsCaseInsensitiveLazyCache.GetOrAdd(
                key: tableNameTerm.FullyQualifiedTableName,
                cacheValueFactory:keyTableName =>
                {
                    var loadedTableDefinition = LoadTableSchemaDefinitionInternal(tableNameTerm, sqlConnectionProvider);
                    return loadedTableDefinition;
                }
            );

			return tableDefinition;
		}

        public SqlBulkHelpersTableDefinition GetTableSchemaDefinition(string tableName, Func<SqlConnection> sqlConnectionFactory)
        {
			sqlConnectionFactory.AssertArgumentIsNotNull(nameof(sqlConnectionFactory));
            var sqlConnectionProvider = new SqlBulkHelpersConnectionProxyExistingProvider(sqlConnectionFactory.Invoke());
            
            return GetTableSchemaDefinition(tableName, sqlConnectionProvider);
        }

        public SqlBulkHelpersTableDefinition GetTableSchemaDefinition(string tableName, SqlTransaction sqlTransaction)
        {
            sqlTransaction.AssertArgumentIsNotNull(nameof(sqlTransaction));
            var sqlConnectionProvider = new SqlBulkHelpersConnectionProxyExistingProvider(sqlTransaction.Connection, sqlTransaction);

            return GetTableSchemaDefinition(tableName, sqlConnectionProvider);
        }
    }
}
