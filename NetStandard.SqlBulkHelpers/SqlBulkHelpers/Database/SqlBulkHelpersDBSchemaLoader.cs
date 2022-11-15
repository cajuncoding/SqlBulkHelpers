using System;
using System.Collections.Generic;
using Microsoft.Data.SqlClient;
using System.Linq;
using SqlBulkHelpers.SqlBulkHelpers.Interfaces;

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
		private Lazy<ILookup<string, SqlBulkHelpersTableDefinition>> _tableDefinitionsLowercaseLookupLazy;
        private readonly ISqlBulkHelpersConnectionProvider _sqlConnectionProvider;

        /// <summary>
        /// Flag denoting if the Schema has been initialized/loaded yet; it is Lazy initialized on demand.
        /// </summary>
        public bool IsInitialized { get; protected set; } = false;
		
		public SqlBulkHelpersDBSchemaLoader(ISqlBulkHelpersConnectionProvider sqlConnectionProvider)
		{
			_sqlConnectionProvider = sqlConnectionProvider.AssertArgumentIsNotNull(nameof(sqlConnectionProvider));

            //TODO: Critical Enhancement to improve and ensure that Exceptions are not Cached; enabling the code to re-attempt to load the cache until eventually a valid connection works and Cache then prevents reloading anymore!
            //Safely initialize the Lazy<> loader for Table Definition Schemas.
            //NOTE: We use a Lazy<> here so that our manual locking does as little work as possible and simply initializes the Lazy<> reference,
            //          leaving the optimized locking for execution of the long-running logic to the underlying Lazy<> object to manage with
            //          maximum efficiency
            ResetSchemaLoaderLazyCache();
        }

        protected void ResetSchemaLoaderLazyCache()
        {
            _tableDefinitionsLowercaseLookupLazy = new Lazy<ILookup<string, SqlBulkHelpersTableDefinition>>(() =>
            {
                var dbSchemaResults = LoadSqlBulkHelpersDBSchemaHelper(_sqlConnectionProvider);
                this.IsInitialized = true;

                return dbSchemaResults;
            });
        }

        public virtual void Reload()
        {
			//To reload, we just need to reset the Cache and it will re-initialize!
			ResetSchemaLoaderLazyCache();
        }

        public virtual ILookup<string, SqlBulkHelpersTableDefinition> GetTableSchemaDefinitionsLowercaseLookupFromLazyCache()
        {
            try
            {
                //This will safely lazy load the Schema, if not already, in a Thread-safe manner by using the power of Lazy<>!
                var tableSchemaLowercaseLookup = _tableDefinitionsLowercaseLookupLazy.Value;
                return tableSchemaLowercaseLookup;
            }
            catch
            {
                //If an Exception occurs then there may have been a DB connection issue, etc. so we do not want to Cache the Exception
                //	for the rest of the apps lifetime, instead we re-attempt next request, and then re-throw the exception to be handled!
                ResetSchemaLoaderLazyCache();
                throw;
            }
        }

        /// <summary>
        /// BBernard
        /// Add all table and their columns from the database into the dictionary in a fully Thread Safe manner using
        /// the Static Constructor!
        /// </summary>
        private static ILookup<string, SqlBulkHelpersTableDefinition> LoadSqlBulkHelpersDBSchemaHelper(ISqlBulkHelpersConnectionProvider sqlConnectionProvider)
		{
			var tableSchemaSql = @"
				SELECT 
					[TABLE_SCHEMA] as TableSchema, 
					[TABLE_NAME] as TableName,
					[Columns] = (
						SELECT 
							COLUMN_NAME as ColumnName,
							ORDINAL_POSITION as OrdinalPosition,
							DATA_TYPE as DataType,
							COLUMNPROPERTY(OBJECT_ID(table_schema+'.'+table_name), COLUMN_NAME, 'IsIdentity') as IsIdentityColumn
						FROM INFORMATION_SCHEMA.COLUMNS c
						WHERE 
							c.TABLE_NAME = t.TABLE_NAME
							and c.TABLE_SCHEMA = t.TABLE_SCHEMA 
							and c.TABLE_CATALOG = t.TABLE_CATALOG 
						ORDER BY c.ORDINAL_POSITION
						FOR JSON PATH
					)
				FROM INFORMATION_SCHEMA.TABLES t
				ORDER BY t.TABLE_NAME
				FOR JSON PATH
			";

			SqlConnection sqlConn = sqlConnectionProvider.NewConnection();

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
				using (SqlCommand sqlCmd = new SqlCommand(tableSchemaSql, sqlConn, sqlTransaction))
				{
					var tableDefinitionsList = sqlCmd.ExecuteForJson<List<SqlBulkHelpersTableDefinition>>();

					//Dynamically convert to a Lookup for immutable cache of data.
					//NOTE: Lookup is immutable (vs Dictionary which is not) and performance for lookups is just as fast.
					var tableDefinitionsLowercaseLookup = tableDefinitionsList.Where(t => t != null).ToLookup(
						t => $"{t.TableFullyQualifiedName.ToLowerInvariant()}"
					);
					return tableDefinitionsLowercaseLookup;
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

		public virtual SqlBulkHelpersTableDefinition GetTableSchemaDefinition(string tableName)
		{
			if (string.IsNullOrWhiteSpace(tableName)) return null;

			//This will safely lazy load teh Schema, if not already, in a Thread-safe manner by using the power of Lazy<>!
			var tableSchemaLowercaseLookup = GetTableSchemaDefinitionsLowercaseLookupFromLazyCache();

			//First Try a Direct Lookup and return if found...
			var parsedTableName = tableName.ParseAsTableNameTerm();
			var tableDefinition = tableSchemaLowercaseLookup[parsedTableName.FullyQualifiedTableName.ToLowerInvariant()]?.FirstOrDefault();
			return tableDefinition;
		}
	}
}
