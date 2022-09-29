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
		private readonly Lazy<ILookup<string, SqlBulkHelpersTableDefinition>> _tableDefinitionsLookupLazy;

		/// <summary>
		/// Flag denoting if the Schema has been initialized/loaded yet; it is Lazy initialized on demand.
		/// </summary>
		public bool IsInitialized { get; protected set; } = false;
		
		public SqlBulkHelpersDBSchemaLoader(ISqlBulkHelpersConnectionProvider sqlConnectionProvider)
		{
			sqlConnectionProvider.AssertArgumentIsNotNull(nameof(sqlConnectionProvider));

            //TODO: Critical Enhancement to improve and ensure that Exceptions are not Cached; enabling the code to re-attempt to load the cache until eventually a valid connection works and Cache then prevents reloading anymore!
			//Safely initialize the Lazy<> loader for Table Definition Schemas.
            //NOTE: We use a Lazy<> here so that our manual locking does as little work as possible and simply initializes the Lazy<> reference,
            //          leaving the optimized locking for execution of the long-running logic to the underlying Lazy<> object to manage with
            //          maximum efficiency
            _tableDefinitionsLookupLazy = new Lazy<ILookup<string, SqlBulkHelpersTableDefinition>>(() =>
			{
				var dbSchemaResults = LoadSqlBulkHelpersDBSchemaHelper(sqlConnectionProvider);
				this.IsInitialized = true;

				return dbSchemaResults;
			});
		}

		/// <summary>
		/// BBernard
		/// Add all table and their columns from the database into the dictionary in a fully Thread Safe manner using
		/// the Static Constructor!
		/// </summary>
		private ILookup<string, SqlBulkHelpersTableDefinition> LoadSqlBulkHelpersDBSchemaHelper(ISqlBulkHelpersConnectionProvider sqlConnectionProvider)
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
					var tableDefinitionsLookup = tableDefinitionsList.Where(t => t != null).ToLookup(
						t => $"{t.TableFullyQualifiedName.ToLowerInvariant()}"
					);
					return tableDefinitionsLookup;
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

		public virtual ILookup<string, SqlBulkHelpersTableDefinition> InitializeSchemaDefinitions()
		{
			//This will safely lazy load teh Schema, if not already, in a Thread-safe manner by using the power of Lazy<>!
			var schemaLookup = _tableDefinitionsLookupLazy.Value;
			return schemaLookup;
		}

		public virtual SqlBulkHelpersTableDefinition GetTableSchemaDefinition(string tableName)
		{
			if (string.IsNullOrWhiteSpace(tableName)) return null;

			//This will safely lazy load teh Schema, if not already, in a Thread-safe manner by using the power of Lazy<>!
			var schemaLookup = InitializeSchemaDefinitions();

			//First Try a Direct Lookup and return if found...
			var parsedTableName = ParseTableFullyQualifiedName(tableName);
			var tableDefinition = schemaLookup[parsedTableName]?.FirstOrDefault();
			return tableDefinition;
		}

		private string ParseTableFullyQualifiedName(string tableName)
		{
			var loweredTableName = tableName.ToLowerInvariant();

			//Second Try Parsing the Table & Schema name a Direct Lookup and return if found...
			var terms = loweredTableName.Split('.');
			switch (terms.Length)
			{
				//Split will always return an array with at least 1 element
				case 1: return $"[dbo].[{TrimTableNameTerm(terms[0])}]";
				default: return $"[{TrimTableNameTerm(terms[0])}].[{TrimTableNameTerm(terms[1])}]";
			}
		}

		private string TrimTableNameTerm(string term)
		{
			var trimmedTerm = term.Trim('[', ']', ' ');
			return trimmedTerm;
		}
	}
}
