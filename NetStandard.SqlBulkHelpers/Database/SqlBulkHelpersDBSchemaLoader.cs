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
				WITH TablesCte AS (
					SELECT
						TableSchema = t.[TABLE_SCHEMA], 
						TableName = t.[TABLE_NAME],
						TableCatalog = t.[TABLE_CATALOG],
						ObjectId = OBJECT_ID('['+t.TABLE_SCHEMA+'].['+t.TABLE_NAME+']')
					FROM INFORMATION_SCHEMA.TABLES t
				)
				SELECT
					t.TableSchema, 
					t.TableName,
					[Columns] = (
						SELECT 
							OrdinalPosition = ORDINAL_POSITION,
							ColumnName = COLUMN_NAME,
							DataType = DATA_TYPE,
							IsIdentityColumn = CAST(COLUMNPROPERTY(t.ObjectId, COLUMN_NAME, 'IsIdentity') AS bit)
						FROM INFORMATION_SCHEMA.COLUMNS c
						WHERE 
							c.TABLE_NAME = t.TableName
							and c.TABLE_SCHEMA = t.TableSchema 
							and c.TABLE_CATALOG = t.TableCatalog 
						ORDER BY c.ORDINAL_POSITION
						FOR JSON PATH
					),
                    [ColumnDefaultConstraints] = (
						SELECT
							ConstraintName = dc.[name],
							[ColumnName] = col.[name],
							[Definition] = dc.[definition]
						FROM sys.default_constraints dc
							INNER JOIN sys.columns AS col ON (col.default_object_id = dc.[object_id])
						WHERE DC.[parent_object_id] = t.ObjectId
						FOR JSON PATH
					),
					[ColumnCheckConstraints] = (
                        SELECT 
	                        ConstraintName = c.CONSTRAINT_NAME, 
							[CheckClause] = (
								SELECT TOP (1) cc.CHECK_CLAUSE 
								FROM INFORMATION_SCHEMA.CHECK_CONSTRAINTS cc 
								WHERE 
									cc.CONSTRAINT_CATALOG = c.CONSTRAINT_CATALOG
									AND cc.CONSTRAINT_SCHEMA = c.CONSTRAINT_SCHEMA 
									AND cc.CONSTRAINT_NAME = c.CONSTRAINT_NAME
							)
                            FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS c
						    WHERE 
								c.TABLE_NAME = t.TableName
								AND c.TABLE_SCHEMA = t.TableSchema 
								AND c.TABLE_CATALOG = t.TableCatalog
								AND c.CONSTRAINT_TYPE = 'CHECK'
                        FOR JSON PATH
					),
					[KeyConstraints] = (
                        SELECT 
	                        ConstraintName = c.CONSTRAINT_NAME,
	                        ConstraintType = CASE c.CONSTRAINT_TYPE
								WHEN 'FOREIGN KEY' THEN 'ForeignKey'
								WHEN 'PRIMARY KEY' THEN 'PrimaryKey'
							END,
	                        [KeyColumns] = (
		                        SELECT 
									OrdinalPosition = col.ORDINAL_POSITION,
									ColumnName = col.COLUMN_NAME
		                        FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE col
		                        WHERE 
									col.TABLE_NAME = c.TABLE_NAME 
									AND col.TABLE_SCHEMA = c.TABLE_SCHEMA 
									AND col.CONSTRAINT_SCHEMA = c.CONSTRAINT_SCHEMA
		                        ORDER BY col.ORDINAL_POSITION
		                        FOR JSON PATH
	                        )
                            FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS c
						    WHERE 
								c.TABLE_NAME = t.TableName
								AND c.TABLE_SCHEMA = t.TableSchema 
								AND c.TABLE_CATALOG = t.TableCatalog
								AND c.CONSTRAINT_TYPE IN ('PRIMARY KEY', 'FOREIGN KEY')
                        FOR JSON PATH
                    ),
                    [TableIndexes] = (
                        SELECT 
	                        IndexId = i.index_id, 
	                        IndexName= i.[name],
	                        IsUnique = i.is_unique,
	                        IsUniqueConstraint = i.is_unique_constraint,
	                        FilterDefinition = i.filter_definition,
	                        [KeyColumns] = (
		                        SELECT 
									OrdinalPosition = ic.index_column_id,
			                        ColumnName = c.[name], 
			                        IsDescending = ic.is_descending_key
		                        FROM sys.index_columns ic
			                        INNER JOIN sys.columns c ON (c.[object_id] = ic.[object_id] and c.column_id = ic.column_id)
		                        WHERE 
									ic.index_id = i.index_id 
									AND ic.[object_id] = i.[object_id] 
									AND key_ordinal > 0 -- KeyOrdinal > 0 are Key Columns
		                        ORDER BY ic.index_column_id
		                        FOR JSON PATH
	                        ),
	                        [IncludeColumns] = (
		                        SELECT
									OrdinalPosition = ic.index_column_id,
			                        ColumnName = c.[name], 
			                        IsDescending = ic.is_descending_key
		                        FROM sys.index_columns ic
			                        INNER JOIN sys.columns c ON (c.[object_id] = ic.[object_id] and c.column_id = ic.column_id)
		                        WHERE 
									ic.index_id = i.index_id 
									AND ic.[object_id] = i.[object_id] 
									AND key_ordinal = 0 -- KeyOrdinal == 0 are Include Columns
		                        ORDER BY ic.index_column_id
		                        FOR JSON PATH
	                        )
                        FROM sys.indexes i
	                    WHERE 
							[type] = 2 -- Type 2 are NONCLUSTERED Table Indexes
							AND [object_id] = t.ObjectId
                        FOR JSON PATH
                    )
				FROM TablesCte t
				ORDER BY t.TableName
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
			var tableNameTerm = tableName.ParseAsTableNameTerm();
			var tableDefinition = tableSchemaLowercaseLookup[tableNameTerm.FullyQualifiedTableName.ToLowerInvariant()]?.FirstOrDefault();
			return tableDefinition;
		}
	}
}
