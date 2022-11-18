using System;
using System.Collections.Generic;
using Microsoft.Data.SqlClient;
using System.Linq;
using LazyCacheHelpers;
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
        //Safely initialize the LazyStaticInMemoryCache<> loader for Table Definition Schemas.
        //NOTE: This provides a high performance self-populating/blocking cache with exception handling (e.g. Exceptions are not cached like they are with default Lazy<> behavior).
        protected readonly LazyStaticInMemoryCache<string, SqlBulkHelpersTableDefinition> TableDefinitionsCaseInsensitiveLazyCache = new LazyStaticInMemoryCache<string, SqlBulkHelpersTableDefinition>();

		public SqlBulkHelpersDBSchemaLoader()
		{
        }

        public virtual void Reload()
        {
            TableDefinitionsCaseInsensitiveLazyCache.ClearCache();
        }

        protected static SqlBulkHelpersTableDefinition LoadTableSchemaDefinitionsInternal(TableNameTerm tableNameTerm, ISqlBulkHelpersConnectionProvider sqlConnectionProvider)
        {
			//Validate inputs and short circuit...
            sqlConnectionProvider.AssertArgumentIsNotNull(nameof(sqlConnectionProvider));

			#region Schema SQL Query/Script
			var tableSchemaSql = $@"
				WITH TablesCte AS (
					SELECT TOP (1)
						TableSchema = t.[TABLE_SCHEMA], 
						TableName = t.[TABLE_NAME],
						--TableCatalog = t.[TABLE_CATALOG],
						ObjectId = OBJECT_ID('['+t.TABLE_SCHEMA+'].['+t.TABLE_NAME+']')
					FROM INFORMATION_SCHEMA.TABLES t
                    WHERE 
                        t.TABLE_SCHEMA = @TableSchema
                        AND t.TABLE_NAME = @TableName
				)
				SELECT
					t.TableSchema, 
					t.TableName,
					[TableColumns] = (
						SELECT 
							OrdinalPosition = ORDINAL_POSITION,
							ColumnName = COLUMN_NAME,
							DataType = DATA_TYPE,
							IsIdentityColumn = CAST(COLUMNPROPERTY(t.ObjectId, COLUMN_NAME, 'IsIdentity') AS bit),
							CharacterMaxLength = CHARACTER_MAXIMUM_LENGTH,
							NumericPrecision = NUMERIC_PRECISION,
							NumericPrecisionRadix = NUMERIC_PRECISION_RADIX,
							NumericScale = NUMERIC_SCALE,
							DateTimePrecision = DATETIME_PRECISION
						FROM INFORMATION_SCHEMA.COLUMNS c
						WHERE 
							c.TABLE_SCHEMA = t.TableSchema 
							AND c.TABLE_NAME = t.TableName
						ORDER BY c.ORDINAL_POSITION
						FOR JSON PATH
					),
					[PrimaryKeyConstraint] = JSON_QUERY((
                        SELECT TOP (1)
	                        ConstraintName = c.CONSTRAINT_NAME,
	                        ConstraintType = 'PrimaryKey',
	                        [KeyColumns] = (
		                        SELECT 
									OrdinalPosition = col.ORDINAL_POSITION,
									ColumnName = col.COLUMN_NAME
		                        FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE col
		                        WHERE 
									col.TABLE_SCHEMA = c.TABLE_SCHEMA
									AND col.TABLE_NAME = c.TABLE_NAME 
									AND col.CONSTRAINT_SCHEMA = c.CONSTRAINT_SCHEMA
                                    AND col.CONSTRAINT_NAME = c.CONSTRAINT_NAME
		                        ORDER BY col.ORDINAL_POSITION
		                        FOR JSON PATH
	                        )
                        FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS c
						WHERE
                            c.TABLE_SCHEMA = t.TableSchema
							AND c.TABLE_NAME = t.TableName 
							AND c.CONSTRAINT_TYPE = 'PRIMARY KEY'
                        FOR JSON PATH, WITHOUT_ARRAY_WRAPPER
                    )),
					[ForeignKeyConstraints] = (
						-- DISTINCT is REQUIRED to Pull Reference Table up to Top Level of the Constraint!
						SELECT DISTINCT 
	                        ConstraintName = c.CONSTRAINT_NAME,
	                        ConstraintType = 'ForeignKey',
                            ReferenceTableSchema = rcol.TABLE_SCHEMA,
                            ReferenceTableName = rcol.TABLE_NAME,
                            ReferentialMatchOption = rc.MATCH_OPTION,
                            ReferentialUpdateRuleClause = rc.UPDATE_RULE,
                            ReferentialDeleteRuleClause = rc.DELETE_RULE,
	                        [KeyColumns] = (
		                        SELECT 
									OrdinalPosition = col.ORDINAL_POSITION,
									ColumnName = col.COLUMN_NAME
		                        FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE col
		                        WHERE 
									col.TABLE_SCHEMA = c.TABLE_SCHEMA 
									AND col.TABLE_NAME = c.TABLE_NAME 
									AND col.CONSTRAINT_SCHEMA = c.CONSTRAINT_SCHEMA
                                    AND col.CONSTRAINT_NAME = c.CONSTRAINT_NAME
		                        ORDER BY col.ORDINAL_POSITION
		                        FOR JSON PATH
	                        ),
                            [ReferenceColumns] = (
		                        SELECT 
									OrdinalPosition = col.ORDINAL_POSITION,
									ColumnName = col.COLUMN_NAME
		                        FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE col
                                WHERE 
	                                --FKeys MUST reference to the Unique Constraints or PKey Unique Constraints...
                                    col.CONSTRAINT_SCHEMA = rc.UNIQUE_CONSTRAINT_SCHEMA
	                                AND col.CONSTRAINT_NAME = rc.UNIQUE_CONSTRAINT_NAME
		                        ORDER BY col.ORDINAL_POSITION
		                        FOR JSON PATH
                            )
                        FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS c
	                        INNER JOIN INFORMATION_SCHEMA.REFERENTIAL_CONSTRAINTS rc ON (rc.CONSTRAINT_SCHEMA = c.CONSTRAINT_SCHEMA AND rc.CONSTRAINT_NAME = c.CONSTRAINT_NAME)
	                        --FKeys MUST reference to the Unique Constraints or PKey Unique Constraints...
	                        INNER JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE rcol ON (rcol.CONSTRAINT_SCHEMA = rc.UNIQUE_CONSTRAINT_SCHEMA AND rcol.CONSTRAINT_NAME = rc.UNIQUE_CONSTRAINT_NAME)                            
						WHERE
							c.TABLE_SCHEMA = t.TableSchema
                            AND c.TABLE_NAME = t.TableName
							AND c.CONSTRAINT_TYPE = 'FOREIGN KEY'
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
							c.TABLE_SCHEMA = t.TableSchema 
							AND c.TABLE_NAME = t.TableName
							AND c.CONSTRAINT_TYPE = 'CHECK'
                        FOR JSON PATH
					),
                    --NOTE: TableIndexes (in SQL Server) include Unique Constraints.
                    [TableIndexes] = (
                        SELECT 
	                        IndexId = i.index_id, 
	                        IndexName= i.[name],
	                        IsUnique = i.is_unique,
	                        IsUniqueConstraint = i.is_unique_constraint,
	                        FilterDefinition = i.filter_definition,
	                        [KeyColumns] = (
		                        SELECT 
									OrdinalPosition = ROW_NUMBER() OVER(ORDER BY ic.index_column_id ASC),
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
									OrdinalPosition = ROW_NUMBER() OVER(ORDER BY ic.index_column_id ASC),
			                        ColumnName = c.[name]
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
				FOR JSON PATH, WITHOUT_ARRAY_WRAPPER
			";
			#endregion

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
                    var loadedTableDefinition = LoadTableSchemaDefinitionsInternal(tableNameTerm, sqlConnectionProvider);
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
