using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Linq;

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
    public class SqlBulkHelpersDBSchemaStaticLoader : ISqlBulkHelpersDBSchemaLoader
    {
        private Lazy<ILookup<String, SqlBulkHelpersTableDefinition>> _tableDefinitionsLookupLazy;

        /// <summary>
        /// Provides a Default instance of the Sql Bulk Helpers DB Schema Loader that uses Static/Lazy loading for high performance.
        /// NOTE: This will use the Default instance of the SqlBulkHelpersConnectionProvider as it's dependency and cache it as a Static
        ///         singleton for high performance.
        /// </summary>
        public static ISqlBulkHelpersDBSchemaLoader Default = new SqlBulkHelpersDBSchemaStaticLoader(SqlBulkHelpersConnectionProvider.Default);

        public SqlBulkHelpersDBSchemaStaticLoader(ISqlBulkHelpersConnectionProvider sqlConnectionProvider)
        {
            sqlConnectionProvider.AssertArgumentIsNotNull(nameof(sqlConnectionProvider));

            //Safely initialize the Lazy<> loader for Table Definition Schemas.
            //NOTE: We use a Lazy<> here so that our manual locking does as little work as possible and simply initializes the Lazy<> reference,
            //          leaving the optimized locking for execution of the long-running logic to the underlying Lazy<> object to manage with
            //          maximum efficiency
            _tableDefinitionsLookupLazy = new Lazy<ILookup<string, SqlBulkHelpersTableDefinition>>(() =>
            {
                //Get a local reference so that it's scoping will be preserved...
                var localScopeSqlConnectionProviderRef = sqlConnectionProvider;
                var dbSchemaResults = LoadSqlBulkHelpersDBSchemaHelper(localScopeSqlConnectionProviderRef);
                return dbSchemaResults;
            });
        }

        /// <summary>
        /// BBernard
        /// Add all table and their columns from the database into the dictionary in a fully Thread Safe manner using
        /// the Static Constructor!
        /// </summary>
        private ILookup<String, SqlBulkHelpersTableDefinition> LoadSqlBulkHelpersDBSchemaHelper(ISqlBulkHelpersConnectionProvider sqlConnectionProvider)
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

            using (SqlConnection sqlConn = sqlConnectionProvider.NewConnection())
            using (SqlCommand sqlCmd = new SqlCommand(tableSchemaSql, sqlConn))
            {
                var tableDefinitionsList = sqlCmd.ExecuteForJson<List<SqlBulkHelpersTableDefinition>>();
                
                //Dynamically convert to a Lookup for immutable cache of data.
                //NOTE: Lookup is immutable (vs Dictionary which is not) and performance for lookups is just as fast.
                var tableDefinitionsLookup = tableDefinitionsList.Where(t => t != null).ToLookup(t => t.TableName.ToLowerInvariant());
                return tableDefinitionsLookup;
            }
        }

        public virtual SqlBulkHelpersTableDefinition GetTableSchemaDefinition(String tableName)
        {
            if (String.IsNullOrEmpty(tableName)) return null;

            var schemaLookup = _tableDefinitionsLookupLazy.Value;
            var tableDefinition = schemaLookup[tableName.ToLowerInvariant()]?.FirstOrDefault();
            return tableDefinition;
        }
    }
}
