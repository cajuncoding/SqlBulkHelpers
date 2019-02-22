using System;
using System.Collections.Generic;
using System.Linq;
using System.Data;
using System.Data.SqlClient;
using System.Threading.Tasks;
using System.Text;
using Newtonsoft.Json;

namespace SqlBulkHelpers
{

    public static class SqlBulkHelpersConstants
    {
        public const String DEFAULT_IDENTITY_COLUMN_NAME = "Id";
        public const String ROWNUMBER_COLUMN_NAME = "SQLBULKHELPERS_ROWNUMBER";
    }

    public class SqlBulkHelpersDBSchemaLoader
    {
        //private static readonly Dictionary<String, List<String>> _tableColumnsDictionary = new Dictionary<String, List<String>>();
        private static readonly ILookup<String, SqlBulkHelpersTableDefinition> _tableDefinitionsLookup;
        private static readonly List<String> _tableNames = new List<String>();

        static SqlBulkHelpersDBSchemaLoader()
        {
            //NOTE: WE MUST LOAD the Table Definitions in the Static Constructor to guarantee Thread Safety
            //          while allowing us to eagerly load all data in one fetch for future performance of all
            //          subsequent calls.
            _tableDefinitionsLookup = LoadSqlBulkHelpersDBSchemaHelper();
        }

        /// <summary>
        /// BBernard
        /// Add all table and their columns from maestro database into the dictionary in a fully Thread Safe manner using
        /// the Static Constructor!
        /// </summary>
        private static ILookup<String, SqlBulkHelpersTableDefinition> LoadSqlBulkHelpersDBSchemaHelper()
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

            using (SqlConnection sqlConn = SqlBulkHelpersConnectionProvider.NewConnection())
            using (SqlCommand sqlCmd = new SqlCommand(tableSchemaSql, sqlConn))
            {
                var tableDefinitionsList = sqlCmd.ExecuteForJson<List<SqlBulkHelpersTableDefinition>>();
                
                //Dynamically convert to a Lookup for immutable cache of data.
                //NOTE: Lookup is immutable (vs Dictionary which is not) and performance for lookups is just as fast.
                var tableDefinitionsLookup = tableDefinitionsList.ToLookup(t => t.TableName.ToLowerInvariant());
                return tableDefinitionsLookup;
            }
        }

        public static SqlBulkHelpersTableDefinition GetTableSchemaDefinition(String tableName)
        {
            if (String.IsNullOrEmpty(tableName)) return null;

            var tableDefinition = _tableDefinitionsLookup[tableName.ToLowerInvariant()]?.FirstOrDefault();
            return tableDefinition;
        }
    }
}
