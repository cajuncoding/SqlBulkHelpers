using System;
using System.Configuration;
using System.Data.SqlClient;
using System.Threading.Tasks;

namespace SqlBulkHelpers
{
    /// <summary>
    /// BBernard
    /// Connection string provider class to keep the responsibility for Loading the connection string in on only one class;
    /// but also supports custom implementations that would initialize the Connection string in any other custom way.
    ///
    /// The Default implementation will load the Connection String from teh AppSettings key "SqlConnectionString"
    /// </summary>
    public class SqlBulkHelpersConnectionProvider : ISqlBulkHelpersConnectionProvider
    {
        /// <summary>
        /// Provides a Default instance of the Sql Bulk Helpers Connection Provider.
        /// </summary>
        public static ISqlBulkHelpersConnectionProvider Default = new SqlBulkHelpersConnectionProvider();

        //For performance we load this via a Lazy to ensure we only ever access AppSettings one time.
        private static readonly Lazy<String> _connectionStringLoaderLazy = new Lazy<string>(
            () => ConfigurationManager.AppSettings["SqlConnectionString"]
        );

        protected virtual String GetConnectionString()
        {
            //LOAD from internal Constant, other configuration, etc.
            return _connectionStringLoaderLazy.Value;
        }

        public virtual SqlConnection NewConnection()
        {
            var connectionString = GetConnectionString();
            var sqlConn = new SqlConnection(connectionString);
            sqlConn.Open();

            return sqlConn;
        }

        public virtual async Task<SqlConnection> NewConnectionAsync()
        {
            var connectionString = GetConnectionString();
            var sqlConn = new SqlConnection(connectionString);
            await sqlConn.OpenAsync();
            return sqlConn;
        }
    }
}
