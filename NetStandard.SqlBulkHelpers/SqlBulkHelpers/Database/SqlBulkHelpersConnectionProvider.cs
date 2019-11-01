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
        public const string SQL_CONNECTION_STRING_CONFIG_KEY = "SqlConnectionString";

        private string _sqlConnectionString;

        public SqlBulkHelpersConnectionProvider()
        {
            _sqlConnectionString = ConfigurationManager.AppSettings[SQL_CONNECTION_STRING_CONFIG_KEY];

            if (String.IsNullOrWhiteSpace(_sqlConnectionString))
            {
                throw new ConfigurationErrorsException(
                    $"The application configuration is missing a value for setting [{SQL_CONNECTION_STRING_CONFIG_KEY}],"
                    + $" so default Sql Connection cannot be initialized; check the App.config Xml file and try again."
                );
            }
        }

        public SqlBulkHelpersConnectionProvider(string sqlConnectionString)
        {
            _sqlConnectionString = sqlConnectionString;

            if (String.IsNullOrWhiteSpace(_sqlConnectionString))
            {
                throw new ArgumentException(
                    $"The argument specified for {nameof(sqlConnectionString)} is null/empty; a valid value must be specified."
                );
            }

        }

        /// <summary>
        /// Provides a Default instance of the Sql Bulk Helpers Connection Provider.
        /// </summary>
        public static ISqlBulkHelpersConnectionProvider Default = new SqlBulkHelpersConnectionProvider();

        protected virtual String GetConnectionString()
        {
            return _sqlConnectionString;
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
