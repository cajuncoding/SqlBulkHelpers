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
        public const string SqlConnectionStringConfigKey = "SqlConnectionString";

        private readonly string _sqlConnectionString;

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
        /// Provide Internal access to the Connection String to help uniquely identify the Connections from this Provider.
        /// </summary>
        /// <returns>Unique string representing connections provided by this provider</returns>
        public virtual string GetDbConnectionUniqueIdentifier()
        {
            return GetConnectionString();
        }

        protected  virtual string GetConnectionString()
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
