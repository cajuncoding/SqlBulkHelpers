using System;
using System.Configuration;
using System.Data.SqlClient;
using System.Threading.Tasks;

namespace SqlBulkHelpers
{
    /// <summary>
    /// BBernard
    /// Connection string provider class to allow re-use of existing connection that may already be initialized to be used
    /// for implementations that may already create connections and just need us to proxy them for use within SqlBulkHelpers.
    /// </summary>
    public class SqlBulkHelpersConnectionProxyExistingProvider : ISqlBulkHelpersConnectionProvider
    {
        private readonly SqlConnection _sqlConnection;

        public SqlBulkHelpersConnectionProxyExistingProvider(SqlConnection sqlConnection)
        {
            _sqlConnection = sqlConnection.AssertArgumentIsNotNull(nameof(sqlConnection));
        }
        
        /// <summary>
        /// Provide Internal access to the Connection String to help uniquely identify the Connections from this Provider.
        /// </summary>
        /// <returns>Unique string representing connections provided by this provider</returns>
        public virtual string GetDbConnectionUniqueIdentifier()
        {
            return GetConnectionString();
        }

        protected virtual string GetConnectionString()
        {
            return _sqlConnection.ConnectionString;
        }

        public virtual SqlConnection NewConnection()
        {
            //Proxy the existing connection that was provided...
            return _sqlConnection;
        }

        public virtual Task<SqlConnection> NewConnectionAsync()
        {
            //Proxy the existing connection that was provided...
            return Task.FromResult(_sqlConnection);
        }
    }
}
