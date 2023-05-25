using System;
using Microsoft.Data.SqlClient;
using System.Threading.Tasks;
using SqlBulkHelpers.CustomExtensions;
using SqlBulkHelpers.SqlBulkHelpers.Interfaces;

namespace SqlBulkHelpers
{
    /// <summary>
    /// BBernard
    /// Connection string provider class to allow re-use of existing connection that may already be initialized to be used
    /// for implementations that may already create connections and just need us to proxy them for use within SqlBulkHelpers.
    /// NOTE: This is INTERNAL for special use so we minimize impacts to our Interfaces, it is not recommended for any external
    ///         consumers to use this.
    /// </summary>
    internal class SqlBulkHelpersConnectionProxyExistingProvider : ISqlBulkHelpersConnectionProvider, ISqlBulkHelpersHasTransaction
    {
        private readonly SqlConnection _sqlConnection;
        private readonly SqlTransaction _sqlTransaction;

        public SqlBulkHelpersConnectionProxyExistingProvider(SqlConnection sqlConnection, SqlTransaction sqlTransaction = null)
        {
            _sqlConnection = sqlConnection.AssertArgumentIsNotNull(nameof(sqlConnection));
            _sqlTransaction = sqlTransaction;
        }
        
        /// <summary>
        /// Provide Internal access to the Connection String to help uniquely identify the Connections from this Provider.
        /// </summary>
        /// <returns>Unique string representing connections provided by this provider</returns>
        public virtual string GetDbConnectionUniqueIdentifier() => _sqlConnection.ConnectionString;

        public virtual SqlConnection NewConnection() => _sqlConnection;

        public virtual Task<SqlConnection> NewConnectionAsync() => Task.FromResult(_sqlConnection);

        /// <summary>
        /// Method that provides direct access to the Sql Transaction when this is used to proxy an Existing Connection
        ///     that also has an associated open transaction.
        /// </summary>
        /// <returns></returns>
        public virtual SqlTransaction GetTransaction() => _sqlTransaction;
    }
}
