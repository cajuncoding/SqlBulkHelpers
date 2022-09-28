using Microsoft.Data.SqlClient;
using System.Threading.Tasks;

namespace SqlBulkHelpers
{
    /// <summary>
    /// BBernard
    /// Interface to support custom implementations for initializing Sql Database connections as needed.
    /// </summary>
    public interface ISqlBulkHelpersConnectionProvider
    {
        /// <summary>
        /// Create a brand new open database connection to use for retrieving Schema details
        /// NOTE: It is expected that this connection is NOT already enrolled in a Transaction!
        /// NOTE: If a connection must be reused with a transaction then the SqlBulkHelpersConnectionProxyExistingProvider should be used.
        /// </summary>
        /// <returns></returns>
        SqlConnection NewConnection();

        /// <summary>
        /// Create a brand new open database connection to use for retrieving Schema details
        /// NOTE: It is expected that this connection is NOT already enrolled in a Transaction!
        /// NOTE: If a connection must be reused with a transaction then the SqlBulkHelpersConnectionProxyExistingProvider should be used.
        /// </summary>
        /// <returns></returns>
        Task<SqlConnection> NewConnectionAsync();

        string GetDbConnectionUniqueIdentifier();
    }
}