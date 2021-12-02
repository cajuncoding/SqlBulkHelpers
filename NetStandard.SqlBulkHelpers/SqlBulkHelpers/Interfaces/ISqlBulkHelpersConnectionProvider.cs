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
        SqlConnection NewConnection();

        Task<SqlConnection> NewConnectionAsync();

        string GetDbConnectionUniqueIdentifier();
    }
}