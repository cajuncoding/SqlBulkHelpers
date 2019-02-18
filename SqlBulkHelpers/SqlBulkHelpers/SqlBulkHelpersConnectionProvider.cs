using System;
using System.Data.SqlClient;
using System.Configuration;
using System.Threading.Tasks;

namespace SqlBulkHelpers
{
    /// <summary>
    /// Connection string provider class to keep the reesponsibility for Loading the Connectionstring statically in
    /// on only one class.
    /// </summary>
    public static class SqlBulkHelpersConnectionProvider
    {
        public static String GetConnectionString()
        {
            //LOAD from internal Constant, other configuration, etc.
            return ConfigurationManager.AppSettings["SqlConnectionString"];
            //return Constants.CONNECTION_STRING;
        }

        public static SqlConnection NewConnection()
        {
            var connectionString = GetConnectionString();
            var sqlConn = new SqlConnection(connectionString);
            sqlConn.Open();

            return sqlConn;
        }

        public static async Task<SqlConnection> NewConnectionAsync()
        {
            var connectionString = GetConnectionString();
            var sqlConn = new SqlConnection(connectionString);
            await sqlConn.OpenAsync();
            return sqlConn;
        }
    }
}
