using System;
using System.Configuration;
using Microsoft.Data.SqlClient;

namespace SqlBulkHelpers.Tests
{
    public static class SqlConnectionHelper
    {
        public static string GetSqlConnectionString()
        {
            var sqlConnectionString = TestConfiguration.SqlConnectionString;
            return sqlConnectionString;
        }

        public static ISqlBulkHelpersConnectionProvider GetConnectionProvider()
        {
            return new SqlBulkHelpersConnectionProvider(GetSqlConnectionString());
        }
        
        public static SqlConnection NewConnection()
        {
            var sqlConn = new SqlConnection(GetSqlConnectionString());
            sqlConn.Open();
            return sqlConn;
        }

        public static Task<SqlConnection> NewConnectionAsync()
        {
            var sqlConnectionProvider = GetConnectionProvider();
            return sqlConnectionProvider.NewConnectionAsync();
        }
    }
}
