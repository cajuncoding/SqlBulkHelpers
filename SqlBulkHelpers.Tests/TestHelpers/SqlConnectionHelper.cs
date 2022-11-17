using System;
using System.Configuration;
using Microsoft.Data.SqlClient;

namespace SqlBulkHelpers.Tests
{
    public static class SqlConnectionHelper
    {
        public static string GetSqlConnectionString()
        {
            var sqlConnectionString = ConfigurationManager.AppSettings[SqlBulkHelpersConnectionProvider.SqlConnectionStringConfigKey];
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
    }
}
