using System;
using System.Collections.Generic;
using System.Configuration;
using System.Runtime.InteropServices;

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
    }
}
