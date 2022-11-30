using System;
using Microsoft.Extensions.Configuration;

namespace SqlBulkHelpers.Tests
{
    internal static class TestConfiguration
    {
        private static readonly IConfiguration _config;

        static TestConfiguration()
        {
            _config = new ConfigurationBuilder()
               .AddJsonFile("appsettings.tests.json")
               .AddEnvironmentVariables()
               .Build();
        }

        public static string? SqlConnectionString => _config[SqlBulkHelpersConnectionProvider.SqlConnectionStringConfigKey];
    }
}
