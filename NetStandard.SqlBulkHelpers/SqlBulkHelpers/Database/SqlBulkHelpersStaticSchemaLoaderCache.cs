using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace SqlBulkHelpers
{
    /// <summary>
    /// Convenience Class to simplify internal caching of SqlBulkHelpersDbSchemaLoader instances.
    /// </summary>
    public static class SqlBulkHelpersStaticSchemaLoaderCache
    {
        private static readonly ConcurrentDictionary<string, Lazy<SqlBulkHelpersDBSchemaStaticLoader>> SchemaLoaderLazyCache =
            new ConcurrentDictionary<string, Lazy<SqlBulkHelpersDBSchemaStaticLoader>>();

        public static ISqlBulkHelpersDBSchemaLoader GetSchemaLoader(ISqlBulkHelpersConnectionProvider sqlConnectionProvider)
        {
            //Validate arg is a Static Schema Loader...
            var sqlConnProvider = sqlConnectionProvider.AssertArgumentIsNotNull(nameof(sqlConnectionProvider));

            //Init cached version if it exists; which may already be initialized!
            var resultLoader = SchemaLoaderLazyCache.GetOrAdd(
                sqlConnProvider.GetDbConnectionUniqueIdentifier(),
                new Lazy<SqlBulkHelpersDBSchemaStaticLoader>(() => new SqlBulkHelpersDBSchemaStaticLoader(sqlConnectionProvider))
            );

            //Unwrap the Lazy<> to get, or construct, a valid Schema Loader...
            return resultLoader.Value;
        }
    }
}
