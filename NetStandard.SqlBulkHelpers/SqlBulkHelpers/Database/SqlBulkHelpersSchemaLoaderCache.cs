using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using Microsoft.Data.SqlClient;
using System.Linq;
using System.Text;

namespace SqlBulkHelpers
{
    /// <summary>
    /// Convenience Class to simplify internal caching of SqlBulkHelpersDbSchemaLoader instances.
    /// </summary>
    public static class SqlBulkHelpersSchemaLoaderCache
    {
        private static readonly ConcurrentDictionary<string, Lazy<SqlBulkHelpersDBSchemaStaticLoader>> SchemaLoaderLazyCache =
            new ConcurrentDictionary<string, Lazy<SqlBulkHelpersDBSchemaStaticLoader>>();

        /// <summary>
        /// This is the preferred way to initialize the Schema Loader. This will retrieve a DB Schema Loader using the
        /// SqlConnection Provider specified.  With an Sql Connection Provider, we can defer (e.g. lazy load)
        /// the loading of the Schema Definitions until it's actually needed.  This may speed up startup time if
        /// this initialization is part of a static element at startup and/or if it is never needed based on execution logic.
        /// </summary>
        /// <param name="sqlConnectionProvider"></param>
        /// <returns></returns>
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

        /// <summary>
        /// It's recommended to provide an ISqlBulkHelpersConnectionProvider however, this convenience method is now provided for
        /// use cases where only a valid SqlConnection exists but the actual ConnectionString is not available to initialize an
        /// ISqlBulkHelpersConnectionProvider.
        /// 
        /// This will retrieve a DB Schema Loader using the existing SqlConnection provided.  This will immediately initialize the DB Schema
        /// definitions from the database when executed, because the SqlConnection is assumed to be valid now, but may not be in
        /// the future when lazy initialization would occur (e.g. a Transaction may not yet be started but may later be initialized,
        /// which would then result in errors).
        /// </summary>
        /// <param name="sqlConnection"></param>
        /// <param name="sqlTransaction"></param>
        /// <returns></returns>
        public static ISqlBulkHelpersDBSchemaLoader GetSchemaLoader(SqlConnection sqlConnection, SqlTransaction sqlTransaction = null, bool initializeImmediately = true)
        {
            //Validate arg is a Static Schema Loader...
            var sqlConnProvider = new SqlBulkHelpersConnectionProxyExistingProvider(
                sqlConnection.AssertArgumentIsNotNull(nameof(sqlConnection)),
                sqlTransaction
            );

            //Use the Proxy Connection provider for the existing connection to get or initialize the DB Schema Loader.
            var schemaLoader = GetSchemaLoader(sqlConnProvider);

            //NOTE: Since a Connection was passed in then we generally should immediately initialize the Schema Loader,
            //      because this connection or transaction may no longer be valid later.
            if(initializeImmediately)
                schemaLoader.InitializeSchemaDefinitions();

            return schemaLoader;
        }
    }
}
