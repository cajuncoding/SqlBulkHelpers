using System;
using System.Collections.Concurrent;
using System.Runtime.CompilerServices;
using LazyCacheHelpers;
using Microsoft.Data.SqlClient;

namespace SqlBulkHelpers
{
    /// <summary>
    /// Convenience Class to simplify internal caching of SqlBulkHelpersDbSchemaLoader instances.
    /// </summary>
    public static class SqlBulkHelpersSchemaLoaderCache
    {
        private static readonly LazyStaticInMemoryCache<string, ISqlBulkHelpersDBSchemaLoader> SchemaLoaderLazyCache =
            new LazyStaticInMemoryCache<string, ISqlBulkHelpersDBSchemaLoader>();

        /// <summary>
        /// This is the preferred way to initialize the Schema Loader. This will retrieve a DB Schema Loader using the
        /// SqlConnection Factory specified.  With an Sql Connection Factory, we can defer (e.g. lazy load)
        /// the the creation of a DB Connection if, and only when, it is actually needed; this eliminates the creation of
        /// an Sql Connection once the DB Schema cache is initialized.
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
                (uniqueId) => new SqlBulkHelpersDBSchemaLoader(sqlConnectionProvider)
            );

            return resultLoader;
        }

        /// <summary>
        /// This is the preferred way to initialize the Schema Loader. This will retrieve a DB Schema Loader using the
        /// SqlConnection Factory specified.  With an Sql Connection Factory, we can defer (e.g. lazy load)
        /// the the creation of a DB Connection if, and only when, it is actually needed; this eliminates the creation of
        /// an Sql Connection once the DB Schema cache is initialized.
        /// </summary>
        /// <param name="uniqueDbCachingIdentifier"></param>
        /// <param name="sqlConnectionFactory"></param>
        /// <returns></returns>
        public static ISqlBulkHelpersDBSchemaLoader GetSchemaLoader(string uniqueDbCachingIdentifier, Func<SqlConnection> sqlConnectionFactory)
        {
            //The Sql Connection Provider will validate the parameters...
            //NOTE: We can create our default provider from the factory Func provided.
            var sqlConnectionProvider = new SqlBulkHelpersConnectionProvider(uniqueDbCachingIdentifier, sqlConnectionFactory);
            return GetSchemaLoader(sqlConnectionProvider);
        }

        /// <summary>
        /// It's recommended to use the other overloads by providing a Connection Factory Func or implement ISqlBulkHelpersConnectionProvider
        /// however, this convenience method is now provided for use cases where only a valid SqlConnection exists but the actual ConnectionString
        /// is not available to initialize an ISqlBulkHelpersConnectionProvider.
        /// 
        /// By default, this will retrieve a DB Schema Loader using the existing SqlConnection provided.  This will immediately initialize the DB Schema
        /// definitions from the database when executed, because the SqlConnection is assumed to be valid now, but may not be in
        /// the future when lazy initialization would occur; such as if the Connection is closed, or a Transaction may not yet be started but may later be initialized,
        /// which would then result in errors, etc.
        /// </summary>
        /// <param name="sqlConnection"></param>
        /// <param name="sqlTransaction"></param>
        /// <param name="initializeImmediately"></param>
        /// <returns></returns>
        public static ISqlBulkHelpersDBSchemaLoader GetSchemaLoader(
            SqlConnection sqlConnection, 
            SqlTransaction sqlTransaction = null, 
            bool initializeImmediately = true
        )
        {
            //The Connection Proxy Provider will validate the parameters...
            var sqlConnProvider = new SqlBulkHelpersConnectionProxyExistingProvider(sqlConnection, sqlTransaction);

            //Use the Proxy Connection provider for the existing connection to get or initialize the DB Schema Loader.
            var schemaLoader = GetSchemaLoader(sqlConnProvider);

            //NOTE: Since a Connection was passed in then we generally should immediately initialize the Schema Loader,
            //      because this connection or transaction may no longer be valid later.
            if(initializeImmediately)
                schemaLoader.InitializeSchemaDefinitions();

            return schemaLoader;
        }

        /// <summary>
        /// Clear the DB Schema Loader cache and enable lazy re-initialization on-demand at next request for a given DB Schema Loader.
        /// </summary>
        public static void ClearCache()
            => SchemaLoaderLazyCache.ClearCache();
    }
}
