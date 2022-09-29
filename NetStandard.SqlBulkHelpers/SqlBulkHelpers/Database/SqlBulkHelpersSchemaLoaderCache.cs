using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using Microsoft.Data.SqlClient;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Text;
using Microsoft.IdentityModel.Tokens;
using System.ComponentModel;
using System.Transactions;

namespace SqlBulkHelpers
{
    /// <summary>
    /// Convenience Class to simplify internal caching of SqlBulkHelpersDbSchemaLoader instances.
    /// </summary>
    public static class SqlBulkHelpersSchemaLoaderCache
    {
        private static readonly ConcurrentDictionary<string, Lazy<SqlBulkHelpersDBSchemaLoader>> SchemaLoaderLazyCache =
            new ConcurrentDictionary<string, Lazy<SqlBulkHelpersDBSchemaLoader>>();

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

            //TODO: Critical Enhancement to improve and ensure that Exceptions are not Cached; enabling the code to re-attempt to load the cache until eventually a valid connection works and Cache then prevents reloading anymore!
            //Init cached version if it exists; which may already be initialized!
            var resultLoader = SchemaLoaderLazyCache.GetOrAdd(
            sqlConnProvider.GetDbConnectionUniqueIdentifier(),
                new Lazy<SqlBulkHelpersDBSchemaLoader>(() => new SqlBulkHelpersDBSchemaLoader(sqlConnectionProvider))
            );

            //Unwrap the Lazy<> to get, or construct, a valid Schema Loader...
            return resultLoader.Value;
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
            var sqlConnectionProvider = new SqlBulkHelpersConnectionProvider(uniqueDbCachingIdentifier, sqlConnectionFactory);
            return GetSchemaLoader(sqlConnectionProvider);
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
        /// <param name="initializeImmediately"></param>
        /// <returns></returns>
        public static ISqlBulkHelpersDBSchemaLoader GetSchemaLoader(SqlConnection sqlConnection, SqlTransaction sqlTransaction = null, bool initializeImmediately = true)
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
    }
}
