using System;
using LazyCacheHelpers;

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
        /// Load the DB Schema Provider based on the unique caching identifier specified (e.g. usually a unique string for each Datasource/Database).
        /// </summary>
        /// <param name="uniqueDbCachingIdentifier"></param>
        /// <returns></returns>
        public static ISqlBulkHelpersDBSchemaLoader GetSchemaLoader(string uniqueDbCachingIdentifier)
        {
            //Init cached version if it exists; which may already be initialized!
            var resultLoader = SchemaLoaderLazyCache.GetOrAdd(
                key: uniqueDbCachingIdentifier,
                cacheValueFactory: (uniqueId) => new SqlBulkHelpersDBSchemaLoader()
            );

            return resultLoader;
        }

        /// <summary>
        /// This is the preferred way to initialize the Schema Loader this will Load the DB Schema Provider based unique connection identifier provided
        /// by the ISqlBulkHelpersConnectionProvider.
        /// </summary>
        /// <param name="sqlConnectionProvider"></param>
        /// <returns></returns>
        public static ISqlBulkHelpersDBSchemaLoader GetSchemaLoader(ISqlBulkHelpersConnectionProvider sqlConnectionProvider)
        {
            //Validate arg is a Static Schema Loader...
            sqlConnectionProvider.AssertArgumentIsNotNull(nameof(sqlConnectionProvider));
            return GetSchemaLoader(sqlConnectionProvider.GetDbConnectionUniqueIdentifier());
        }

        /// <summary>
        /// Clear the DB Schema Loader cache and enable lazy re-initialization on-demand at next request for a given DB Schema Loader.
        /// </summary>
        public static void ClearCache()
            => SchemaLoaderLazyCache.ClearCache();

        /// <summary>
        /// Gets the Count of SchemaLoaders in the Cache.
        /// </summary>
        public static int Count => SchemaLoaderLazyCache.GetCacheCount();
    }
}
