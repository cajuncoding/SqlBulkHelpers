using System;
using Microsoft.Data.SqlClient;

namespace SqlBulkHelpers
{
    public static class SqlBulkHelpersConfigConstants
    {
        public const int DefaultMaxConcurrentConnections = 5;
    }

    public interface ISqlBulkHelpersConfig
    {
        int SqlBulkBatchSize { get; } //General guidance is that 2000-5000 is efficient enough.
        int SqlBulkPerBatchTimeoutSeconds { get; }
        bool IsSqlBulkTableLockEnabled { get; }
        SqlBulkCopyOptions SqlBulkCopyOptions { get; }

        int MaterializeDataStructureProcessingTimeoutSeconds { get; }
        string MaterializedDataLoadingSchema { get; }
        string MaterializedDataDiscardingSchema { get; }
        bool IsCloningIdentitySeedValueEnabled { get; }

        ISqlBulkHelpersConnectionProvider ConcurrentConnectionFactory { get; }
        int MaxConcurrentConnections { get; }
        bool IsConcurrentConnectionProcessingEnabled { get; }
        bool IsFullTextIndexHandlingEnabled { get; }
    }

    public class SqlBulkHelpersConfig : ISqlBulkHelpersConfig
    {
        public static ISqlBulkHelpersConfig DefaultConfig { get; private set; } = new SqlBulkHelpersConfig();

        public static SqlBulkHelpersConfig Create(Action<SqlBulkHelpersConfig> configAction)
        {
            configAction.AssertArgumentIsNotNull(nameof(configAction));

            var newConfig = new SqlBulkHelpersConfig();
            configAction.Invoke(newConfig);
            return newConfig;
        }

        /// <summary>
        /// Configure the Default values for Sql Bulk Helpers and Materialized Data Helpers.
        /// </summary>
        /// <param name="configAction"></param>
        public static void ConfigureDefaults(Action<SqlBulkHelpersConfig> configAction)
        {
            DefaultConfig = Create(configAction);
        }

        /// <summary>
        /// Convenience method to enable the Concurrent Connection processing and other elements that must be handled in Isolated Connections and/or outside
        ///     of the materialized data processing Transaction!
        /// </summary>
        /// <param name="sqlConnectionFactory"></param>
        /// <param name="maxConcurrentConnections"></param>
        /// <param name="enableFullTextIndexHandling"></param>
        public void EnableConcurrentSqlConnectionProcessing(
            Func<SqlConnection> sqlConnectionFactory, 
            int maxConcurrentConnections = SqlBulkHelpersConfigConstants.DefaultMaxConcurrentConnections, 
            bool enableFullTextIndexHandling = true
        )
        {
            sqlConnectionFactory.AssertArgumentIsNotNull(nameof(sqlConnectionFactory));
            this.ConcurrentConnectionFactory = new SqlBulkHelpersConnectionProvider(sqlConnectionFactory);
            this.MaxConcurrentConnections = maxConcurrentConnections;
            this.IsFullTextIndexHandlingEnabled = enableFullTextIndexHandling;
        }

        /// <summary>
        /// Convenience method to enable the Concurrent Connection processing and other elements that must be handled in Isolated Connections and/or outside
        ///     of the materialized data processing Transaction!
        /// </summary>
        /// <param name="sqlConnectionProvider"></param>
        /// <param name="maxConcurrentConnections"></param>
        /// <param name="enableFullTextIndexHandling"></param>
        public void EnableConcurrentSqlConnectionProcessing(
            ISqlBulkHelpersConnectionProvider sqlConnectionProvider, 
            int maxConcurrentConnections = SqlBulkHelpersConfigConstants.DefaultMaxConcurrentConnections, 
            bool enableFullTextIndexHandling = true
        )
        {
            this.ConcurrentConnectionFactory = sqlConnectionProvider.AssertArgumentIsNotNull(nameof(sqlConnectionProvider));
            this.MaxConcurrentConnections = maxConcurrentConnections;
            this.IsFullTextIndexHandlingEnabled = enableFullTextIndexHandling;
        }

        public int SqlBulkBatchSize { get; set; } = 2000; //General guidance is that 2000-5000 is efficient enough.

        public int SqlBulkPerBatchTimeoutSeconds { get; set; }

        public bool IsSqlBulkTableLockEnabled
        {
            get => SqlBulkCopyOptions.HasFlag(SqlBulkCopyOptions.TableLock);
            set
            {
                if(value)
                    SqlBulkCopyOptions |= SqlBulkCopyOptions.TableLock;
                else
                    SqlBulkCopyOptions &= ~SqlBulkCopyOptions.TableLock;
            }
        }

        //NOTE: Default to TableLock to be enabled since our process always Writes to the Temp Table!
        public SqlBulkCopyOptions SqlBulkCopyOptions { get; set; } = SqlBulkCopyOptions.Default | SqlBulkCopyOptions.TableLock;
        
        public int MaterializeDataStructureProcessingTimeoutSeconds { get; set; } = 30;
        public string MaterializedDataLoadingSchema { get; set; } = "materializing_load";
        public string MaterializedDataDiscardingSchema { get; set; } = "materializing_discard";
        public bool IsCloningIdentitySeedValueEnabled { get; set; } = true;
        public ISqlBulkHelpersConnectionProvider ConcurrentConnectionFactory { get; set; } = null;

        private int _maxConcurrentConnections = SqlBulkHelpersConfigConstants.DefaultMaxConcurrentConnections;
        public int MaxConcurrentConnections
        {
            get => _maxConcurrentConnections;
            set
            {
                if (value < 1) throw new ArgumentOutOfRangeException(nameof(value), "The number of of max concurrent connections must be at least 1.");
                _maxConcurrentConnections = value;
            }
        }
        public bool IsConcurrentConnectionProcessingEnabled => ConcurrentConnectionFactory != null;
        public bool IsFullTextIndexHandlingEnabled { get; set; } = true;
    }
}
