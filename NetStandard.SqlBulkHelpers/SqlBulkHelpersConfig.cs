using System;
using Microsoft.Data.SqlClient;
using SqlBulkHelpers.CustomExtensions;
using SqlBulkHelpers.MaterializedData;

namespace SqlBulkHelpers
{
    public static class SqlBulkHelpersConfigConstants
    {
        public const int DefaultMaxConcurrentConnections = 5;
    }

    public enum SchemaCopyMode
    {
        OutsideTransactionAvoidSchemaLocks = 1,
        InsideTransactionAllowSchemaLocks = 2,
    }

    public interface ISqlBulkHelpersConfig
    {
        int SqlBulkBatchSize { get; } //General guidance is that 2000-5000 is efficient enough.
        int SqlBulkPerBatchTimeoutSeconds { get; }
        bool IsSqlBulkTableLockEnabled { get; }
        SqlBulkCopyOptions SqlBulkCopyOptions { get; }

        int DbSchemaLoaderQueryTimeoutSeconds { get; }

        int MaterializeDataStructureProcessingTimeoutSeconds { get; }
        int MaterializedDataSwitchTableWaitTimeoutMinutes { get; }
        SwitchWaitTimeoutAction MaterializedDataSwitchTimeoutAction { get; }

        string MaterializedDataLoadingSchema { get; }
        string MaterializedDataLoadingTablePrefix { get; }
        string MaterializedDataLoadingTableSuffix { get; }

        string MaterializedDataDiscardingSchema { get; }
        string MaterializedDataDiscardingTablePrefix { get; }
        string MaterializedDataDiscardingTableSuffix { get; }

        SchemaCopyMode MaterializedDataSchemaCopyMode { get; }
        bool MaterializedDataMakeSchemaCopyNamesUnique { get; }
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
            
            //Validate the Configuration!
            if (DefaultConfig.IsFullTextIndexHandlingEnabled && !DefaultConfig.IsConcurrentConnectionProcessingEnabled)
                throw new InvalidOperationException(
                $"Full Text Index Handling is currently enabled however Concurrent Connections are disabled and/or " +
                        $"no {nameof(SqlBulkHelpersConfig.ConcurrentConnectionFactory)} or {nameof(ISqlBulkHelpersConnectionProvider)} instance was provided. " +
                        $"Concurrent connection support is required due to Sql Server limitations that do not allow Full Text Indexes " +
                        $"on tables being Switched, and also prohibit Full Text Indexes from being disabled/dropped within a user transaction; " +
                        $"therefore a separate concurrent connection must be used."
                );
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
            bool enableFullTextIndexHandling = false
        )
        {
            sqlConnectionFactory.AssertArgumentIsNotNull(nameof(sqlConnectionFactory));
            this.ConcurrentConnectionFactory = new SqlBulkHelpersConnectionProvider(sqlConnectionFactory);
            this.MaxConcurrentConnections = maxConcurrentConnections;
            //NOTE: Full text index handling may have been enabled already so we preserve it or turn it on if specified to this convenience method!
            this.IsFullTextIndexHandlingEnabled = IsFullTextIndexHandlingEnabled || enableFullTextIndexHandling;
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
            bool enableFullTextIndexHandling = false
        )
        {
            this.ConcurrentConnectionFactory = sqlConnectionProvider.AssertArgumentIsNotNull(nameof(sqlConnectionProvider));
            this.MaxConcurrentConnections = maxConcurrentConnections;
            //NOTE: Full text index handling may have been enabled already so we preserve it or turn it on if specified to this convenience method!
            this.IsFullTextIndexHandlingEnabled = IsFullTextIndexHandlingEnabled || enableFullTextIndexHandling;
        }

        public int SqlBulkBatchSize { get; set; } = 2000; //General guidance is that 2000-5000 is efficient enough.

        public int SqlBulkPerBatchTimeoutSeconds { get; set; }

        public bool IsSqlBulkTableLockEnabled
        {
            get => SqlBulkCopyOptions.HasFlag(SqlBulkCopyOptions.TableLock);
            set
            {
                if(value) SqlBulkCopyOptions |= SqlBulkCopyOptions.TableLock;
                else SqlBulkCopyOptions &= ~SqlBulkCopyOptions.TableLock;
            }
        }

        //NOTE: Default to TableLock to be enabled since our process always Writes to the Temp Table!
        public SqlBulkCopyOptions SqlBulkCopyOptions { get; set; } = SqlBulkCopyOptions.Default | SqlBulkCopyOptions.TableLock;

        public int DbSchemaLoaderQueryTimeoutSeconds { get; set; } = 30;

        public int MaterializeDataStructureProcessingTimeoutSeconds { get; set; } = 30;
        public int MaterializedDataSwitchTableWaitTimeoutMinutes { get; set; } = 1;
        public SwitchWaitTimeoutAction MaterializedDataSwitchTimeoutAction { get; } = SwitchWaitTimeoutAction.Abort;
        public SchemaCopyMode MaterializedDataSchemaCopyMode { get; set; } = SchemaCopyMode.OutsideTransactionAvoidSchemaLocks;
        public bool MaterializedDataMakeSchemaCopyNamesUnique { get; set; } = true;

        public string MaterializedDataLoadingSchema { get; set; } = "materializing_load";
        public string MaterializedDataLoadingTablePrefix { get; set; } = string.Empty;
        public string MaterializedDataLoadingTableSuffix { get; set; } = "_Loading";

        public string MaterializedDataDiscardingSchema { get; set; } = "materializing_discard";
        public string MaterializedDataDiscardingTablePrefix { get; set; } = string.Empty;
        public string MaterializedDataDiscardingTableSuffix { get; set; } = "_Discarding";

        public bool IsCloningIdentitySeedValueEnabled { get; set; } = true;
        public ISqlBulkHelpersConnectionProvider ConcurrentConnectionFactory { get; set; } = null;

        private int _maxConcurrentConnections = SqlBulkHelpersConfigConstants.DefaultMaxConcurrentConnections;
        /// <summary>
        /// Determines the maximum number of Concurrent Sql Connections that can be used (to boost performance) when
        /// Concurrent Connections are enabled and a valid ConcurrentConnectionFactory or ISqlBulkHelpersConnectionProvider is configured.
        /// </summary>
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

        /// <summary>
        /// The Materialized Data api can automatically handle Full Text Indexes if enabled, however
        /// Concurrent Connection support is also required and therefore a ConcurrentConnectionFactory
        /// or ISqlBulkHelpersConnectionProvider must also be provided.
        ///
        /// Recommended to use the SqlBulkHelpersConfig.EnableConcurrentSqlConnectionProcessing() convenience method(s) to enable this more easily!
        /// </summary>
        public bool IsFullTextIndexHandlingEnabled { get; set; } = false;
    }
}
