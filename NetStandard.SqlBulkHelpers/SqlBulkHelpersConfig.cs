using System;
using Microsoft.Data.SqlClient;

namespace SqlBulkHelpers
{
    public interface ISqlBulkHelpersConfig
    {
        int SqlBulkBatchSize { get; } //General guidance is that 2000-5000 is efficient enough.
        int SqlBulkPerBatchTimeoutSeconds { get; }
        bool SqlBulkTableLockEnabled { get; }
        SqlBulkCopyOptions SqlBulkCopyOptions { get; }

        int MaterializeDataStructureProcessingTimeoutSeconds { get; }
        string MaterializedDataDefaultLoadingSchema { get; }
        string MaterializedDataDefaultTempHoldingSchema { get; }
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

        public static void ConfigureDefaults(Action<SqlBulkHelpersConfig> configAction)
        {
            DefaultConfig = Create(configAction);
        }

        public int SqlBulkBatchSize { get; set; } = 2000; //General guidance is that 2000-5000 is efficient enough.

        public int SqlBulkPerBatchTimeoutSeconds { get; set; }

        public bool SqlBulkTableLockEnabled {
            get => SqlBulkCopyOptions.HasFlag(SqlBulkCopyOptions.TableLock);
            set => SqlBulkCopyOptions |= SqlBulkCopyOptions.TableLock;
        }

        //NOTE: Default to TableLock to be enabled since our process always Writes to the Temp Table!
        public SqlBulkCopyOptions SqlBulkCopyOptions { get; set; } = SqlBulkCopyOptions.Default | SqlBulkCopyOptions.TableLock;
        
        public int MaterializeDataStructureProcessingTimeoutSeconds { get; set; } = 30;
        public string MaterializedDataDefaultLoadingSchema { get; set; } = "dbo_materializing";
        public string MaterializedDataDefaultTempHoldingSchema { get; set; } = "dbo_materializing_temp";
    }
}
