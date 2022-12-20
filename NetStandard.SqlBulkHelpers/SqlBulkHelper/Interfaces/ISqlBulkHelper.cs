using System;
using System.Collections.Generic;
using Microsoft.Data.SqlClient;
using System.Threading.Tasks;

namespace SqlBulkHelpers
{
    public interface ISqlBulkHelper<T> where T: class
    {
        #region Dependency / Helper Methods
        SqlBulkHelpersTableDefinition GetTableSchemaDefinition(SqlTransaction sqlTransaction, string tableName = null);
        #endregion

        #region Async Operation Methods
        [Obsolete("This method is from v1 API and is deprecated, it will be removed eventually as it is replaced by the overload with optional parameters.")]
        Task<IEnumerable<T>> BulkInsertAsync(
            IEnumerable<T> entityList, 
            String tableName,
            SqlTransaction transaction, 
            SqlMergeMatchQualifierExpression matchQualifierExpression = null
        );

        [Obsolete("This method is from v1 API and is deprecated, it will be removed eventually as it is replaced by the overload with optional parameters.")]
        Task<IEnumerable<T>> BulkUpdateAsync(
            IEnumerable<T> entityList,
            String tableName,
            SqlTransaction transaction,
            SqlMergeMatchQualifierExpression matchQualifierExpression = null
        );

        [Obsolete("This method is from v1 API and is deprecated, it will be removed eventually as it is replaced by the overload with optional parameters.")]
        Task<IEnumerable<T>> BulkInsertOrUpdateAsync(
            IEnumerable<T> entityList,
            String tableName,
            SqlTransaction transaction,
            SqlMergeMatchQualifierExpression matchQualifierExpression = null
        );
        #endregion

        #region Synchronous Operation Methods
        [Obsolete("Use of Sync I/O is strongly discouraged. In addition, this method is from v1 API and is deprecated, it will be removed eventually as it is replaced by the overload with optional parameters.")]
        IEnumerable<T> BulkInsert(
            IEnumerable<T> entityList,
            String tableName,
            SqlTransaction transaction,
            SqlMergeMatchQualifierExpression matchQualifierExpression = null
        );

        [Obsolete("Use of Sync I/O is strongly discouraged. In addition, this method is from v1 API and is deprecated, it will be removed eventually as it is replaced by the overload with optional parameters.")]
        IEnumerable<T> BulkUpdate(
            IEnumerable<T> entityList,
            String tableName,
            SqlTransaction transaction,
            SqlMergeMatchQualifierExpression matchQualifierExpression = null
        );

        [Obsolete("Use of Sync I/O is strongly discouraged. In addition, this method is from v1 API and is deprecated, it will be removed eventually as it is replaced by the overload with optional parameters.")]
        IEnumerable<T> BulkInsertOrUpdate(
            IEnumerable<T> entityList,
            String tableName,
            SqlTransaction transaction,
            SqlMergeMatchQualifierExpression matchQualifierExpression = null
        );
        #endregion
    }
}
