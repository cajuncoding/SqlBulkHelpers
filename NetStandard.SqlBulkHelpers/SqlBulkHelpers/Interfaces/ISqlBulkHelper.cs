using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Threading.Tasks;

namespace SqlBulkHelpers
{
    public interface ISqlBulkHelper<T> where T: class
    {
        #region Dependency / Helper Methods
        SqlBulkHelpersTableDefinition GetTableSchemaDefinition(String tableName);
        #endregion

        #region Async Operation Methods
        Task<IEnumerable<T>> BulkInsertAsync(
            IEnumerable<T> entityList, 
            String tableName, 
            SqlTransaction transaction, 
            SqlMergeMatchQualifierExpression matchQualifierExpression = null
        );

        Task<IEnumerable<T>> BulkUpdateAsync(
            IEnumerable<T> entityList,
            String tableName,
            SqlTransaction transaction,
            SqlMergeMatchQualifierExpression matchQualifierExpression = null
        );

        Task<IEnumerable<T>> BulkInsertOrUpdateAsync(
            IEnumerable<T> entityList,
            String tableName,
            SqlTransaction transaction,
            SqlMergeMatchQualifierExpression matchQualifierExpression = null
        );
        #endregion

        #region Synchronous Operation Methods
        IEnumerable<T> BulkInsert(
            IEnumerable<T> entityList,
            String tableName,
            SqlTransaction transaction,
            SqlMergeMatchQualifierExpression matchQualifierExpression = null
        );

        IEnumerable<T> BulkUpdate(
            IEnumerable<T> entityList,
            String tableName,
            SqlTransaction transaction,
            SqlMergeMatchQualifierExpression matchQualifierExpression = null
        );

        IEnumerable<T> BulkInsertOrUpdate(
            IEnumerable<T> entityList,
            String tableName,
            SqlTransaction transaction,
            SqlMergeMatchQualifierExpression matchQualifierExpression = null
        );
        #endregion
    }
}
