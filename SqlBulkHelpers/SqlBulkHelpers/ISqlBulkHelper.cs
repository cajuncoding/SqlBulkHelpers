using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace SqlBulkHelpers
{
    public interface ISqlBulkHelper<T> where T: class
    {
        #region Async Methods
        Task<IEnumerable<T>> BulkInsertAsync(IEnumerable<T> entityList, String tableName, SqlTransaction transaction);
        Task<IEnumerable<T>> BulkUpdateAsync(IEnumerable<T> entityList, String tableName, SqlTransaction transaction);
        Task<IEnumerable<T>> BulkInsertOrUpdateAsync(IEnumerable<T> entityList, String tableName, SqlTransaction transaction);
        #endregion

        #region Synchronous Methods
        IEnumerable<T> BulkInsert(IEnumerable<T> entityList, String tableName, SqlTransaction transaction);
        IEnumerable<T> BulkUpdate(IEnumerable<T> entityList, String tableName, SqlTransaction transaction);
        IEnumerable<T> BulkInsertOrUpdate(IEnumerable<T> entityList, String tableName, SqlTransaction transaction);
        #endregion
    }
}
