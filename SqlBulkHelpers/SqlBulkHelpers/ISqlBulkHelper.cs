using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace SqlBulkHelpers
{
    public interface ISqlBulkHelper<T> where T : BaseIdentityIdModel
    {
        Task<List<T>> BulkInsertAsync(List<T> entityList, String tableName, SqlTransaction transaction);
        Task<List<T>> BulkUpdateAsync(List<T> entityList, String tableName, SqlTransaction transaction);
        Task<List<T>> BulkInsertOrUpdateAsync(List<T> entityList, String tableName, SqlTransaction transaction);
    }
}
