using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace SqlBulkHelpers
{
    //TODO: BBenard - TO BE IMPLEMENTED as an Sql Bulk Helper that simply inserts raw data with NO help for Identity Data!
    public class SqlBulkBasicHelper<T> : ISqlBulkHelper<T> where T: BaseIdentityIdModel
    {
        public Task<List<T>> BulkInsertAsync(List<T> entityList, string tableName, SqlTransaction transaction)
        {
            throw new NotImplementedException();
        }

        public Task<List<T>> BulkInsertOrUpdateAsync(List<T> entityList, string tableName, SqlTransaction transaction)
        {
            throw new NotImplementedException();
        }

        public Task<List<T>> BulkUpdateAsync(List<T> entityList, string tableName, SqlTransaction transaction)
        {
            throw new NotImplementedException();
        }
    }
}
