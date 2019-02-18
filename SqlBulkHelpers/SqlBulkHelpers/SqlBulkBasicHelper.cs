using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace SqlBulkHelpers
{
    //TODO: BBenard - IF Needed (for performance of non-identity tables) WE COULD IMPLEMENT this as a SqlBulkHelper 
    //          that simply inserts raw data with NO help for Identity Data!  Initial benchmarks show that there is little practical
    //          value in this for normal application usages . . . likely more valuable for Data Migration Activities, etc.
    public class SqlBulkBasicHelper<T> : ISqlBulkHelper<T> where T: BaseIdentityIdModel
    {
        public Task<List<T>> BulkInsertAsync(List<T> entityList, string tableName, SqlTransaction transaction)
        {
            throw new NotImplementedException();
        }

        public List<T> BulkInsert(List<T> entityList, string tableName, SqlTransaction transaction)
        {
            throw new NotImplementedException();
        }

        public List<T> BulkInsertOrUpdate(List<T> entityList, string tableName, SqlTransaction transaction)
        {
            throw new NotImplementedException();
        }

        public Task<List<T>> BulkInsertOrUpdateAsync(List<T> entityList, string tableName, SqlTransaction transaction)
        {
            throw new NotImplementedException();
        }

        public List<T> BulkUpdate(List<T> entityList, string tableName, SqlTransaction transaction)
        {
            throw new NotImplementedException();
        }

        public Task<List<T>> BulkUpdateAsync(List<T> entityList, string tableName, SqlTransaction transaction)
        {
            throw new NotImplementedException();
        }
    }
}
