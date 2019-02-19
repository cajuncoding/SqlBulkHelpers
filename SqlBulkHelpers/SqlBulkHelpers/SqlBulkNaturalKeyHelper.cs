using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace SqlBulkHelpers
{
    //TODO: BBenard - IF Needed for flexibility with tables that use Natrual Keys (vs Surrogate Key via Identity Id) 
    //          WE COULD IMPLEMENT this as a SqlBulkHelper that has Support for PrimaryKey lookups vs Identity PKeys!
    //          But this will take a bit more work to determine the PKey fields from teh DB Schema, and dynamically process them
    //          in the MERGE query instead of using the Surrogate key via Identity Id (which does simplify this process!).
    public class SqlBulkNaturalKeyHelper<T> : BaseSqlBulkHelper<T>, ISqlBulkHelper<T> where T: class
    {
        public Task<IEnumerable<T>> BulkInsertAsync(IEnumerable<T> entityList, string tableName, SqlTransaction transaction)
        {
            throw new NotImplementedException();
        }

        public IEnumerable<T> BulkInsert(IEnumerable<T> entityList, string tableName, SqlTransaction transaction)
        {
            throw new NotImplementedException();
        }

        public IEnumerable<T> BulkInsertOrUpdate(IEnumerable<T> entityList, string tableName, SqlTransaction transaction)
        {
            throw new NotImplementedException();
        }

        public Task<IEnumerable<T>> BulkInsertOrUpdateAsync(IEnumerable<T> entityList, string tableName, SqlTransaction transaction)
        {
            throw new NotImplementedException();
        }

        public IEnumerable<T> BulkUpdate(IEnumerable<T> entityList, string tableName, SqlTransaction transaction)
        {
            throw new NotImplementedException();
        }

        public Task<IEnumerable<T>> BulkUpdateAsync(IEnumerable<T> entityList, string tableName, SqlTransaction transaction)
        {
            throw new NotImplementedException();
        }

        private IEnumerable<T> BulkInsertOrUpdate(IEnumerable<T> entityList, String tableName, SqlBulkHelpersMergeAction mergeAction, SqlTransaction transaction)
        {
            throw new NotImplementedException();
        }
    }
}
