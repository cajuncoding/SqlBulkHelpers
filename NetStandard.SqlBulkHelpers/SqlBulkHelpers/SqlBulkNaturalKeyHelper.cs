using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Threading.Tasks;

namespace SqlBulkHelpers
{
    //TODO: BBernard - IF Needed for flexibility with tables that use Natural Keys (vs Surrogate Key via Identity Id) 
    //          WE COULD IMPLEMENT this as a SqlBulkHelper that has Support for PrimaryKey lookups vs Identity PKeys!
    //          But this will take a bit more work to determine the PKey fields from teh DB Schema, and dynamically process them
    //          in the MERGE query instead of using the Surrogate key via Identity Id (which does simplify this process!).
    public class SqlBulkNaturalKeyHelper<T> : BaseSqlBulkHelper<T>, ISqlBulkHelper<T> where T: class
    {
        private const string NOT_IMPLEMENTED_MESSAGE = "Potential future enhancement may be added to support Natural Keys within the existing framework, " +
                                              "however for now it's easier to manually implement the SqlBulkCopy directly for collections that " +
                                              "do not use Identity columns that need to be returned. This is easier by using the utility classes provided" +
                                              "as publically accessible helpers in this library (e.g. SqlBulkHelpersObjectMapper can be used to convert " +
                                              "Lists of Entities to Datatable) ";

        public SqlBulkNaturalKeyHelper(ISqlBulkHelpersDBSchemaLoader sqlDbSchemaLoader)
            : base(sqlDbSchemaLoader)
        {
            throw new NotImplementedException(NOT_IMPLEMENTED_MESSAGE);
        }

        public SqlBulkNaturalKeyHelper(ISqlBulkHelpersConnectionProvider sqlBulkHelpersConnectionProvider)
            : base(sqlBulkHelpersConnectionProvider)
        {
            throw new NotImplementedException(NOT_IMPLEMENTED_MESSAGE);
        }

        public SqlBulkNaturalKeyHelper()
            : base()
        {
            throw new NotImplementedException(NOT_IMPLEMENTED_MESSAGE);
        }

        public virtual Task<IEnumerable<T>> BulkInsertAsync(
            IEnumerable<T> entityList, 
            string tableName, 
            SqlTransaction transaction, 
            SqlMergeMatchQualifierExpression matchQualifierExpression = null
        )
        {
            throw new NotImplementedException();
        }

        public virtual IEnumerable<T> BulkInsert(
            IEnumerable<T> entityList,
            string tableName,
            SqlTransaction transaction,
            SqlMergeMatchQualifierExpression matchQualifierExpression = null
        )
        {
            throw new NotImplementedException();
        }

        public virtual IEnumerable<T> BulkInsertOrUpdate(
            IEnumerable<T> entityList,
            string tableName,
            SqlTransaction transaction,
            SqlMergeMatchQualifierExpression matchQualifierExpression = null
        )
        {
            throw new NotImplementedException();
        }

        public virtual Task<IEnumerable<T>> BulkInsertOrUpdateAsync(
            IEnumerable<T> entityList,
            string tableName,
            SqlTransaction transaction,
            SqlMergeMatchQualifierExpression matchQualifierExpression = null
        )
        {
            throw new NotImplementedException();
        }

        public virtual IEnumerable<T> BulkUpdate(
            IEnumerable<T> entityList,
            string tableName,
            SqlTransaction transaction,
            SqlMergeMatchQualifierExpression matchQualifierExpression = null
        )
        {
            throw new NotImplementedException();
        }

        public virtual Task<IEnumerable<T>> BulkUpdateAsync(
            IEnumerable<T> entityList,
            string tableName,
            SqlTransaction transaction,
            SqlMergeMatchQualifierExpression matchQualifierExpression = null
        )
        {
            throw new NotImplementedException();
        }

        public virtual IEnumerable<T> BulkInsertOrUpdate(
            IEnumerable<T> entityList, 
            String tableName, 
            SqlBulkHelpersMergeAction mergeAction, 
            SqlTransaction transaction,
            SqlMergeMatchQualifierExpression matchQualifierExpression = null
        )
        {
            throw new NotImplementedException();
        }
    }
}
