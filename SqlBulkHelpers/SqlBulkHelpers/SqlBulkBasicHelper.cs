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
    public class SqlBulkBasicHelper<T> : BaseSqlBulkHelper<T>, ISqlBulkHelper<T> where T: class
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

        //public virtual async void BulkDeleteAsync(IEnumerable<T> entities)
        //{
        //    string idProperty = Activator.CreateInstance<T>().GetIdProperty();

        //    String ids = String.Join(",", entities.Select(e => e.Id));

        //    String sql = $"DELETE FROM {RepositoryTypeName} WHERE {idProperty} IN ({ids})";

        //    await _connection.ExecuteAsync(sql, null, _transaction);
        //}


        //public virtual IEnumerable<T> BulkInsertWithoutIdentity(IEnumerable<T> entities)
        //{
        //    DataTable dataTable = new DataTable();
        //    dataTable.AddIntoTable(entities);

        //    using (SqlBulkCopy sqlBulk = CreateSqlBulkCopyHelper(dataTable))
        //    {
        //        sqlBulk.DestinationTableName = RepositoryTypeName;
        //        sqlBulk.WriteToServer(dataTable);
        //    }

        //    return entities;
        //}

        //public virtual async Task<IEnumerable<T>> BulkInsertWithoutIdentityAsync(IEnumerable<T> entities)
        //{
        //    DataTable dataTable = new DataTable();
        //    dataTable.AddIntoTable(entities);

        //    using (SqlBulkCopy sqlBulk = CreateSqlBulkCopyHelper(dataTable))
        //    {
        //        sqlBulk.DestinationTableName = RepositoryTypeName;
        //        await sqlBulk.WriteToServerAsync(dataTable);
        //    }

        //    return entities;
        //}

        //private SqlBulkCopy CreateSqlBulkCopyHelper(DataTable dataTable)
        //{
        //    SqlBulkCopy sqlBulk = new SqlBulkCopy((SqlConnection)_connection, SqlBulkCopyOptions.Default, (SqlTransaction)_transaction);

        //    //TODO: BBernard - REFACTOR to use the SqlBulkHelpers class SqlBulkHelpersDBSchemaLoader
        //    foreach (string destinationColumn in Constants.DatabaseTablesAndColumns[RepositoryTypeName])
        //    {
        //        //TODO: Refactor this loop which is inefficient becasue it loops/casts the DataColumns every time inside the parent loop.
        //        DataColumn sourceColumn = dataTable.Columns.Cast<DataColumn>()
        //                .FirstOrDefault(c => c.ColumnName.ToLower().Equals(destinationColumn.ToLower()));

        //        if (sourceColumn != null)
        //        {
        //            sqlBulk.ColumnMappings.Add(sourceColumn.ColumnName, destinationColumn);
        //        }
        //    }

        //    return sqlBulk;
        //}
    }
}
