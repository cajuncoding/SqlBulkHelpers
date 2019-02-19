using System;
using System.Collections.Generic;
using System.Linq;
using System.Data;
using System.Data.SqlClient;

namespace SqlBulkHelpers
{
    //BBernard - Base Class for future flexibility...
    public abstract class BaseSqlBulkHelper<T> where T: BaseIdentityIdModel
    {
        protected SqlBulkHelpersTableDefinition GetTableSchemaDefinitionHelper(String tableName)
        {
            var tableDefinition = SqlBulkHelpersDBSchemaLoader.GetTableSchemaDefinition(tableName);
            if (tableDefinition == null) throw new ArgumentOutOfRangeException(nameof(tableName), $"The specified argument [{tableName}] is invalid.");
            return tableDefinition;
        }

        protected DataTable ConvertEntitiesToDatatableHelper(IEnumerable<T> entityList, SqlBulkHelpersColumnDefinition identityColumnDefinition)
        {
            var SqlBulkHelpersMapper = new SqlBulkHelpersObjectMapper();
            var dataTable = SqlBulkHelpersMapper.ConvertEntitiesToDatatable(entityList, identityColumnDefinition);
            return dataTable;
        }

        protected SqlBulkCopy CreateSqlBulkCopyHelper(DataTable dataTable, SqlBulkHelpersTableDefinition tableDefinition, SqlTransaction transaction)
        {
            var factory = new SqlBulkCopyFactory(); //Load with all Defaults from our Factory.
            var sqlBulkCopy = factory.CreateSqlBulkCopy(dataTable, tableDefinition, transaction);
            return sqlBulkCopy;
        }

        //TODO: BBernard - If beneficial, we can Add Caching here at this point to cache the fully formed Merge Queries!
        protected SqlMergeScriptResults BuildSqlMergeScriptsHelper(SqlBulkHelpersTableDefinition tableDefinition, SqlBulkHelpersMergeAction mergeAction)
        {
            var mergeScriptBuilder = new SqlBulkHelpersMergeScriptBuilder();
            var sqlScripts = mergeScriptBuilder.BuildSqlMergeScripts(tableDefinition, mergeAction);
            return sqlScripts;
        }

        //NOTE: This is Protected Class because it is ONLY needed by the SqlBulkHelper implementations with Merge Operations 
        //          for organized code when post-processing results.
        protected class MergeResult
        {
            public int RowNumber { get; set; }
            public int IdentityId { get; set; }
            public SqlBulkHelpersMergeAction MergeAction { get; set; }
        }

        protected List<T> PostProcessEntitiesWithMergeResults(List<T> entityList, List<MergeResult> mergeResultsList)
        {
            foreach (var mergeResult in mergeResultsList.Where(r => r.MergeAction.HasFlag(SqlBulkHelpersMergeAction.Insert)))
            {
                //NOTE: List is 0 (zero) based, but our RowNumber is 1 (one) based.
                var entity = entityList[mergeResult.RowNumber - 1];
                entity.Id = mergeResult.IdentityId;
            }

            //Return the Updated Entities List (for chainability) and easier to read code
            //NOTE: even though we have actually mutated the original list by reference this helps with code readability.
            return entityList;
        }

    }
}
