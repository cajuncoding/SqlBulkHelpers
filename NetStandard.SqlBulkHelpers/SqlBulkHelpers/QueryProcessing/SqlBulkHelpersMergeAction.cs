using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace SqlBulkHelpers
{
    [Flags]
    public enum SqlBulkHelpersMergeAction
    {
        Insert = 1,
        Update = 2,
        Delete = 4,
        InsertOrUpdate = Insert | Update
    }

    public class SqlBulkHelpersMerge
    {
        public static SqlBulkHelpersMergeAction ParseMergeActionString(String actionString)
        {
            Enum.TryParse<SqlBulkHelpersMergeAction>(actionString, true, out var mergeAction);
            return mergeAction;
        }
    }
}
