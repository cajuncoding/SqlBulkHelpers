using System;

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
        public static SqlBulkHelpersMergeAction ParseMergeActionString(string actionString)
        {
            switch (actionString.ToLowerInvariant())
            {
                case "insertorupdate": return SqlBulkHelpersMergeAction.InsertOrUpdate;
                case "insert": return SqlBulkHelpersMergeAction.Insert;
                case "update": return SqlBulkHelpersMergeAction.Update;
                case "delete": return SqlBulkHelpersMergeAction.Delete;
                //Attempt an InsertOrUpdate by Default
                default: return SqlBulkHelpersMergeAction.InsertOrUpdate;
            }
        }
    }
}
