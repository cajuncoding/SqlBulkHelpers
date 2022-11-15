using System;

namespace SqlBulkHelpers.SqlBulkHelpers.Attributes
{
    [AttributeUsage(AttributeTargets.Class, Inherited = true, AllowMultiple = false)]
    public class SqlBulkTableAttribute : Attribute
    {
        public string SchemaName { get; protected set; }
        public string TableName { get; protected set; }
        public string FullyQualifiedTableName { get; protected set; }
        public bool UniqueMatchMergeValidationEnabled { get; protected set; }

        public SqlBulkTableAttribute(string schemaName, string tableName, bool uniqueMatchMergeValidationEnabled = true)
            : this($"[{schemaName}].[{tableName}]")
        {
            this.UniqueMatchMergeValidationEnabled = uniqueMatchMergeValidationEnabled;
        }

        public SqlBulkTableAttribute(string tableName)
        {
            (this.SchemaName, this.TableName, this.FullyQualifiedTableName) = tableName.ParseAsTableNameTerm();
        }
    }
}
