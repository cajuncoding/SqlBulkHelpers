using System;
using SqlBulkHelpers.CustomExtensions;

namespace SqlBulkHelpers
{
    [AttributeUsage(AttributeTargets.Class, Inherited = true, AllowMultiple = false)]
    public class SqlBulkTableAttribute : Attribute
    {
        public string SchemaName { get; protected set; }
        public string TableName { get; protected set; }
        public string FullyQualifiedTableName { get; protected set; }
        public bool UniqueMatchMergeValidationEnabled { get; protected set; }

        public SqlBulkTableAttribute(string schemaName, string tableName, bool uniqueMatchMergeValidationEnabled = true)
            : this($"[{schemaName.TrimTableNameTerm()}].[{tableName.TrimTableNameTerm()}]")
        {
            this.UniqueMatchMergeValidationEnabled = uniqueMatchMergeValidationEnabled;
        }

        public SqlBulkTableAttribute(string tableName, bool uniqueMatchMergeValidationEnabled = true)
        {
            var tableNameTerm = tableName.ParseAsTableNameTerm();
            this.SchemaName = tableNameTerm.SchemaName;
            this.TableName = tableNameTerm.TableName;
            this.FullyQualifiedTableName = tableNameTerm.FullyQualifiedTableName;
            this.UniqueMatchMergeValidationEnabled = uniqueMatchMergeValidationEnabled;
        }
    }
}
