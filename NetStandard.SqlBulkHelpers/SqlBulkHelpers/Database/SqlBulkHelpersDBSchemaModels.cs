using System;
using System.Collections.Generic;
using System.Linq;

namespace SqlBulkHelpers
{
    public class SqlBulkHelpersTableDefinition
    {
        private readonly ILookup<string, SqlBulkHelpersColumnDefinition> _columnLowercaseLookup;

        public SqlBulkHelpersTableDefinition(string tableSchema, string tableName, List<SqlBulkHelpersColumnDefinition> columns)
        {
            this.TableSchema = tableSchema.AssertArgumentIsNotNullOrWhiteSpace(nameof(tableSchema));
            this.TableName = tableName.AssertArgumentIsNotNullOrWhiteSpace(nameof(tableName));
            //Ensure that the Columns Collection is always NullSafe and is Immutable/ReadOnly!
            this.Columns = (columns ?? new List<SqlBulkHelpersColumnDefinition>()).AsReadOnly();

            //Initialize Helper Elements for Fast Processing (Cached Immutable data references)
            this.IdentityColumn = this.Columns.FirstOrDefault(c => c.IsIdentityColumn);
            this._columnLowercaseLookup = this.Columns.ToLookup(c => c.ColumnName.ToLowerInvariant());
            this.TableFullyQualifiedName = $"[{TableSchema}].[{TableName}]";
        }

        public string TableSchema { get; private set; }
        public string TableName { get; private set; }
        public string TableFullyQualifiedName { get; private set; }
        public IList<SqlBulkHelpersColumnDefinition> Columns { get; private set; }
        public SqlBulkHelpersColumnDefinition IdentityColumn { get; private set; }

        public IList<string> GetColumnNames(bool includeIdentityColumn = true)
        {
            IEnumerable<string> results = includeIdentityColumn 
                ? this.Columns.Select(c => c.ColumnName)
                : this.Columns.Where(c => !c.IsIdentityColumn).Select(c => c.ColumnName);

            //Ensure that our List is Immutable/ReadOnly!
            return results.ToList().AsReadOnly();
        }

        public SqlBulkHelpersColumnDefinition FindColumnCaseInsensitive(string columnName)
            => _columnLowercaseLookup[columnName.ToLowerInvariant()]?.FirstOrDefault();

        public override string ToString()
            => this.TableFullyQualifiedName;
    }

    public class SqlBulkHelpersColumnDefinition
    {
        public SqlBulkHelpersColumnDefinition(string columnName, int ordinalPosition, string dataType, bool isIdentityColumn)
        {
            this.ColumnName = columnName;
            this.OrdinalPosition = ordinalPosition;
            this.DataType = dataType;
            this.IsIdentityColumn = isIdentityColumn;
        }

        public string ColumnName { get; private set; }
        public int OrdinalPosition { get; private set; }
        public string DataType { get; private set; }
        public bool IsIdentityColumn { get; private set; }

        public override string ToString()
        {
            return $"{this.ColumnName} [{this.DataType}]";
        }
    }
}
