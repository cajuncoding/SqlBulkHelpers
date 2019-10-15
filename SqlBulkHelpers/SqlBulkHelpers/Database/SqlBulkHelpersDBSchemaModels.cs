using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace SqlBulkHelpers
{
    public class SqlBulkHelpersTableDefinition
    {
        private readonly ILookup<String, SqlBulkHelpersColumnDefinition> _caseInsensitiveColumnLookup;

        public SqlBulkHelpersTableDefinition(String tableSchema, String tableName, List<SqlBulkHelpersColumnDefinition> columns)
        {
            this.TableSchema = tableSchema;
            this.TableName = tableName;
            //Ensure that the Columns Collection is always NullSafe and is Immutable/ReadOnly!
            this.Columns = (columns ?? new List<SqlBulkHelpersColumnDefinition>()).AsReadOnly();

            //Initialize Helper Elements for Fast Processing (Cached Immutable data references)
            this.IdentityColumn = this.Columns.FirstOrDefault(c => c.IsIdentityColumn);
            this._caseInsensitiveColumnLookup = this.Columns.ToLookup(c => c.ColumnName.ToLower());
        }
        public String TableSchema { get; private set; }
        public String TableName { get; private set; }
        public IList<SqlBulkHelpersColumnDefinition> Columns { get; private set; }
        public SqlBulkHelpersColumnDefinition IdentityColumn { get; private set; }

        public IList<String> GetColumnNames(bool includeIdentityColumn = true)
        {
            IEnumerable<String> results = includeIdentityColumn 
                                            ? this.Columns.Select(c => c.ColumnName)
                                            : this.Columns.Where(c => !c.IsIdentityColumn).Select(c => c.ColumnName);

            //Ensure that our List is Immutable/ReadOnly!
            return results.ToList().AsReadOnly();
        }

        public SqlBulkHelpersColumnDefinition FindColumnCaseInsensitive(String columnName)
        {
            var lookup = _caseInsensitiveColumnLookup;
            return lookup[columnName.ToLower()]?.FirstOrDefault();
        }

        public override string ToString()
        {
            return this.TableName;
        }
    }

    public class SqlBulkHelpersColumnDefinition
    {
        public SqlBulkHelpersColumnDefinition(String columnName, int ordinalPosition, String dataType, bool isIdentityColumn)
        {
            this.ColumnName = columnName;
            this.OrdinalPosition = ordinalPosition;
            this.DataType = dataType;
            this.IsIdentityColumn = isIdentityColumn;
        }

        public String ColumnName { get; private set; }
        public int OrdinalPosition { get; private set; }
        public String DataType { get; private set; }
        public bool IsIdentityColumn { get; private set; }

        public override string ToString()
        {
            return $"{this.ColumnName} [{this.DataType}]";
        }
    }
}
