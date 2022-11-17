using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.Json.Serialization;

namespace SqlBulkHelpers
{
    public class SqlBulkHelpersTableDefinition
    {
        protected Dictionary<string, TableColumnDefinition> ColumnCaseInsensitiveDictionary { get; }

        public SqlBulkHelpersTableDefinition(
            string tableSchema, 
            string tableName, 
            List<TableColumnDefinition> tableColumns, 
            List<KeyConstraintDefinition> keyConstraints,
            List<ColumnDefaultConstraintDefinition> columnDefaultConstraints,
            List<ColumnCheckConstraintDefinition> columnCheckConstraints,
            List<TableIndexDefinition> tableIndexes)
        {
            TableSchema = tableSchema.AssertArgumentIsNotNullOrWhiteSpace(nameof(tableSchema));
            TableName = tableName.AssertArgumentIsNotNullOrWhiteSpace(nameof(tableName));
            //Derived Table properties...
            TableFullyQualifiedName = $"[{TableSchema}].[{TableName}]";

            //Ensure that the Columns, Constraints, etc. collections are always NullSafe and is Immutable/ReadOnly!
            TableColumns = (tableColumns ?? new List<TableColumnDefinition>()).AsReadOnly();
            KeyConstraints = (keyConstraints ?? new List<KeyConstraintDefinition>()).AsReadOnly();
            ColumnDefaultConstraints = (columnDefaultConstraints ?? new List<ColumnDefaultConstraintDefinition>()).AsReadOnly();
            ColumnCheckConstraints = (columnCheckConstraints ?? new List<ColumnCheckConstraintDefinition>()).AsReadOnly();
            TableIndexes = (tableIndexes ?? new List<TableIndexDefinition>()).AsReadOnly();

            //Derived Key/Constraint properties for Convenience/Fast Processing
            IdentityColumn = this.TableColumns.FirstOrDefault(c => c.IsIdentityColumn);
            PrimaryKeyConstraint = this.KeyConstraints.FirstOrDefault(kc => kc.ConstraintType == KeyConstraintType.PrimaryKey);

            //Initialize the Case-insensitive Dictionary for quickly looking up Columns...
            ColumnCaseInsensitiveDictionary = this.TableColumns.ToDictionary(c => c.ColumnName, c => c, StringComparer.OrdinalIgnoreCase);
        }

        public string TableSchema { get; }
        public string TableName { get; }
        public string TableFullyQualifiedName { get; }
        public IList<TableColumnDefinition> TableColumns { get; }
        public IList<KeyConstraintDefinition> KeyConstraints { get; }
        public IList<ColumnDefaultConstraintDefinition> ColumnDefaultConstraints { get; }
        public IList<ColumnCheckConstraintDefinition> ColumnCheckConstraints { get; }
        public IList<TableIndexDefinition> TableIndexes { get; }

        //The following are derived references for Convenience...
        public TableColumnDefinition IdentityColumn { get; }
        public KeyConstraintDefinition PrimaryKeyConstraint { get; }

        //The following are Helper Methods for processing...
        public IList<string> GetColumnNames(bool includeIdentityColumn = true)
        {
            IEnumerable<string> results = includeIdentityColumn 
                ? this.TableColumns.Select(c => c.ColumnName)
                : this.TableColumns.Where(c => !c.IsIdentityColumn).Select(c => c.ColumnName);

            //Ensure that our List is Immutable/ReadOnly!
            return results.ToList().AsReadOnly();
        }

        public TableColumnDefinition FindColumnCaseInsensitive(string columnName)
            => ColumnCaseInsensitiveDictionary.GetValueOrDefault(columnName);

        public override string ToString() => this.TableFullyQualifiedName;
    }

    public class TableColumnDefinition
    {
        public TableColumnDefinition(int ordinalPosition, string columnName, string dataType, bool isIdentityColumn)
        {
            this.OrdinalPosition = ordinalPosition;
            this.ColumnName = columnName;
            this.DataType = dataType;
            this.IsIdentityColumn = isIdentityColumn;
        }

        public int OrdinalPosition { get; }
        public string ColumnName { get; }
        public string DataType { get; }
        public bool IsIdentityColumn { get; }

        public override string ToString()
        {
            return $"{this.ColumnName} [{this.DataType}]";
        }
    }

    [JsonConverter(typeof(JsonStringEnumConverter))]
    public class KeyConstraintDefinition
    {
        public KeyConstraintDefinition(string constraintName, KeyConstraintType constraintType, List<KeyColumnDefinition> keyColumns)
        {
            ConstraintName = constraintName;
            ConstraintType = constraintType;
            KeyColumns = keyColumns;
        }
        public string ConstraintName { get; }
        public KeyConstraintType ConstraintType { get; }
        public IList<KeyColumnDefinition> KeyColumns { get; }
    }

    public enum KeyConstraintType
    {
        PrimaryKey, 
        ForeignKey
    }

    public class ColumnDefaultConstraintDefinition
    {
        public ColumnDefaultConstraintDefinition(string constraintName, string columnName, string definition)
        {
            ConstraintName = constraintName;
            ColumnName = columnName;
            Definition = definition;
        }
        public string ConstraintName { get; }
        public string ColumnName { get; }
        public string Definition { get; }
    }

    public class ColumnCheckConstraintDefinition
    {
        public ColumnCheckConstraintDefinition(string constraintName, string checkClause)
        {
            ConstraintName = constraintName;
            CheckClause = checkClause;
        }
        public string ConstraintName { get; }
        public string CheckClause { get; }
    }

    public class TableIndexDefinition
    {
        public TableIndexDefinition(int indexId, string indexName, bool isUnique, bool isUniqueConstraint, string filterDefinition, List<KeyColumnDefinition> keyColumns, List<IncludeColumnDefinition> includeColumns)
        {
            IndexId = indexId;
            IndexName = indexName;
            IsUnique = isUnique;
            IsUniqueConstraint = isUniqueConstraint;
            FilterDefinition = filterDefinition;
            KeyColumns = keyColumns;
            IncludeColumns = includeColumns;
        }

        public int IndexId { get; }
        public string IndexName { get; }
        public bool IsUnique { get; }
        public bool IsUniqueConstraint { get; }
        public string FilterDefinition { get; }
        public IList<KeyColumnDefinition> KeyColumns { get; }
        public IList<IncludeColumnDefinition> IncludeColumns { get; }
    }

    public class IndexKeyColumnDefinition : KeyColumnDefinition
    {
        public IndexKeyColumnDefinition(int ordinalPosition, string columnName, bool isDescending)
            : base(ordinalPosition, columnName)
        {
            IsDescending = isDescending;
        }
        public bool IsDescending { get; }
    }

    public class KeyColumnDefinition
    {
        public KeyColumnDefinition(int ordinalPosition, string columnName)
        {
            OrdinalPosition = ordinalPosition;
            ColumnName = columnName;
        }
        public int OrdinalPosition { get; }
        public string ColumnName { get; }
    }

    public class IncludeColumnDefinition : KeyColumnDefinition
    {
        public IncludeColumnDefinition(int ordinalPosition, string columnName)
            : base(ordinalPosition, columnName)
        {
        }
    }

}
