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
            PrimaryKeyConstraintDefinition primaryKeyConstraint,
            List<ForeignKeyConstraintDefinition> foreignKeyConstraints,
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
            PrimaryKeyConstraint = primaryKeyConstraint;
            ForeignKeyConstraints = (foreignKeyConstraints ?? new List<ForeignKeyConstraintDefinition>()).AsReadOnly();
            ColumnDefaultConstraints = (columnDefaultConstraints ?? new List<ColumnDefaultConstraintDefinition>()).AsReadOnly();
            ColumnCheckConstraints = (columnCheckConstraints ?? new List<ColumnCheckConstraintDefinition>()).AsReadOnly();
            TableIndexes = (tableIndexes ?? new List<TableIndexDefinition>()).AsReadOnly();

            //Derived Key/Constraint properties for Convenience/Fast Processing
            IdentityColumn = this.TableColumns.FirstOrDefault(c => c.IsIdentityColumn);

            //Initialize the Case-insensitive Dictionary for quickly looking up Columns...
            ColumnCaseInsensitiveDictionary = this.TableColumns.ToDictionary(c => c.ColumnName, c => c, StringComparer.OrdinalIgnoreCase);
        }

        public string TableSchema { get; }
        public string TableName { get; }
        public string TableFullyQualifiedName { get; }
        public IList<TableColumnDefinition> TableColumns { get; }
        public PrimaryKeyConstraintDefinition PrimaryKeyConstraint { get; }
        public IList<ForeignKeyConstraintDefinition> ForeignKeyConstraints { get; }
        public IList<ColumnDefaultConstraintDefinition> ColumnDefaultConstraints { get; }
        public IList<ColumnCheckConstraintDefinition> ColumnCheckConstraints { get; }
        public IList<TableIndexDefinition> TableIndexes { get; }

        //The following are derived references for Convenience...
        public TableColumnDefinition IdentityColumn { get; }

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
        public TableColumnDefinition(
            int ordinalPosition, 
            string columnName, 
            string dataType, 
            bool isIdentityColumn,
            int? charactersMaxLength,
            int? numericPrecision,
            int? numericPrecisionRadix,
            int? numericScale,
            int? dateTimePrecision
        )
        {
            this.OrdinalPosition = ordinalPosition;
            this.ColumnName = columnName;
            this.DataType = dataType;
            this.IsIdentityColumn = isIdentityColumn;
            CharacterMaxLength = charactersMaxLength;
            NumericPrecision = numericPrecision;
            NumericPrecisionRadix = numericPrecisionRadix;
            NumericScale = numericScale;
            DateTimePrecision = dateTimePrecision;
        }

        public int OrdinalPosition { get; }
        public string ColumnName { get; }
        public string DataType { get; }
        public bool IsIdentityColumn { get; }
        public int? CharacterMaxLength { get; }
        public int? NumericPrecision { get; }
        public int? NumericPrecisionRadix { get; }
        public int? NumericScale { get; }
        public int? DateTimePrecision { get; }

        public override string ToString()
        {
            return $"{this.ColumnName} [{this.DataType}]";
        }
    }

    [JsonConverter(typeof(JsonStringEnumConverter))]
    public abstract class KeyConstraintDefinition
    {
        protected KeyConstraintDefinition(string constraintName, KeyConstraintType constraintType, List<KeyColumnDefinition> keyColumns)
        {
            ConstraintName = constraintName;
            ConstraintType = constraintType;
            KeyColumns = keyColumns;
        }
        public string ConstraintName { get; }
        public KeyConstraintType ConstraintType { get; }
        public IList<KeyColumnDefinition> KeyColumns { get; }

        public override string ToString() => ConstraintName;
    }

    [JsonConverter(typeof(JsonStringEnumConverter))]
    public class PrimaryKeyConstraintDefinition : KeyConstraintDefinition
    {
        public PrimaryKeyConstraintDefinition(string constraintName, KeyConstraintType constraintType, List<KeyColumnDefinition> keyColumns)
            : base(constraintName, constraintType, keyColumns)
        {
        }
    }

    [JsonConverter(typeof(JsonStringEnumConverter))]
    public class ForeignKeyConstraintDefinition : KeyConstraintDefinition
    {
        public ForeignKeyConstraintDefinition(
            string constraintName, 
            KeyConstraintType constraintType,
            string referenceTableSchema,
            string referenceTableName,
            List<KeyColumnDefinition> keyColumns, 
            List<KeyColumnDefinition> referenceColumns,
            string referentialMatchOption, 
            string referentialUpdateRuleClause, 
            string referentialDeleteRuleClause
        ) : base(constraintName, constraintType, keyColumns)
        {
            ReferenceTableSchema = referenceTableSchema;
            ReferenceTableName = referenceTableName;
            ReferenceTableFullyQualifiedName = $"[{referenceTableSchema}].[{referenceTableName}]";
            ReferenceColumns = referenceColumns;
            ReferentialMatchOption = referentialMatchOption;
            ReferentialUpdateRuleClause = referentialUpdateRuleClause;
            ReferentialDeleteRuleClause = referentialDeleteRuleClause;
        }

        public string ReferenceTableSchema { get; }
        public string ReferenceTableName { get; }
        public string ReferenceTableFullyQualifiedName { get; }
        public string ReferentialMatchOption { get; }
        public string ReferentialUpdateRuleClause { get; }
        public string ReferentialDeleteRuleClause { get; }
        public IList<KeyColumnDefinition> ReferenceColumns { get; }
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

        public override string ToString() => ConstraintName;
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

        public override string ToString() => ConstraintName;
    }

    public class TableIndexDefinition
    {
        public TableIndexDefinition(
            int indexId, 
            string indexName, 
            bool isUnique, 
            bool isUniqueConstraint, 
            string filterDefinition, 
            List<IndexKeyColumnDefinition> keyColumns, 
            List<IndexIncludeColumnDefinition> includeColumns
        )
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
        public IList<IndexKeyColumnDefinition> KeyColumns { get; }
        public IList<IndexIncludeColumnDefinition> IncludeColumns { get; }

        public override string ToString() => IndexName;
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

    public class IndexIncludeColumnDefinition : KeyColumnDefinition
    {
        public IndexIncludeColumnDefinition(int ordinalPosition, string columnName)
            : base(ordinalPosition, columnName)
        {
        }
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

        public override string ToString() => $"[{OrdinalPosition}] {ColumnName}";
    }
}
