﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.Json.Serialization;
using SqlBulkHelpers.CustomExtensions;

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
            //Initialize our Table Name Term properties...
            TableNameTerm = TableNameTerm.From(
                tableSchema.AssertArgumentIsNotNullOrWhiteSpace(nameof(tableSchema)),
                tableName.AssertArgumentIsNotNullOrWhiteSpace(nameof(tableName))
            );

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

        public TableNameTerm TableNameTerm { get; }
        public string TableSchema => TableNameTerm.SchemaName;
        public string TableName => TableNameTerm.TableName;
        public string TableFullyQualifiedName => TableNameTerm.FullyQualifiedTableName;
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

        public override string ToString() => TableNameTerm.FullyQualifiedTableName;
    }

    public class TableColumnDefinition
    {
        public TableColumnDefinition(
            string sourceTableSchema,
            string sourceTableName,
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
            SourceTableSchema = sourceTableSchema;
            SourceTableName = sourceTableName;
            OrdinalPosition = ordinalPosition;
            ColumnName = columnName;
            DataType = dataType;
            IsIdentityColumn = isIdentityColumn;
            CharacterMaxLength = charactersMaxLength;
            NumericPrecision = numericPrecision;
            NumericPrecisionRadix = numericPrecisionRadix;
            NumericScale = numericScale;
            DateTimePrecision = dateTimePrecision;
        }

        public string SourceTableSchema { get; }
        public string SourceTableName { get; }
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
    public abstract class CommonConstraintDefinition
    {
        protected CommonConstraintDefinition(
            string sourceTableSchema,
            string sourceTableName,
            string constraintName,
            KeyConstraintType constraintType
        )
        {
            SourceTableSchema = sourceTableSchema;
            SourceTableName = sourceTableName;
            ConstraintName = constraintName;
            ConstraintType = constraintType;
        }
        public string SourceTableSchema { get; }
        public string SourceTableName { get; }
        public string ConstraintName { get; }
        public KeyConstraintType ConstraintType { get; }

        public override string ToString() => ConstraintName;

        public string MapConstraintNameToTarget(TableNameTerm targetTable)
            => this.ConstraintName.ReplaceCaseInsensitive(this.SourceTableName, targetTable.TableName).QualifySqlTerm();
    }

    [JsonConverter(typeof(JsonStringEnumConverter))]
    public abstract class KeyConstraintDefinition : CommonConstraintDefinition
    {
        protected KeyConstraintDefinition(
            string sourceTableSchema,
            string sourceTableName, 
            string constraintName, 
            KeyConstraintType constraintType, 
            List<KeyColumnDefinition> keyColumns
        ) : base(sourceTableSchema, sourceTableName, constraintName, constraintType)
        {
            KeyColumns = keyColumns;
        }
        public IList<KeyColumnDefinition> KeyColumns { get; }
    }

    [JsonConverter(typeof(JsonStringEnumConverter))]
    public class PrimaryKeyConstraintDefinition : KeyConstraintDefinition
    {
        public PrimaryKeyConstraintDefinition(
            string sourceTableSchema,
            string sourceTableName,
            string constraintName, 
            KeyConstraintType constraintType, 
            List<KeyColumnDefinition> keyColumns
        )
            : base(sourceTableSchema, sourceTableName, constraintName, constraintType, keyColumns)
        {
        }
    }

    [JsonConverter(typeof(JsonStringEnumConverter))]
    public class ForeignKeyConstraintDefinition : KeyConstraintDefinition
    {
        public ForeignKeyConstraintDefinition(
            string sourceTableSchema, 
            string sourceTableName, 
            string constraintName, 
            KeyConstraintType constraintType,
            string referenceTableSchema,
            string referenceTableName,
            List<KeyColumnDefinition> keyColumns, 
            List<KeyColumnDefinition> referenceColumns,
            string referentialMatchOption, 
            string referentialUpdateRuleClause, 
            string referentialDeleteRuleClause
        ) : base(sourceTableSchema, sourceTableName, constraintName, constraintType, keyColumns)
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
        ForeignKey,
        ColumnDefaultConstraint,
        ColumnCheckConstraint
    }

    public class ColumnDefaultConstraintDefinition : CommonConstraintDefinition
    {
        public ColumnDefaultConstraintDefinition(string sourceTableSchema, string sourceTableName, string constraintName, string columnName, string definition)
            : base(sourceTableSchema, sourceTableName, constraintName, KeyConstraintType.ColumnDefaultConstraint)
        {
            ColumnName = columnName;
            Definition = definition;
        }
        public string ColumnName { get; }
        public string Definition { get; }
    }

    public class ColumnCheckConstraintDefinition : CommonConstraintDefinition
    {
        public ColumnCheckConstraintDefinition(string sourceTableSchema, string sourceTableName, string constraintName, string checkClause)
            : base(sourceTableSchema, sourceTableName, constraintName, KeyConstraintType.ColumnCheckConstraint)
        {
            CheckClause = checkClause;
        }
        public string CheckClause { get; }
    }

    public class TableIndexDefinition
    {
        public TableIndexDefinition(
            string sourceTableSchema, 
            string sourceTableName, 
            int indexId, 
            string indexName, 
            bool isUnique, 
            bool isUniqueConstraint, 
            string filterDefinition, 
            List<IndexKeyColumnDefinition> keyColumns, 
            List<IndexIncludeColumnDefinition> includeColumns
        )
        {
            SourceTableSchema = sourceTableSchema;
            SourceTableName = sourceTableName;
            IndexId = indexId;
            IndexName = indexName;
            IsUnique = isUnique;
            IsUniqueConstraint = isUniqueConstraint;
            FilterDefinition = filterDefinition;
            KeyColumns = keyColumns;
            IncludeColumns = includeColumns;
        }

        public string SourceTableSchema { get; }
        public string SourceTableName { get; }

        public int IndexId { get; }
        public string IndexName { get; }
        public bool IsUnique { get; }
        public bool IsUniqueConstraint { get; }
        public string FilterDefinition { get; }
        public IList<IndexKeyColumnDefinition> KeyColumns { get; }
        public IList<IndexIncludeColumnDefinition> IncludeColumns { get; }

        public override string ToString() => IndexName;

        public string MapIndexNameToTarget(TableNameTerm targetTable)
            => this.IndexName.ReplaceCaseInsensitive(this.SourceTableName, targetTable.TableName).QualifySqlTerm();
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