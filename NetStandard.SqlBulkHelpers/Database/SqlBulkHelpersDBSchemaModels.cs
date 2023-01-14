using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.Json.Serialization;
using Newtonsoft.Json.Converters;
using SqlBulkHelpers.CustomExtensions;

namespace SqlBulkHelpers
{
    [JsonConverter(typeof(StringEnumConverter))]
    public class SqlBulkHelpersTableDefinition
    {
        protected ILookup<string, TableColumnDefinition> ColumnLookupByNameCaseInsensitive { get; }

        public SqlBulkHelpersTableDefinition(
            string tableSchema, 
            string tableName,
            TableSchemaDetailLevel schemaDetailLevel,
            List<TableColumnDefinition> tableColumns,
            PrimaryKeyConstraintDefinition primaryKeyConstraint,
            List<ForeignKeyConstraintDefinition> foreignKeyConstraints,
            List<ReferencingForeignKeyConstraintDefinition> referencingForeignKeyConstraints,
            List<ColumnDefaultConstraintDefinition> columnDefaultConstraints,
            List<ColumnCheckConstraintDefinition> columnCheckConstraints,
            List<TableIndexDefinition> tableIndexes,
            FullTextIndexDefinition fullTextIndex
        )
        {
            //Initialize our Table Name Term properties...
            TableNameTerm = TableNameTerm.From(
                tableSchema.AssertArgumentIsNotNullOrWhiteSpace(nameof(tableSchema)),
                tableName.AssertArgumentIsNotNullOrWhiteSpace(nameof(tableName))
            );

            //Table Schema Detail Level (what was included when queried...)
            SchemaDetailLevel = schemaDetailLevel;

            //Ensure that the Columns, Constraints, etc. collections are always NullSafe and is Immutable/ReadOnly!
            TableColumns = (tableColumns ?? new List<TableColumnDefinition>()).AsReadOnly();
            PrimaryKeyConstraint = primaryKeyConstraint;
            ForeignKeyConstraints = (foreignKeyConstraints ?? new List<ForeignKeyConstraintDefinition>()).AsReadOnly();
            ReferencingForeignKeyConstraints = (referencingForeignKeyConstraints ?? new List<ReferencingForeignKeyConstraintDefinition>()).AsReadOnly(); 
            ColumnDefaultConstraints = (columnDefaultConstraints ?? new List<ColumnDefaultConstraintDefinition>()).AsReadOnly();
            ColumnCheckConstraints = (columnCheckConstraints ?? new List<ColumnCheckConstraintDefinition>()).AsReadOnly();
            TableIndexes = (tableIndexes ?? new List<TableIndexDefinition>()).AsReadOnly();
            FullTextIndex = fullTextIndex;

            //Derived Key/Constraint properties for Convenience/Fast Processing
            IdentityColumn = this.TableColumns.FirstOrDefault(c => c.IsIdentityColumn);

            //Initialize the Case-insensitive Dictionary for quickly looking up Columns...
            ColumnLookupByNameCaseInsensitive = this.TableColumns.ToLookup(c => c.ColumnName, StringComparer.OrdinalIgnoreCase);
        }

        public TableNameTerm TableNameTerm { get; }
        public TableSchemaDetailLevel SchemaDetailLevel { get; }
        public string TableSchema => TableNameTerm.SchemaName;
        public string TableName => TableNameTerm.TableName;
        public string TableFullyQualifiedName => TableNameTerm.FullyQualifiedTableName;
        public IList<TableColumnDefinition> TableColumns { get; }
        public PrimaryKeyConstraintDefinition PrimaryKeyConstraint { get; }
        public IList<ForeignKeyConstraintDefinition> ForeignKeyConstraints { get; }
        public IList<ReferencingForeignKeyConstraintDefinition> ReferencingForeignKeyConstraints { get; }
        public IList<ColumnDefaultConstraintDefinition> ColumnDefaultConstraints { get; }
        public IList<ColumnCheckConstraintDefinition> ColumnCheckConstraints { get; }
        public IList<TableIndexDefinition> TableIndexes { get; }
        
        //ONLY ONE Full Text Index is optionally allowed per Table:
        //https://learn.microsoft.com/en-us/sql/t-sql/statements/create-fulltext-index-transact-sql?view=sql-server-ver16
        public FullTextIndexDefinition FullTextIndex { get; }

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
            => ColumnLookupByNameCaseInsensitive[columnName].FirstOrDefault();

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
    public abstract class CommonSourceDefinition
    {
        protected CommonSourceDefinition(
            string sourceTableSchema,
            string sourceTableName
        )
        {
            SourceTableSchema = sourceTableSchema;
            SourceTableName = sourceTableName;
            SourceTableNameTerm = TableNameTerm.From(sourceTableSchema, sourceTableName);
        }
        public string SourceTableSchema { get; }
        public string SourceTableName { get; }
        public TableNameTerm SourceTableNameTerm { get; }
    }


    [JsonConverter(typeof(JsonStringEnumConverter))]
    public abstract class CommonConstraintDefinition : CommonSourceDefinition
    {
        protected CommonConstraintDefinition(
            string sourceTableSchema,
            string sourceTableName,
            string constraintName,
            KeyConstraintType constraintType
        ) : base(sourceTableSchema, sourceTableName)
        {
            ConstraintName = constraintName;
            ConstraintType = constraintType;
        }

        public string ConstraintName { get; }
        public KeyConstraintType ConstraintType { get; }
        //NOTE: Source Table and Constraint Name are required since this is used as a Lookup Identifier for Referencing, and FKey constraints
        public override string ToString() => $"{SourceTableNameTerm} {ConstraintName}";

        public string MapConstraintNameToTargetAndEnsureUniqueness(TableNameTerm targetTable)
            => SqlSchemaUtils.MapNameToTargetAndEnsureUniqueness(ConstraintName, SourceTableNameTerm, targetTable);
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
            KeyColumns = (keyColumns ?? new List<KeyColumnDefinition>()).AsReadOnly();
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
            ReferenceColumns = (referenceColumns ?? new List<KeyColumnDefinition>()).AsReadOnly();
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

        public void AssertIsForeignKeyConstraint()
        {
            if (ConstraintType != KeyConstraintType.ForeignKey)
                throw new ArgumentException($"The Key Constraint provided is not a {nameof(KeyConstraintType.ForeignKey)} constraint type.");
        }
    }

    [JsonConverter(typeof(JsonStringEnumConverter))]
    public class ReferencingForeignKeyConstraintDefinition : CommonConstraintDefinition
    {
        public ReferencingForeignKeyConstraintDefinition(
            string sourceTableSchema,
            string sourceTableName,
            string constraintName,
            KeyConstraintType constraintType
        ) : base(sourceTableSchema, sourceTableName, constraintName, constraintType)
        {
        }
        
        public void AssertIsForeignKeyConstraint()
        {
            if (ConstraintType != KeyConstraintType.ForeignKey)
                throw new ArgumentException($"The Key Constraint provided is not a {nameof(KeyConstraintType.ForeignKey)} constraint type.");
        }
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

    public class TableIndexDefinition : CommonSourceDefinition
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
        ) : base(sourceTableSchema, sourceTableName)
        {
            IndexId = indexId;
            IndexName = indexName;
            IsUnique = isUnique;
            IsUniqueConstraint = isUniqueConstraint;
            FilterDefinition = filterDefinition;
            KeyColumns = (keyColumns ?? new List<IndexKeyColumnDefinition>()).AsReadOnly();
            IncludeColumns = (includeColumns ?? new List<IndexIncludeColumnDefinition>()).AsReadOnly();
        }

        public int IndexId { get; }
        public string IndexName { get; }
        public bool IsUnique { get; }
        public bool IsUniqueConstraint { get; }
        public string FilterDefinition { get; }
        public IList<IndexKeyColumnDefinition> KeyColumns { get; }
        public IList<IndexIncludeColumnDefinition> IncludeColumns { get; }

        public override string ToString() => IndexName;

        public string MapIndexNameToTargetAndEnsureUniqueness(TableNameTerm targetTable)
            => SqlSchemaUtils.MapNameToTargetAndEnsureUniqueness(IndexName, SourceTableNameTerm, targetTable);
    }

    public class FullTextIndexDefinition : CommonSourceDefinition
    {
        public FullTextIndexDefinition(
            string sourceTableSchema, 
            string sourceTableName, 
            string fullTextCatalogName, 
            string uniqueIndexName,
            string changeTrackingStateDescription,
            string stopListName,
            string propertyListName,
            List<FullTextIndexColumnDefinition> indexedColumns
        ) : base(sourceTableSchema, sourceTableName)
        {
            FullTextCatalogName = fullTextCatalogName;
            UniqueIndexName = uniqueIndexName;
            ChangeTrackingStateDescription = changeTrackingStateDescription;
            StopListName = stopListName;
            PropertyListName = propertyListName;
            IndexedColumns = (indexedColumns ?? new List<FullTextIndexColumnDefinition>()).AsReadOnly();
        }

        public string FullTextCatalogName { get; }
        public string UniqueIndexName { get; }
        public string ChangeTrackingStateDescription { get; }
        public string StopListName { get; }
        public string PropertyListName { get; set; }
        public IList<FullTextIndexColumnDefinition> IndexedColumns { get; }

        public override string ToString() => $"FULLTEXT INDEX ON {SourceTableNameTerm}({IndexedColumns.Select(c => c.ColumnName).ToCsv()})";
    }

    public class FullTextIndexColumnDefinition : KeyColumnDefinition
    {
        public FullTextIndexColumnDefinition(
            int ordinalPosition, 
            string columnName,
            int languageId,
            bool statisticalSemanticsEnabled,
            string typeColumnName
        ) : base(ordinalPosition, columnName)
        {
            LanguageId = languageId;
            StatisticalSemanticsEnabled = statisticalSemanticsEnabled;
            TypeColumnName = typeColumnName;
        }

        public int LanguageId { get; }
        public bool StatisticalSemanticsEnabled { get; }
        public string TypeColumnName { get; }
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

    internal static class SqlSchemaUtils
    {
        public static string MapNameToTargetAndEnsureUniqueness(string originalObjectName, TableNameTerm sourceTable, TableNameTerm targetTable)
        {
            //THis is the max length of SQL Object names (e.g. Indexes, Columns, etc.)
            //More Info Here: https://stackoverflow.com/questions/5808332/sql-server-maximum-character-length-of-object-names
            const int SQL_NAME_MAX_LENGTH = 128;

            var mappedName = originalObjectName.ReplaceCaseInsensitive(sourceTable.TableName, targetTable.TableName);

            //NOTE: We must ensure that the PKey Name does not exceed the max length and that it is also unique!
            //      We need to handle cases where the original Table Name was not used in the Constraint Naming convention;
            //      which is common with auto-generated constraints.
            if (mappedName.Length >= SQL_NAME_MAX_LENGTH || mappedName.Equals(originalObjectName))
            {
                var uniqueIdSuffix = string.Concat("_", IdGenerator.NewId());
                var truncatedMappedName = mappedName.TruncateToLength(SQL_NAME_MAX_LENGTH - uniqueIdSuffix.Length);
                mappedName = string.Concat(truncatedMappedName, uniqueIdSuffix);
            }

            return mappedName.QualifySqlTerm();
        }
    }
}
