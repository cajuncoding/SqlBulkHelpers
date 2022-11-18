using SqlBulkHelpers.MaterializedData.Interfaces;
using System;
using System.Linq;
using System.Text;

namespace SqlBulkHelpers.MaterializedData
{
    public enum ScriptAction
    {
        RecreateIfExists,
        StopProcessingIfExists,
        ContinueProcessingIfExists
    }

    public class MaterializedDataScriptBuilder : ISqlScriptBuilder
    {
        protected StringBuilder ScriptBuilder { get; } = new StringBuilder();

        protected MaterializedDataScriptBuilder()
        {
        }

        public static MaterializedDataScriptBuilder NewScript() => new MaterializedDataScriptBuilder();

        public MaterializedDataScriptBuilder Go()
        {
            ScriptBuilder.Append(Environment.NewLine).Append("GO");
            return this;
        }

        public MaterializedDataScriptBuilder CreateSchema(string schemaName)
        {
            var sanitizedSchemaName = schemaName.TrimTableNameTerm();
            ScriptBuilder.Append($@"
	            --Run only if the Schema doesn't already that the Schemas Exists (Lazy Initialize/Idempotent)
	            If SCHEMA_ID('{sanitizedSchemaName}') IS NULL
		            EXEC('CREATE SCHEMA {sanitizedSchemaName.QualifySqlTerm()}');
            ");
            return this;
        }

        public MaterializedDataScriptBuilder DropTable(TableNameTerm tableName)
        {
            ScriptBuilder.Append($@"
	            --Run only if the Table actually exists (Idempotent)
                IF EXISTS (SELECT TOP (1) 1 FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = '{tableName.SchemaName}' AND TABLE_NAME = '{tableName.TableName}')
                    EXEC('DROP TABLE {tableName.FullyQualifiedTableName}');
                ");
            return this;
        }

        public MaterializedDataScriptBuilder CloneTableStructure(TableNameTerm sourceTable, TableNameTerm targetTable, ScriptAction ifExistsAction = ScriptAction.RecreateIfExists)
        {
            CreateSchema(targetTable.SchemaName);
            if (ifExistsAction == ScriptAction.RecreateIfExists)
            {
                DropTable(targetTable);
            }
            else if (ifExistsAction == ScriptAction.StopProcessingIfExists)
            {
                ScriptBuilder.Append($@"
                    --Create the new Target Table by copying the core structure from the Source Table...
	                --Run only if the Table doesn't Already Exist (Idempotent)
                    IF EXISTS (SELECT TOP (1) 1 FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = '{targetTable.SchemaName}' AND TABLE_NAME = '{targetTable.TableName}')
                        RETURN;
                ");
            }
            else if (ifExistsAction == ScriptAction.ContinueProcessingIfExists)
            {
                ScriptBuilder.Append($@"
                    --Create the new Target Table by copying the core structure from the Source Table...
	                --Run only if the Table doesn't Already Exist (Idempotent)
                    IF NOT EXISTS (SELECT TOP (1) 1 FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = '{targetTable.SchemaName}' AND TABLE_NAME = '{targetTable.TableName}')
                        EXEC('SELECT TOP (0) * INTO {targetTable.FullyQualifiedTableName} FROM {sourceTable.FullyQualifiedTableName}');
                ");
            }

            return this;
        }

        public MaterializedDataScriptBuilder AddPrimaryKeyConstraint(TableNameTerm tableName, PrimaryKeyConstraintDefinition pkeyConstraint)
        {
            if (pkeyConstraint == null) return this;

            if (pkeyConstraint.ConstraintType != KeyConstraintType.PrimaryKey)
                throw new ArgumentException($"The Key Constraint provided is not a {nameof(KeyConstraintType.PrimaryKey)} constraint type.");

            var pkeyName = pkeyConstraint.MapConstraintNameToTarget(tableName);
            var keyColumns = pkeyConstraint.KeyColumns.OrderBy(c => c.OrdinalPosition).Select(c => c.ColumnName.QualifySqlTerm());

            ScriptBuilder.Append($@"
                ALTER TABLE {tableName.FullyQualifiedTableName} ADD CONSTRAINT {pkeyName} PRIMARY KEY CLUSTERED ({keyColumns.ToCSV()});
            ");
            return this;
        }

        public MaterializedDataScriptBuilder AddForeignKeyConstraints(TableNameTerm tableName, params ForeignKeyConstraintDefinition[] fkeyConstraints)
        {
            foreach (var fkeyConstraint in fkeyConstraints)
            {
                if (fkeyConstraint.ConstraintType != KeyConstraintType.ForeignKey)
                    throw new ArgumentException($"The Key Constraint provided is not a {nameof(KeyConstraintType.ForeignKey)} constraint type.");

                var fkeyName = fkeyConstraint.MapConstraintNameToTarget(tableName);
                var keyColumns = fkeyConstraint.KeyColumns.OrderBy(c => c.OrdinalPosition).Select(c => c.ColumnName.QualifySqlTerm());
                var referenceColumns = fkeyConstraint.ReferenceColumns.OrderBy(c => c.OrdinalPosition).Select(c => c.ColumnName.QualifySqlTerm());

                ScriptBuilder.Append($@"
                    ALTER TABLE {tableName.FullyQualifiedTableName} ADD CONSTRAINT {fkeyName} 
	                        FOREIGN KEY ({keyColumns.ToCSV()}) 
                            REFERENCES {fkeyConstraint.ReferenceTableFullyQualifiedName} ({referenceColumns.ToCSV()})
	                        ON UPDATE {fkeyConstraint.ReferentialUpdateRuleClause}
                            ON DELETE {fkeyConstraint.ReferentialDeleteRuleClause};
                ");
            }

            return this;
        }

        public MaterializedDataScriptBuilder AddTableIndexes(TableNameTerm tableName, params TableIndexDefinition[] tableIndexes)
        {
            foreach (var index in tableIndexes)
            {
                var indexName = index.MapIndexNameToTarget(tableName);
                var keyColumns = index.KeyColumns.OrderBy(c => c.OrdinalPosition).Select(c => c.ColumnName.QualifySqlTerm());

                if (index.IsUniqueConstraint)
                {
                    //Add a Unique (Non-PKEY) Constraint -- Sql Server stores Unique Constraints along with Indexes so they are processed from teh same model.
                    ScriptBuilder.Append($@"
                        ALTER TABLE {tableName.FullyQualifiedTableName} ADD CONSTRAINT {indexName} UNIQUE ({keyColumns.ToCSV()});
                    ");
                }
                else
                {
                    var includeColumns = index.IncludeColumns.OrderBy(c => c.OrdinalPosition).Select(c => c.ColumnName.QualifySqlTerm());

                    var includeSql = includeColumns.HasAny()
                        ? $"INCLUDE ({includeColumns.ToCSV()})"
                        : string.Empty;

                    var filterSql = !string.IsNullOrWhiteSpace(index.FilterDefinition)
                        ? $"WHERE {index.FilterDefinition}"
                        : string.Empty;

                    var uniqueSql = index.IsUnique ? "UNIQUE" : string.Empty;

                    ScriptBuilder.Append($@"
                        CREATE {uniqueSql} NONCLUSTERED INDEX {indexName} 
                            ON {tableName.FullyQualifiedTableName} ({keyColumns.ToCSV()})
                            {includeSql}
                            {filterSql}
                    ");
                }
            }

            return this;
        }

        public MaterializedDataScriptBuilder AddColumnCheckConstraints(TableNameTerm tableName, params ColumnCheckConstraintDefinition[] columnCheckConstraints)
        {
            foreach(var columnCheckConstraint in columnCheckConstraints)
            {
                var constraintName = columnCheckConstraint.MapConstraintNameToTarget(tableName);
                ScriptBuilder.Append($@"
                    ALTER TABLE {tableName.FullyQualifiedTableName} WITH CHECK ADD CONSTRAINT {constraintName} CHECK {columnCheckConstraint.CheckClause};
                    ALTER TABLE {tableName.FullyQualifiedTableName} CHECK CONSTRAINT {constraintName};
                ");
            }
            return this;
        }

        public MaterializedDataScriptBuilder AddColumnDefaultConstraints(TableNameTerm tableName, params ColumnDefaultConstraintDefinition[] columnDefaultConstraints)
        {
            foreach (var columnDefaultConstraint in columnDefaultConstraints)
            {
                var constraintName = columnDefaultConstraint.MapConstraintNameToTarget(tableName);
                var columnName = columnDefaultConstraint.ColumnName.QualifySqlTerm();
                ScriptBuilder.Append($@"
                    ALTER TABLE dbo.doc_exz ADD CONSTRAINT DF_Doc_Exz_Column_B DEFAULT 50 FOR column_b;
                    ALTER TABLE {tableName.FullyQualifiedTableName} ADD CONSTRAINT {constraintName} DEFAULT {columnDefaultConstraint.Definition} FOR {columnName};
                ");
            }
            return this;
        }

        public string BuildSqlScript()
        {
            ScriptBuilder.Append($@"
                --Return IsSuccessful = true once completed...
                SELECT IsSuccessful = CAST(1 as BIT); 
            ");

            return ScriptBuilder.ToString();
        }

        public override string ToString() => BuildSqlScript();
    }
}
