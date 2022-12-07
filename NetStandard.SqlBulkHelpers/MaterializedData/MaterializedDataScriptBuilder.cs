using SqlBulkHelpers.MaterializedData.Interfaces;
using System;
using System.Linq;
using System.Text;

namespace SqlBulkHelpers.MaterializedData
{
    public enum IfExists
    {
        Recreate,
        StopProcessingWithException,
        ContinueProcessing
    }

    public class MaterializedDataScriptBuilder : ISqlScriptBuilder
    {
        public bool IsSqlScriptFinished { get; protected set; } = false;

        protected StringBuilder ScriptBuilder { get; } = new StringBuilder();

        protected MaterializedDataScriptBuilder()
        {
        }

        public static MaterializedDataScriptBuilder NewSqlScript() => new MaterializedDataScriptBuilder();

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
            tableName.AssertArgumentIsNotNull(nameof(tableName));
            ScriptBuilder.Append($@"
	            --Run only if the Table actually exists (Idempotent)
                IF OBJECT_ID('{tableName.FullyQualifiedTableName}') IS NOT NULL
                    EXEC('DROP TABLE {tableName.FullyQualifiedTableName}');
                ");
            return this;
        }

        public MaterializedDataScriptBuilder TruncateTable(TableNameTerm tableName)
        {
            tableName.AssertArgumentIsNotNull(nameof(tableName));
            ScriptBuilder.Append($@"
	            --Run only if the Table actually exists (Idempotent)
                IF OBJECT_ID('{tableName.FullyQualifiedTableName}') IS NOT NULL
                    EXEC('TRUNCATE TABLE {tableName.FullyQualifiedTableName}');
                ");
            return this;
        }

        public MaterializedDataScriptBuilder CloneTableWithAllElements(SqlBulkHelpersTableDefinition sourceTableDefinition, TableNameTerm targetTable, IfExists ifExists = IfExists.Recreate)
        {
            sourceTableDefinition.AssertArgumentIsNotNull(nameof(sourceTableDefinition));
            targetTable.AssertArgumentIsNotNull(nameof(targetTable));

            this
                .CloneTableWithColumnsOnly(sourceTableDefinition.TableNameTerm, targetTable, ifExists)
                .AddPrimaryKeyConstraint(targetTable, sourceTableDefinition.PrimaryKeyConstraint)
                .AddForeignKeyConstraints(targetTable, sourceTableDefinition.ForeignKeyConstraints.AsArray())
                .AddColumnDefaultConstraints(targetTable, sourceTableDefinition.ColumnDefaultConstraints.AsArray())
                .AddColumnCheckConstraints(targetTable, sourceTableDefinition.ColumnCheckConstraints.AsArray())
                .AddTableIndexes(targetTable, sourceTableDefinition.TableIndexes.AsArray());

            return this;
        }

        public MaterializedDataScriptBuilder SwitchTables(TableNameTerm sourceTable, TableNameTerm targetTable)
        {
            sourceTable.AssertArgumentIsNotNull(nameof(targetTable));
            targetTable.AssertArgumentIsNotNull(nameof(targetTable));

            ScriptBuilder.Append($@"
	            --Switch/Swap the Tables (with ALL Data) instantaneously
                ALTER TABLE {sourceTable.FullyQualifiedTableName} SWITCH TO {targetTable.FullyQualifiedTableName};
            ");
            return this;
        }

        public MaterializedDataScriptBuilder CloneTableWithColumnsOnly(TableNameTerm sourceTable, TableNameTerm targetTable, IfExists ifExists = IfExists.Recreate)
        {
            CreateSchema(targetTable.SchemaName);

            bool addTableCopyScript = false;
            if (ifExists == IfExists.Recreate)
            {
                addTableCopyScript = true;
                DropTable(targetTable);
            }
            else if (ifExists == IfExists.StopProcessingWithException)
            {
                addTableCopyScript = true;
                ScriptBuilder.Append($@"
                    --Create the new Target Table by copying the core structure from the Source Table...
	                --Run only if the Table doesn't Already Exist (Idempotent)
					IF OBJECT_ID('{targetTable.FullyQualifiedTableName}') IS NOT NULL
                        --NOTE: Error severity must be 16 to ensure that the Exception is thrown to the catch block or to C#...
                        RAISERROR('Failed to clone table {targetTable.FullyQualifiedTableName}; the table already exists and no option to re-create or continue was specified.', 16, 0);;
                ");
            }
            
            if (addTableCopyScript || ifExists == IfExists.ContinueProcessing)
            {
                ScriptBuilder.Append($@"
                    --Create the new Target Table by copying the core structure from the Source Table...
	                --Run only if the Table doesn't Already Exist (Idempotent)
                    IF OBJECT_ID('{targetTable.FullyQualifiedTableName}') IS NULL
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
                fkeyConstraint.AssertIsForeignKeyConstraint();

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

        public MaterializedDataScriptBuilder DropForeignKeyConstraints(TableNameTerm tableName, params ForeignKeyConstraintDefinition[] fkeyConstraints)
        {
            foreach (var fkeyConstraint in fkeyConstraints)
            {
                fkeyConstraint.AssertIsForeignKeyConstraint();

                var fkeyName = fkeyConstraint.MapConstraintNameToTarget(tableName);

                ScriptBuilder.Append($@"
                    ALTER TABLE {tableName.FullyQualifiedTableName} DROP CONSTRAINT {fkeyName};
                ");
            }
            return this;
        }

        public MaterializedDataScriptBuilder DisableAllTableConstraintChecks(TableNameTerm tableName)
        {
            ScriptBuilder.Append($@"
                ALTER TABLE {tableName.FullyQualifiedTableName} NOCHECK CONSTRAINT ALL;
            ");
            return this;
        }

        public MaterializedDataScriptBuilder EnableAllTableConstraintChecks(TableNameTerm tableName, bool executeDataValidation = true)
        {
            var doCheckClause = executeDataValidation ? "WITH CHECK" : string.Empty;
            ScriptBuilder.Append($@"
                ALTER TABLE {tableName.FullyQualifiedTableName} {doCheckClause} CHECK CONSTRAINT ALL;
            ");
            return this;
        }

        public MaterializedDataScriptBuilder EnableForeignKeyChecks(params ForeignKeyConstraintDefinition[] fkeyConstraints)
        {
            foreach (var fkeyConstraint in fkeyConstraints)
            {
                fkeyConstraint.AssertIsForeignKeyConstraint();

                ScriptBuilder.Append($@"
                    ALTER TABLE {fkeyConstraint.SourceTableNameTerm.FullyQualifiedTableName} CHECK CONSTRAINT {fkeyConstraint.ConstraintName.QualifySqlTerm()};
                ");
            }
            return this;
        }

        public MaterializedDataScriptBuilder DisableReferencingForeignKeyChecks(params ReferencingForeignKeyConstraintDefinition[] referencingFKeyConstraints)
        {
            foreach (var referencingFKey in referencingFKeyConstraints)
            {
                referencingFKey.AssertIsForeignKeyConstraint();

                ScriptBuilder.Append($@"
                    ALTER TABLE {referencingFKey.SourceTableNameTerm.FullyQualifiedTableName} NOCHECK CONSTRAINT {referencingFKey.ConstraintName.QualifySqlTerm()};
                ");
            }
            return this;
        }

        public MaterializedDataScriptBuilder EnableReferencingForeignKeyChecks(bool executeDataValidation, params ReferencingForeignKeyConstraintDefinition[] referencingFKeyConstraints)
        {
            var doCheckClause = executeDataValidation ? "WITH CHECK" : string.Empty;
            foreach (var referencingFKey in referencingFKeyConstraints)
            {
                referencingFKey.AssertIsForeignKeyConstraint();

                ScriptBuilder.Append($@"
                    ALTER TABLE {referencingFKey.SourceTableNameTerm.FullyQualifiedTableName} {doCheckClause} CHECK CONSTRAINT {referencingFKey.ConstraintName.QualifySqlTerm()};
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

        public MaterializedDataScriptBuilder FinishSqlScript()
        {
            if(!IsSqlScriptFinished)
                ScriptBuilder.Append($@"
                    --Return IsSuccessful = true once completed...
                    SELECT IsSuccessful = CAST(1 as BIT); 
                ");
            
            return this;
        }

        public string BuildSqlScript()
        {
            FinishSqlScript();
            return ScriptBuilder.ToString();
        }

        //NOTE: WE can't call BuildSqlScript() because that will result in the Finish being called every time we it's converted to a String
        //          which incorrectly mutates the underlying string builder.
        public override string ToString() => ScriptBuilder.ToString();
    }
}
