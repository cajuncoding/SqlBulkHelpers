using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using SqlBulkHelpers.MaterializedData.Interfaces;

namespace SqlBulkHelpers.MaterializedData
{
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

        public MaterializedDataScriptBuilder CloneTableStructure(TableNameTerm sourceTable, TableNameTerm targetTable, bool recreateIfExists = true)
        {
            CreateSchema(targetTable.SchemaName);
            if (recreateIfExists)
            {
                DropTable(targetTable);
            }

            ScriptBuilder.Append($@"
                --Create the new Target Table by copying the core structure from the Source Table...
	            --Run only if the Table doesn't Already Exist (Idempotent)
                IF NOT EXISTS (SELECT TOP (1) 1 FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = '{targetTable.SchemaName}' AND TABLE_NAME = '{targetTable.TableName}')
                    EXEC('SELECT TOP (0) * INTO {targetTable.FullyQualifiedTableName} FROM {sourceTable.FullyQualifiedTableName}');
            ");

            return this;
        }

        public MaterializedDataScriptBuilder AddPrimaryKeyConstraint(TableNameTerm tableName, PrimaryKeyConstraintDefinition pkeyConstraint)
        {
            if (pkeyConstraint.ConstraintType != KeyConstraintType.PrimaryKey)
                throw new ArgumentException($"The Key Constraint provided is not a {nameof(KeyConstraintType.PrimaryKey)} constraint type.");

            var keyColumns = pkeyConstraint.KeyColumns.OrderBy(c => c.OrdinalPosition).Select(c => c.ColumnName.QualifySqlTerm());

            ScriptBuilder.Append($@"
                ALTER TABLE {tableName.FullyQualifiedTableName} ADD CONSTRAINT {pkeyConstraint.ConstraintName.QualifySqlTerm()} PRIMARY KEY CLUSTERED ({keyColumns.ToCSV()});
            ");
            return this;
        }

        public MaterializedDataScriptBuilder AddForeignKeyConstraint(TableNameTerm tableName, params ForeignKeyConstraintDefinition[] fkeyConstraints)
        {
            foreach (var fkeyConstraint in fkeyConstraints)
            {
                if (fkeyConstraint.ConstraintType != KeyConstraintType.ForeignKey)
                    throw new ArgumentException($"The Key Constraint provided is not a {nameof(KeyConstraintType.ForeignKey)} constraint type.");

                var keyColumns = fkeyConstraint.KeyColumns.OrderBy(c => c.OrdinalPosition).Select(c => c.ColumnName.QualifySqlTerm());
                var referenceColumns = fkeyConstraint.ReferenceColumns.OrderBy(c => c.OrdinalPosition).Select(c => c.ColumnName.QualifySqlTerm());

                ScriptBuilder.Append($@"
                    ALTER TABLE {tableName.FullyQualifiedTableName} ADD CONSTRAINT {fkeyConstraint.ConstraintName.QualifySqlTerm()} 
	                        FOREIGN KEY ({keyColumns.ToCSV()}) 
                            REFERENCES {fkeyConstraint.ReferenceTableFullyQualifiedName} ({referenceColumns.ToCSV()})
	                        ON UPDATE {fkeyConstraint.ReferentialUpdateRuleClause}
                            ON DELETE {fkeyConstraint.ReferentialDeleteRuleClause};
                ");
            }

            return this;
        }
        public MaterializedDataScriptBuilder AddColumnCheckConstraint(TableNameTerm tableName, params ColumnDefaultConstraintDefinition[] columnCheckConstraints)
        {
            throw new NotImplementedException();
            return this;
        }

        public MaterializedDataScriptBuilder AddColumnDefaultConstraint(TableNameTerm tableName, params ColumnCheckConstraintDefinition[] columnDefaultConstraints)
        {
            throw new NotImplementedException();
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
