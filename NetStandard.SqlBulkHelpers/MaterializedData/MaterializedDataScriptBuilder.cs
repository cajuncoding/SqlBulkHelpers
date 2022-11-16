using System;
using System.Text;

namespace SqlBulkHelpers.MaterializedData
{
    public class MaterializedDataScriptBuilder
    {
        protected StringBuilder ScriptBuilder { get; } = new StringBuilder();

        protected MaterializedDataScriptBuilder()
        {
        }

        public static MaterializedDataScriptBuilder NewScript() => new MaterializedDataScriptBuilder();

        public MaterializedDataScriptBuilder CloneTableStructure(TableNameTerm sourceTable, TableNameTerm targetTable, bool recreateIfExists = true)
        {
            ScriptBuilder.Append($@"
                --Determine if our Target Table Already Exists..
                DECLARE @TargetTableExists bit = ISNULL((SELECT TOP (1) 1 FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = '{targetTable.SchemaName}' AND TABLE_NAME = '{targetTable.TableName}'), 0);

	            --Ensure that the Schemas Exist (Lazy Initialize/Idempotent)
	            If SCHEMA_ID('{targetTable.SchemaName}') IS NULL
		            EXEC('CREATE SCHEMA {targetTable.SchemaName};');
            ");

            if (recreateIfExists)
            {
                ScriptBuilder.Append($@"
                    --Drop the table if specified to Recreate and it actually Exists...
                    IF @TargetTableExists = 1
                        EXEC('DROP TABLE {targetTable.FullyQualifiedTableName};');
                ");
            }

            ScriptBuilder.Append($@"
                --Create the Table if it doesn't already exist...
                IF TargetTableExists = 0
                    EXEC('SELECT TOP (0) * INTO {targetTable.FullyQualifiedTableName} FROM {sourceTable.FullyQualifiedTableName};')
            ");

            return this;
        }

        public MaterializedDataScriptBuilder AddPrimaryKey(TableNameTerm? tableName = null)
        {
            //TODO: WIP...
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
