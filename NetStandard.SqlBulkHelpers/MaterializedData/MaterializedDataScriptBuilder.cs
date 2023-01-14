using SqlBulkHelpers.MaterializedData.Interfaces;
using System;
using System.Collections.Generic;
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

        protected Dictionary<string, string> Variables = new Dictionary<string, string>();

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

        public MaterializedDataScriptBuilder CloneTableWithAllElements(
            SqlBulkHelpersTableDefinition sourceTableDefinition, 
            TableNameTerm targetTable, 
            IfExists ifExists = IfExists.Recreate, 
            bool cloneIdentitySeedValue = true, 
            bool includeFKeyConstraints = true,
            bool copyDataFromSource = false
        )
        {
            sourceTableDefinition.AssertArgumentIsNotNull(nameof(sourceTableDefinition));
            targetTable.AssertArgumentIsNotNull(nameof(targetTable));

            CloneTableWithColumnsOnly(sourceTableDefinition.TableNameTerm, targetTable, ifExists);

            if (copyDataFromSource)
                CopyTableData(sourceTableDefinition, targetTable);

            //GET the Seed Value immediately, since there is a small chance of it changing...
            if (cloneIdentitySeedValue && sourceTableDefinition.IdentityColumn != null)
                SyncIdentitySeedValue(sourceTableDefinition.TableNameTerm, targetTable);

            AddPrimaryKeyConstraint(targetTable, sourceTableDefinition.PrimaryKeyConstraint);
            AddColumnDefaultConstraints(targetTable, sourceTableDefinition.ColumnDefaultConstraints.AsArray());
            AddColumnCheckConstraints(targetTable, sourceTableDefinition.ColumnCheckConstraints.AsArray());
            AddTableIndexes(targetTable, sourceTableDefinition.TableIndexes.AsArray());

            if (includeFKeyConstraints)
                AddForeignKeyConstraints(targetTable, sourceTableDefinition.ForeignKeyConstraints.AsArray());

            return this;
        }

        public MaterializedDataScriptBuilder CopyTableData(SqlBulkHelpersTableDefinition sourceTableDefinition, TableNameTerm targetTable)
        {
            sourceTableDefinition.AssertArgumentIsNotNull(nameof(sourceTableDefinition));
            targetTable.AssertArgumentIsNotNull(nameof(targetTable));

            bool hasIdentityColumn = sourceTableDefinition.IdentityColumn != null;

            //In this overload we handle the Source Table Definition and can dynamically determine if there is An Identity column we handle by enabling insertion for them...
            if (hasIdentityColumn)
            {
                ScriptBuilder.Append($@"
                    --The Table {sourceTableDefinition.TableFullyQualifiedName} has an Identity Column {sourceTableDefinition.IdentityColumn.ColumnName.QualifySqlTerm()} so we must allow Insertion of IDENTITY values to copy raw table data...
                    SET IDENTITY_INSERT {targetTable.FullyQualifiedTableName} ON;
                ");
            }

            //Now we can Copy data between the two tables...
            CopyTableData(sourceTableDefinition.TableNameTerm, targetTable, sourceTableDefinition.TableColumns.AsArray());

            //In this overload we handle the Source Table Definition and can dynamically determine if there is An Identity column we handle by enabling insertion for them...
            if (hasIdentityColumn)
            {
                ScriptBuilder.Append($@"
	                --We now disable IDENTITY Inserts once all data is copied into {targetTable}...
                    SET IDENTITY_INSERT {targetTable.FullyQualifiedTableName} OFF;
                ");
            }

            return this;
        }

        public MaterializedDataScriptBuilder CopyTableData(TableNameTerm sourceTable, TableNameTerm targetTable, params TableColumnDefinition[] columnDefs)
        {
            sourceTable.AssertArgumentIsNotNull(nameof(sourceTable));
            targetTable.AssertArgumentIsNotNull(nameof(targetTable));

            //Validate that we have Table Columns...
            if (!columnDefs.HasAny())
                throw new ArgumentException($"At least one valid column definition must be specified to denote what data to copy between tables.");

            var columnNamesCsv = columnDefs.Select(c => c.ColumnName.QualifySqlTerm()).ToCsv();

            ScriptBuilder.Append($@"
	            --Syncs the Identity Seed value of the Target Table with the current value of the Source Table (captured into Variable at top of script)
                INSERT INTO {targetTable.FullyQualifiedTableName} ({columnNamesCsv})
                    SELECT {columnNamesCsv}
                    FROM {sourceTable.FullyQualifiedTableName};
            ");
            return this;
        }

        public MaterializedDataScriptBuilder SyncIdentitySeedValue(TableNameTerm sourceTable, TableNameTerm targetTable)
        {
            sourceTable.AssertArgumentIsNotNull(nameof(sourceTable));
            targetTable.AssertArgumentIsNotNull(nameof(targetTable));

            //Variables will be written out at the Top of the Script so they are initialized quickly and values used are consistent for the entire script...
            var currentIdentityVariable = $"@CurrentIdentity_{sourceTable.TableNameVariable}";
            Variables.TryAdd(currentIdentityVariable, $"DECLARE {currentIdentityVariable} int = IDENT_CURRENT('{sourceTable.FullyQualifiedTableName}');");

            ScriptBuilder.Append($@"
	            --Syncs the Identity Seed value of the Target Table with the current value of the Source Table (captured into Variable at top of script)
                DBCC CHECKIDENT('{targetTable.FullyQualifiedTableName}', RESEED, {currentIdentityVariable});
            ");
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
            sourceTable.AssertArgumentIsNotNull(nameof(sourceTable));
            targetTable.AssertArgumentIsNotNull(nameof(targetTable));

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

            var pkeyName = pkeyConstraint.MapConstraintNameToTargetAndEnsureUniqueness(tableName);
            var keyColumns = pkeyConstraint.KeyColumns?.OrderBy(c => c.OrdinalPosition).Select(c => c.ColumnName.QualifySqlTerm());

            //Validate that we have Primary Key Columns...
            if (!keyColumns.HasAny())
                throw new ArgumentException($"At least one valid Key Column definition must be specified to create a Primary Key Constraint {pkeyName} on {tableName}.");

            ScriptBuilder.Append($@"
                ALTER TABLE {tableName.FullyQualifiedTableName} ADD CONSTRAINT {pkeyName} PRIMARY KEY CLUSTERED ({keyColumns.ToCsv()});
            ");
            return this;
        }

        public MaterializedDataScriptBuilder AddForeignKeyConstraints(TableNameTerm tableName, params ForeignKeyConstraintDefinition[] fkeyConstraints)
            => AddForeignKeyConstraints(tableName, true, fkeyConstraints);

        public MaterializedDataScriptBuilder AddForeignKeyConstraints(TableNameTerm tableName, bool executeConstraintValidation, params ForeignKeyConstraintDefinition[] fkeyConstraints)
        {
            foreach (var fkeyConstraint in fkeyConstraints)
            {
                fkeyConstraint.AssertIsForeignKeyConstraint();

                var fkeyName = fkeyConstraint.MapConstraintNameToTargetAndEnsureUniqueness(tableName);
                var keyColumns = fkeyConstraint.KeyColumns?.OrderBy(c => c.OrdinalPosition).Select(c => c.ColumnName.QualifySqlTerm());
                var referenceColumns = fkeyConstraint.ReferenceColumns?.OrderBy(c => c.OrdinalPosition).Select(c => c.ColumnName.QualifySqlTerm());

                //Validate that we have FKey Columns...
                if (!keyColumns.HasAny())
                    throw new ArgumentException($"At least one valid Key Column definition must be specified to create a Foreign Key Constraint {fkeyName} on {tableName}.");

                //Validate that we have FKey Reference Columns...
                if (!referenceColumns.HasAny())
                    throw new ArgumentException($"At least one valid Reference Column definition must be specified to create a Foreign Key Constraint {fkeyName} on {tableName}.");

                ScriptBuilder.Append($@"
                    ALTER TABLE {tableName.FullyQualifiedTableName} {GetNoCheckClause(executeConstraintValidation)} ADD CONSTRAINT {fkeyName} 
	                    FOREIGN KEY ({keyColumns.ToCsv()}) 
                        REFERENCES {fkeyConstraint.ReferenceTableFullyQualifiedName} ({referenceColumns.ToCsv()})
	                    ON UPDATE {fkeyConstraint.ReferentialUpdateRuleClause}
                        ON DELETE {fkeyConstraint.ReferentialDeleteRuleClause};
                ");
            }
            return this;
        }

        /// <summary>
        /// Drops FKey Constraints, and attempts to Map names that contain Source Table Name in the convention.
        /// NOTE: We currently DO NOT support dropping FKeys for whom the Name was mutated to be Unique! These will result in
        ///         SQL Exception that the Constraint does not exist; the workaround to this currently is to retrieve the full DB Schema
        ///         of cloned tables that result in Unique Constraint Names so that the full name is known...
        /// </summary>
        /// <param name="tableName"></param>
        /// <param name="fkeyConstraints"></param>
        /// <returns></returns>
        public MaterializedDataScriptBuilder DropForeignKeyConstraints(TableNameTerm tableName, params ForeignKeyConstraintDefinition[] fkeyConstraints)
        {
            foreach (var fkeyConstraint in fkeyConstraints)
            {
                fkeyConstraint.AssertIsForeignKeyConstraint();

                //NOTE: We currently do not support dropping FKeys for whom the Name was mutated to be Unique!
                var fkeyName = fkeyConstraint.MapConstraintNameToTargetAndEnsureUniqueness(tableName);

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

        public MaterializedDataScriptBuilder EnableAllTableConstraintChecks(TableNameTerm tableName, bool executeConstraintValidation = true)
        {
            ScriptBuilder.Append($@"
                ALTER TABLE {tableName.FullyQualifiedTableName} {GetCheckClause(executeConstraintValidation)} CHECK CONSTRAINT ALL;
            ");
            return this;
        }

        public MaterializedDataScriptBuilder DisableForeignKeyChecks(params ForeignKeyConstraintDefinition[] fkeyConstraints)
        {
            foreach (var fkeyConstraint in fkeyConstraints)
            {
                fkeyConstraint.AssertIsForeignKeyConstraint();

                ScriptBuilder.Append($@"
                    --Enabling the FKey Constraints and Trigger validation checks for each of them...
                    ALTER TABLE {fkeyConstraint.SourceTableNameTerm.FullyQualifiedTableName} NOCHECK CONSTRAINT {fkeyConstraint.ConstraintName.QualifySqlTerm()};
                ");
            }
            return this;
        }

        public MaterializedDataScriptBuilder DisableForeignKeyChecks(TableNameTerm tableName, params ForeignKeyConstraintDefinition[] fkeyConstraints)
        {
            foreach (var fkeyConstraint in fkeyConstraints)
            {
                fkeyConstraint.AssertIsForeignKeyConstraint();

                ScriptBuilder.Append($@"
                    --Enabling the FKey Constraints and Trigger validation checks for each of them...
                    ALTER TABLE {tableName.FullyQualifiedTableName} NOCHECK CONSTRAINT {fkeyConstraint.ConstraintName.QualifySqlTerm()};
                ");
            }
            return this;
        }

        public MaterializedDataScriptBuilder EnableForeignKeyChecks(TableNameTerm tableName, params ForeignKeyConstraintDefinition[] fkeyConstraints)
            => EnableForeignKeyChecks(tableName, true, fkeyConstraints);

        public MaterializedDataScriptBuilder EnableForeignKeyChecks(TableNameTerm tableName, bool executeConstraintValidation, params ForeignKeyConstraintDefinition[] fkeyConstraints)
        {
            foreach (var fkeyConstraint in fkeyConstraints)
            {
                fkeyConstraint.AssertIsForeignKeyConstraint();

                var fkeyConstraintNameQualified = fkeyConstraint.ConstraintName.QualifySqlTerm();
                var fullyQualifiedTableName = tableName.FullyQualifiedTableName;
                var errorMsgVariableName = $"@errorMsg_{IdGenerator.NewId()}";

                //We manually provide better error handling because the Messages from Sql Server are vague and it's unclear to a developer
                //  that this FKey constraint Check was the likely cause of failures, so we provide more details in a custom error message!
                //NOTE: We use THROW, not RAISEERROR(), as the recommended best practice by Microsoft because it honors SET XACT_ABORT.
                //More Info Here: https://learn.microsoft.com/en-us/sql/t-sql/language-elements/raiserror-transact-sql?view=sql-server-ver16
                ScriptBuilder.Append($@"
                    --Due to the Nebulous errors returned by Sql Server when a CHECK violation occurs, we handle this and return a more helpful error 
                    --  when FKey Checks fail so that developers have a better idea of why it failed; likely due to related data not being valid.
                    BEGIN TRY  
                        --Enabling the FKey Constraints and Trigger validation checks for each of them...
                        ALTER TABLE {fullyQualifiedTableName} {GetCheckClause(executeConstraintValidation)} CHECK CONSTRAINT {fkeyConstraintNameQualified};
                    END TRY  
                    BEGIN CATCH  
                        -- Raise a custom error that can be handled within C# as defined by severity level:
                        DECLARE {errorMsgVariableName} NVARCHAR(2048) = CONCAT(
							'An error occurred while executing the FKey constraint check for Foreign Key {fkeyConstraintNameQualified} on {fullyQualifiedTableName}. ', 
							'This exception ensures that the data integrity is maintained. ',
							ERROR_MESSAGE()
						);
						
                        THROW 51000, {errorMsgVariableName}, 1;
					END CATCH; 
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
                    --Disabling the Referencing FKey Constraint
                    ALTER TABLE {referencingFKey.SourceTableNameTerm.FullyQualifiedTableName} NOCHECK CONSTRAINT {referencingFKey.ConstraintName.QualifySqlTerm()};
                ");
            }
            return this;
        }

        public MaterializedDataScriptBuilder EnableReferencingForeignKeyChecks(bool executeConstraintValidation, params ReferencingForeignKeyConstraintDefinition[] referencingFKeyConstraints)
        {
            foreach (var referencingFKey in referencingFKeyConstraints)
            {
                referencingFKey.AssertIsForeignKeyConstraint();

                var fkeyConstraintNameQualified = referencingFKey.ConstraintName.QualifySqlTerm();
                var fullyQualifiedTableName = referencingFKey.SourceTableNameTerm.FullyQualifiedTableName;
                var errorMsgVariableName = $"@errorMsg_{IdGenerator.NewId()}";

                //We manually provide better error handling because the Messages from Sql Server are vague and it's unclear to a developer
                //  that this FKey constraint Check was the likely cause of failures, so we provide more details in a custom error message!
                //NOTE: We use THROW, not RAISEERROR(), as the recommended best practice by Microsoft because it honors SET XACT_ABORT.
                //More Info Here: https://learn.microsoft.com/en-us/sql/t-sql/language-elements/raiserror-transact-sql?view=sql-server-ver16
                ScriptBuilder.Append($@"
                    --Due to the Nebulous errors returned by Sql Server when a CHECK violation occurs, we handle this and return a more helpful error 
                    --  when FKey Checks fail so that developers have a better idea of why it failed; likely due to related data not being valid.
                    BEGIN TRY  
                        --Enabling the Referencing FKey Constraints...
                        ALTER TABLE {fullyQualifiedTableName} {GetCheckClause(executeConstraintValidation)} CHECK CONSTRAINT {fkeyConstraintNameQualified};
                    END TRY  
                    BEGIN CATCH  
                        -- Raise a custom error that can be handled within C# as defined by severity level:
                        DECLARE {errorMsgVariableName} NVARCHAR(2048) = CONCAT(
							'An error occurred while executing the FKey constraint check for Foreign Key {fkeyConstraintNameQualified} on {fullyQualifiedTableName}. ', 
							'This exception ensures that the data integrity is maintained. ',
							ERROR_MESSAGE()
						);
						
                        THROW 51000, {errorMsgVariableName}, 1;
					END CATCH; 
                ");

            }
            return this;
        }


        /// <summary>
        /// Drops the one (and only) Full Text Index on the specified table. This method is idempotent and safe and will only Drop it if it exists.
        /// NOTE: Sql Server only allows one Full Text Index per table, so it doesn't have a unique name:
        ///     https://learn.microsoft.com/en-us/sql/t-sql/statements/create-fulltext-index-transact-sql?view=sql-server-ver16
        /// </summary>
        /// <param name="tableName"></param>
        /// <returns></returns>
        public MaterializedDataScriptBuilder DropFullTextIndex(TableNameTerm tableName)
        {
            //NOTE: We currently do not support dropping FKeys for whom the Name was mutated to be Unique!
            ScriptBuilder.Append($@"
                IF EXISTS(SELECT * FROM sys.fulltext_indexes fti WHERE fti.[object_id] = OBJECT_ID('{tableName.FullyQualifiedTableName}'))
                    DROP FULLTEXT INDEX ON {tableName.FullyQualifiedTableName};
            ");
            return this;
        }

        /// <summary>
        /// Adds the provided Full Text Index to the specified table.
        /// NOTE: Sql Server only allows one Full Text Index per table:
        ///     https://learn.microsoft.com/en-us/sql/t-sql/statements/create-fulltext-index-transact-sql?view=sql-server-ver16
        /// </summary>
        /// <param name="tableName"></param>
        /// <param name="fullTextIndex"></param>
        /// <returns></returns>
        public MaterializedDataScriptBuilder AddFullTextIndex(TableNameTerm tableName, FullTextIndexDefinition fullTextIndex)
        {
            if (fullTextIndex == null) return this;

            const string TAB = "\t";

            //NOTE: We currently do not support dropping FKeys for whom the Name was mutated to be Unique!
            ScriptBuilder
                .AppendLine()
                .AppendLine($"CREATE FULLTEXT INDEX ON {tableName.FullyQualifiedTableName}")
                .Append("(");
                
            var i = 0;
            foreach (var col in fullTextIndex.IndexedColumns.OrderBy(c => c.OrdinalPosition))
            {
                //CSV Separator for the Prior Item before the next/current one...
                if (i++ > 0) ScriptBuilder.Append(",");
                    
                ScriptBuilder
                    .AppendLine()//New Line for each Column definition...
                    .Append(TAB).Append(col.ColumnName.QualifySqlTerm()); //Column Name

                //TYPE COLUMN directive (Optional)
                if (!string.IsNullOrWhiteSpace(col.TypeColumnName))
                    ScriptBuilder.Append($" TYPE COLUMN {col.TypeColumnName}");

                //LANGUAGE directive (probably always Exists, but we check for safety)
                if (col.LanguageId > 0)
                    ScriptBuilder.Append($" LANGUAGE {col.LanguageId}");

                //STATISTICAL_SEMANTICS directive (Optional)
                if(col.StatisticalSemanticsEnabled)
                    ScriptBuilder.Append($" STATISTICAL_SEMANTICS");
            }

            ScriptBuilder
                .AppendLine()
                .AppendLine(")")
                .Append($"KEY INDEX {fullTextIndex.UniqueIndexName.QualifySqlTerm()} ON {fullTextIndex.FullTextCatalogName.QualifySqlTerm()}");

            //Initialize and Populate With Commands!
            var withCommands = new List<string>();
            if(!string.IsNullOrWhiteSpace(fullTextIndex.ChangeTrackingStateDescription))
                withCommands.Add($"CHANGE_TRACKING {fullTextIndex.ChangeTrackingStateDescription}");

            if(!string.IsNullOrWhiteSpace(fullTextIndex.StopListName))
                withCommands.Add($"STOPLIST = {fullTextIndex.StopListName}");

            if (!string.IsNullOrWhiteSpace(fullTextIndex.PropertyListName))
                withCommands.Add($"SEARCH PROPERTY LIST = {fullTextIndex.PropertyListName}");

            if (withCommands.Any())
                ScriptBuilder.Append(" WITH ").Append(withCommands.ToCsv());

            //End the statement...
            ScriptBuilder.Append(";").AppendLine();
            return this;
        }

        public MaterializedDataScriptBuilder AddTableIndexes(TableNameTerm tableName, params TableIndexDefinition[] tableIndexes)
        {
            foreach (var index in tableIndexes)
            {
                var indexName = index.MapIndexNameToTargetAndEnsureUniqueness(tableName);
                var keyColumns = index.KeyColumns?
                    .OrderBy(c => c.OrdinalPosition)
                    .Select(c => c.ColumnName.QualifySqlTerm());

                //Validate that we have FKey Columns...
                if (!keyColumns.HasAny())
                    throw new ArgumentException($"At least one valid Key Column definition must be specified to create an Index {indexName} on {tableName}.");

                if (index.IsUniqueConstraint)
                {
                    //Add a Unique (Non-PKEY) Constraint -- Sql Server stores Unique Constraints along with Indexes so they are processed from teh same model.
                    ScriptBuilder.Append($@"
                        ALTER TABLE {tableName.FullyQualifiedTableName} ADD CONSTRAINT {indexName} UNIQUE ({keyColumns.ToCsv()});
                    ");
                }
                else
                {
                    //NOTE: We must be Null-Safe here when initially calling Linq
                    var includeColumnsCsv = index.IncludeColumns
                        ?.OrderBy(c => c.OrdinalPosition)
                        .Select(c => c.ColumnName.QualifySqlTerm())
                        .ToCsv();

                    var includeSql = !string.IsNullOrWhiteSpace(includeColumnsCsv)
                        ? $"INCLUDE ({includeColumnsCsv})"
                        : string.Empty;

                    var filterSql = !string.IsNullOrWhiteSpace(index.FilterDefinition)
                        ? $"WHERE {index.FilterDefinition}"
                        : string.Empty;

                    var uniqueSql = index.IsUnique ? "UNIQUE" : string.Empty;

                    ScriptBuilder.Append($@"
                        CREATE {uniqueSql} NONCLUSTERED INDEX {indexName} 
                            ON {tableName.FullyQualifiedTableName} ({keyColumns.ToCsv()})
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
                var constraintName = columnCheckConstraint.MapConstraintNameToTargetAndEnsureUniqueness(tableName);
                ScriptBuilder.Append($@"
                    --Adding Column Check Constraint
                    ALTER TABLE {tableName.FullyQualifiedTableName} WITH CHECK ADD CONSTRAINT {constraintName} CHECK {columnCheckConstraint.CheckClause};
                ");
            }
            return this;
        }

        public MaterializedDataScriptBuilder AddColumnDefaultConstraints(TableNameTerm tableName, params ColumnDefaultConstraintDefinition[] columnDefaultConstraints)
        {
            foreach (var columnDefaultConstraint in columnDefaultConstraints)
            {
                var constraintName = columnDefaultConstraint.MapConstraintNameToTargetAndEnsureUniqueness(tableName);
                var columnName = columnDefaultConstraint.ColumnName.QualifySqlTerm();
                ScriptBuilder.Append($@"
                    --Adding Column Default Constraint
                    ALTER TABLE {tableName.FullyQualifiedTableName} ADD CONSTRAINT {constraintName} DEFAULT {columnDefaultConstraint.Definition} FOR {columnName};
                ");
            }
            return this;
        }

        public MaterializedDataScriptBuilder FinishSqlScript()
        {
            if (!IsSqlScriptFinished)
            {
                //Insert Variables at the Top if any exist...
                if (Variables.HasAny())
                {
                    var variablesScript = string.Concat(string.Join(Environment.NewLine, Variables.Values), Environment.NewLine, Environment.NewLine);
                    ScriptBuilder.Insert(0, variablesScript);
                }

                //Add our Successful Status result to the bottom...
                ScriptBuilder.Append($@"
                    --Return IsSuccessful = true once completed...
                    SELECT IsSuccessful = CAST(1 as BIT); 
                ");

                IsSqlScriptFinished = true;
            }

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

        #region Helpers

        private static string GetCheckClause(bool executeConstraintValidation) => executeConstraintValidation ? "WITH CHECK" : string.Empty;

        private static string GetNoCheckClause(bool executeConstraintValidation) => executeConstraintValidation ? string.Empty : "WITH NOCHECK";

        #endregion
    }
}
