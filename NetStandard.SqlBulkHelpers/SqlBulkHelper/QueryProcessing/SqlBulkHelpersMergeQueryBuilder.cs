using System;
using System.Collections.Generic;
using System.Linq;
using SqlBulkHelpers.CustomExtensions;

namespace SqlBulkHelpers
{
    public class SqlBulkHelpersMergeScriptBuilder
    {
        public const string SPACE = " ";

        public virtual SqlMergeScriptResults BuildSqlMergeScripts(
            SqlBulkHelpersTableDefinition tableDefinition,
            SqlBulkHelpersProcessingDefinition processingDefinition,
            SqlBulkHelpersMergeAction mergeAction, 
            SqlMergeMatchQualifierExpression matchQualifierExpressionParam = null,
            bool enableIdentityInsert = false
        )
        {
            tableDefinition.AssertArgumentIsNotNull(nameof(tableDefinition));

            //NOTE: BBernard - This temp table name MUST begin with 1 (and only 1) hash "#" to ensure it is a Transaction Scoped table!
            var sanitizedTableName = tableDefinition.TableName.EnforceUnderscoreTableNameTerm();
            var tempStagingTableName = $"#SqlBulkHelpers_STAGING_{sanitizedTableName}".MakeTableNameUnique();
            var tempOutputIdentityTableName = $"#SqlBulkHelpers_OUTPUT_IDENTITY_TABLE".MakeTableNameUnique();

            //Validate the MatchQualifiers that may be specified, and limit to ONLY valid fields of the Table Definition...
            //NOTE: We use the parameter argument for Match Qualifier if specified, otherwise we fall-back to to use the Identity Column.
            SqlMergeMatchQualifierExpression sanitizedQualifierExpression = null;
            //NOTE: We use the parameter argument for Match Qualifier if specified, otherwise we fall-back to to use what may
            //      have been configured on the Entity model via SqlMatchQualifier property attributes.
            var matchQualifierExpression = matchQualifierExpressionParam ?? processingDefinition.MergeMatchQualifierExpressionFromEntityModel;
            if (matchQualifierExpression != null)
            {
                //Get Sanitized mapped names for Fields Specified as follows:
                //1) If it's already an exact Table Match then we use it...
                //2) If not then, when Mapping Lookups are enabled, we look to the Class/Model Processing Definition to determine if it's a valid
                //      property with Mapped Database name that we should use instead...
                //3) Finally, Mapped Fields are STILL checked to ensure they actually exist as valid Table Columns, otherwise they are excluded!
                var sanitizedQualifierFields = new List<SqlMatchQualifierField>();
                foreach (var qualifierField in matchQualifierExpression.MatchQualifierFields)
                {
                    if (tableDefinition.FindColumnCaseInsensitive(qualifierField.SanitizedName) != null)
                    {
                        sanitizedQualifierFields.Add(qualifierField);
                    }
                    else if (processingDefinition.IsMappingLookupEnabled)
                    {
                        var propDef = processingDefinition.FindPropDefinitionByNameCaseInsensitive(qualifierField.SanitizedName);
                        if(propDef != null && tableDefinition.FindColumnCaseInsensitive(propDef.MappedDbColumnName) != null)
                            sanitizedQualifierFields.Add(new SqlMatchQualifierField(propDef.MappedDbColumnName));
                    }
                }

                //If we have valid Fields, then we must re-initialize a valid Qualifier Expression parameter with ONLY the valid fields...
                sanitizedQualifierExpression = new SqlMergeMatchQualifierExpression(sanitizedQualifierFields)
                {
                    //Need to correctly copy over the original setting for Non Unique Match validation!
                    ThrowExceptionIfNonUniqueMatchesOccur = matchQualifierExpression.ThrowExceptionIfNonUniqueMatchesOccur
                };
            }
            else if (tableDefinition.PrimaryKeyConstraint != null)
            {
                //By Default we use the Primary Key Fields as the Qualifier Fields! And, if an Identity Column exists it is not necessarily the Unique PKey.
                var pkeySortedColumnNames = tableDefinition.PrimaryKeyConstraint.KeyColumns
                    .OrderBy(k => k.OrdinalPosition)
                    .Select(k => k.ColumnName);

                sanitizedQualifierExpression = new SqlMergeMatchQualifierExpression(pkeySortedColumnNames)
                {
                    //PKey Matches should always be Unique!
                    ThrowExceptionIfNonUniqueMatchesOccur = true
                };
            }

            //Validate that we have a valid state:
            //1. An Identity Column which can be used as the default Match Qualifier!
            //2. A set of Match Qualifier Fields is specified and used as an override, or required if no Identity Column exists to be used as default.
            if (sanitizedQualifierExpression == null || sanitizedQualifierExpression.MatchQualifierFields.IsNullOrEmpty())
                throw new ArgumentException(
                $"No valid match qualifiers could be resolved for the target table {tableDefinition.TableFullyQualifiedName}, and the table does"
                        + " not have a Primary Key defined to be used as the default match qualifier. One or the other must be"
                        + " provided/exist to safely match the rows during the bulk merging process."
                );

            //Initialize Identity & other Column processing references...
            var columnNamesListWithoutIdentity = tableDefinition.GetColumnNames(includeIdentityColumn: false);
            var columnNamesWithoutIdentityCsv = columnNamesListWithoutIdentity.QualifySqlTerms().ToCsv();

            var columnNamesListIncludingIdentity = tableDefinition.GetColumnNames();
            var columnNamesIncludingIdentityCsv = columnNamesListIncludingIdentity.QualifySqlTerms().ToCsv();

            var hasIdentityColumn = tableDefinition.IdentityColumn != null;
            var enableIdentityValueRetrieval =  hasIdentityColumn && !enableIdentityInsert;
            var identityColumn = tableDefinition.IdentityColumn;
            var identityColumnName = identityColumn?.ColumnName;
            var identityColumnDataType = identityColumn?.DataType;

            //Dynamically build the Merge Match Qualifier Fields Expression
            //NOTE: This is an optional parameter when an Identity Column exists as it is initialized to the IdentityColumn as a Default (Validated above!)
            var mergeMatchQualifierExpressionSql = BuildMergeMatchQualifierExpressionSql(sanitizedQualifierExpression);

            //Dynamically Build the Merge Temp tables, Merge Output clause, & Output SELECT query...
            //NOTE: If hasIdentityColumn is true these will all include the Identity, otherwise they will exclude them for small performance improvement;
            //          but we MUST include it even if we aren't retrieving it (e.g. enableIdentityInsert = true) because it may be (and usually is) part of the
            //          PKey or Match Qualifier Expression!
            //NOTE: THe ROWNUMBER_COLUMN_NAME (3'rd Column) IS CRITICAL to include because SqlBulkCopy and Sql Server OUTPUT clause do not
            //          preserve Order; e.g. it may change based on execution plan (indexes/no indexes, etc.).
            var mergeTempTablesSql = $@"
                --TEMP Staging/Loading Table...
                SELECT TOP(0)
                    {(hasIdentityColumn ? $"[{identityColumnName}] = CONVERT({identityColumnDataType}, -1)," : string.Empty)}
                    {columnNamesWithoutIdentityCsv},
                    [{SqlBulkHelpersConstants.ROWNUMBER_COLUMN_NAME}] = CONVERT(INT, -1)
                INTO [{tempStagingTableName}] 
                FROM {tableDefinition.TableFullyQualifiedName};

                --TEMP Output Return/Retrieval Table...
                SELECT TOP(0)
                    [{SqlBulkHelpersConstants.ROWNUMBER_COLUMN_NAME}] = CONVERT(INT, -1)
                    {(enableIdentityValueRetrieval ? $",[IDENTITY_ID] = CONVERT({identityColumnDataType}, -1)" : string.Empty)}
                    --,[MERGE_ACTION] = CONVERT(VARCHAR(10), '') --Removed as Small Performance Improvement since the Action is not used.
                INTO [{tempOutputIdentityTableName}];
            ";

            var mergeOutputSql = $@"
                OUTPUT 
                    source.[{SqlBulkHelpersConstants.ROWNUMBER_COLUMN_NAME}]
                    {(enableIdentityValueRetrieval ? $",INSERTED.[{identityColumnName}]" : string.Empty)}
                    --, $action --Removed as Small Performance Improvement since the Action is not used.
                INTO [{tempOutputIdentityTableName}] (
                    [{SqlBulkHelpersConstants.ROWNUMBER_COLUMN_NAME}]
                    {(enableIdentityValueRetrieval ? $",[IDENTITY_ID]" : string.Empty)}
                    --, [MERGE_ACTION] --Removed as Small Performance Improvement since the Action is not used.
                );

                SELECT 
                    [{SqlBulkHelpersConstants.ROWNUMBER_COLUMN_NAME}]
                    {(enableIdentityValueRetrieval ? $", [IDENTITY_ID]" : string.Empty)}
                    --,[MERGE_ACTION] --Removed as Small Performance Improvement since the Action is not used.
                FROM [{tempOutputIdentityTableName}]
                ORDER BY [{SqlBulkHelpersConstants.ROWNUMBER_COLUMN_NAME}] ASC;
            ";

            var columnNamesToInsertOrUpdateCsv = enableIdentityInsert ? columnNamesIncludingIdentityCsv : columnNamesWithoutIdentityCsv;
            var columnNamesListToInsertOrUpdate = enableIdentityInsert ? columnNamesListIncludingIdentity : columnNamesListWithoutIdentity;

            string mergeInsertSql = string.Empty;
            if (mergeAction.HasFlag(SqlBulkHelpersMergeAction.Insert))
            {
                mergeInsertSql = $@"
                    WHEN NOT MATCHED BY TARGET THEN
                        INSERT ({columnNamesToInsertOrUpdateCsv}) 
                        VALUES ({columnNamesListToInsertOrUpdate.Select(c => $"source.[{c}]").ToCsv()})
                ";
            }

            string mergeUpdateSql = string.Empty;
            if (mergeAction.HasFlag(SqlBulkHelpersMergeAction.Update))
            {
                mergeUpdateSql = $@"
                    WHEN MATCHED THEN
                        UPDATE SET {columnNamesListToInsertOrUpdate.Select(c => $"target.[{c}] = source.[{c}]").ToCsv()}
                ";
            }

            //Build the FULL Dynamic Merge Script here...
            //BBernard - 2019-08-07
            //NOTE: This is ALL now completed very efficiently on the Sql Server Database side with NO unnecessary round trips to the Database!
            //NOTE: We now sort on the RowNumber column that we define; this FIXES issue with SqlBulkCopy.WriteToServer()
            //      where the order of data being written is NOT guaranteed, and there is still no support for the ORDER() hint.
            //      In general it results in inverting the order of data being sent in Bulk which then resulted in Identity
            //      values being incorrect based on the order of data specified.
            //NOTE: We MUST SORT the OUTPUT Results by ROWNUMBER and then by IDENTITY Column in case there are multiple matches due to
            //      custom match Qualifiers; this ensures that data is sorted in a way that postprocessing
            //      can occur & be validated as expected.
            string mergeProcessScriptSql = $@"

                {(enableIdentityInsert ? $"SET IDENTITY_INSERT {tableDefinition.TableFullyQualifiedName} ON;" : string.Empty)}

                MERGE {tableDefinition.TableFullyQualifiedName} as target
				USING (
					SELECT TOP 100 PERCENT * 
					FROM [{tempStagingTableName}] 
					ORDER BY [{SqlBulkHelpersConstants.ROWNUMBER_COLUMN_NAME}] ASC
				) as source
                ON {mergeMatchQualifierExpressionSql}
                {mergeUpdateSql}
                {mergeInsertSql}

                {mergeOutputSql}

                {(enableIdentityInsert ? $"SET IDENTITY_INSERT {tableDefinition.TableFullyQualifiedName} OFF;" : string.Empty)}

                DROP TABLE IF EXISTS [{tempStagingTableName}];
                DROP TABLE IF EXISTS [{tempOutputIdentityTableName}];
            ";

            return new SqlMergeScriptResults(
                tempStagingTableName,
                tempOutputIdentityTableName,
                mergeTempTablesSql,
                mergeProcessScriptSql,
                sanitizedQualifierExpression
            );
        }

        /// <summary>
        /// BBernard - 12/07/2020
        /// Delegate method to build the Match Qualification expression text from the MatchQualifierExpression model provided.
        /// NOTE: This can be overridden if needed to provide highly specialized logic and reutrn any match qualification expression
        ///     text needed in edge use-cases.
        /// </summary>
        /// <param name="matchQualifierExpression"></param>
        /// <returns></returns>
        protected virtual string BuildMergeMatchQualifierExpressionSql(SqlMergeMatchQualifierExpression matchQualifierExpression)
        {
            //Construct the full Match Qualifier Expression
            var qualifierFields = matchQualifierExpression.MatchQualifierFields.Select(f =>
                $"target.[{f.SanitizedName}] = source.[{f.SanitizedName}]"
            );

            var fullExpressionText = string.Join(" AND ", qualifierFields);
            return fullExpressionText;
        }
    }

    public class SqlMergeScriptResults
    {
        public SqlMergeScriptResults(
            string tempStagingTableName, 
            string tempOutputTableName, 
            string tempTableScript, 
            string mergeProcessScript,
            SqlMergeMatchQualifierExpression sqlMatchQualifierExpression
        )
        {
            this.SqlScriptToInitializeTempTables = tempTableScript;
            this.SqlScriptToExecuteMergeProcess = mergeProcessScript;
            this.TempStagingTableName = tempStagingTableName;
            this.TempOutputTableName = tempOutputTableName;
            this.SqlMatchQualifierExpression = sqlMatchQualifierExpression;
        }

        public string TempOutputTableName { get; private set; }
        public string TempStagingTableName { get; private set; }
        public string SqlScriptToInitializeTempTables { get; private set; }
        public string SqlScriptToExecuteMergeProcess { get; private set; }
        public SqlMergeMatchQualifierExpression SqlMatchQualifierExpression { get; private set; }
    }
}
