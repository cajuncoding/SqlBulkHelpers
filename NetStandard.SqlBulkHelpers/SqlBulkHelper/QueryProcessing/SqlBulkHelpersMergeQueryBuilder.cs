using System;
using System.Linq;

namespace SqlBulkHelpers
{
    public class SqlBulkHelpersMergeScriptBuilder
    {
        public virtual SqlMergeScriptResults BuildSqlMergeScripts(
            SqlBulkHelpersTableDefinition tableDefinition, 
            SqlBulkHelpersMergeAction mergeAction, 
            SqlMergeMatchQualifierExpression matchQualifierExpression = null
        )
        {
            tableDefinition.AssertArgumentIsNotNull(nameof(tableDefinition));

            //NOTE: BBernard - This temp table name MUST begin with 1 (and only 1) hash "#" to ensure it is a Transaction Scoped table!
            var tempStagingTableName = $"#SqlBulkHelpers_STAGING_TABLE_{Guid.NewGuid()}";
            var tempOutputIdentityTableName = $"#SqlBulkHelpers_OUTPUT_IDENTITY_TABLE_{Guid.NewGuid()}";
            var hasIdentityColumn = tableDefinition.IdentityColumn != null;
            var identityColumnName = tableDefinition.IdentityColumn?.ColumnName ?? string.Empty;
            bool uniqueMatchMergeValidationEnabled = matchQualifierExpression?.ThrowExceptionIfNonUniqueMatchesOccur ?? true;

            //Validate the MatchQualifiers that may be specified, and limit to ONLY valid fields of the Table Definition...
            //NOTE: We use the parameter argument for Match Qualifier if specified, otherwise we fall-back to to use the Identity Column.
            var mergeQualifierExpression = matchQualifierExpression ?? (hasIdentityColumn ? new SqlMergeMatchQualifierExpression(identityColumnName): default);

            var sanitizedQualifierFields = mergeQualifierExpression?.MatchQualifierFields.Where(
                q => tableDefinition.FindColumnCaseInsensitive(q.SanitizedName) != null
            );

            //Re-initialize a valid Qualifier Expression parameter with ONLY the valid fields...
            var sanitizedQualifierExpression = new SqlMergeMatchQualifierExpression(sanitizedQualifierFields)
            {
                //Need to correctly copy over the original setting for Non Unique Match validation!
                ThrowExceptionIfNonUniqueMatchesOccur = uniqueMatchMergeValidationEnabled
            };

            //Validate that we have a valid state:
            //1. An Identity Column which can be used as the default Match Qualifier!
            //2. A set of Match Qualifier Fields is specified and used as an override, or required if no Identity Column exists to be used as default.
            if (sanitizedQualifierExpression.MatchQualifierFields.IsNullOrEmpty())
                throw new ArgumentException(
                $"No valid match qualifiers could be resolved for the target table {tableDefinition.TableFullyQualifiedName}, and the table does"
                        + " not have an Identity Column to be used as the default match qualifier. One or the other must be"
                        + " provided/exist to safely match the rows during the bulk merging process."
                );

            var columnNamesListWithoutIdentity = tableDefinition.GetColumnNames(false);
            var columnNamesWithoutIdentityCSV = columnNamesListWithoutIdentity.Select(c => $"[{c}]").ToCSV();

            //Dynamically build the Merge Match Qualifier Fields Expression
            //NOTE: This is an optional parameter when an Identity Column exists as it is initialized to the IdentityColumn as a Default (Validated above!)
            var mergeMatchQualifierExpressionSql = BuildMergeMatchQualifierExpressionSql(mergeQualifierExpression);

            //Initialize/Create the Staging Table!
            //NOTE: THe ROWNUMBER_COLUMN_NAME (3'rd Column) IS CRITICAL because SqlBulkCopy and Sql Server OUTPUT claus do not
            //          preserve Order; e.g. it may change based on execution plan (indexes/no indexes, etc.).
            string mergeTempTablesSql;
            if (hasIdentityColumn)
            {
                mergeTempTablesSql = $@"
                    SELECT TOP(0)
                        -1 as [{identityColumnName}],
                        {columnNamesWithoutIdentityCSV},
                        -1 as [{SqlBulkHelpersConstants.ROWNUMBER_COLUMN_NAME}] 
                    INTO [{tempStagingTableName}] 
                    FROM {tableDefinition.TableFullyQualifiedName};

                    ALTER TABLE [{tempStagingTableName}] ADD PRIMARY KEY ([{identityColumnName}]);

                    SELECT TOP(0)
                        CAST('' AS nvarchar(10)) as [MERGE_ACTION],
                        CAST(-1 AS int) as [IDENTITY_ID], 
                        CAST(-1 AS int) [{SqlBulkHelpersConstants.ROWNUMBER_COLUMN_NAME}] 
                    INTO [{tempOutputIdentityTableName}];
                ";
            }
            else 
            {
                mergeTempTablesSql = $@"
                    SELECT TOP(0)
                        {columnNamesWithoutIdentityCSV},
                        -1 as [{SqlBulkHelpersConstants.ROWNUMBER_COLUMN_NAME}] 
                    INTO [{tempStagingTableName}] 
                    FROM {tableDefinition.TableFullyQualifiedName};
                ";

                //If specified (is true by Default) then we still use the Merge Output to validate unique updates/insert actions!
                if (mergeQualifierExpression.ThrowExceptionIfNonUniqueMatchesOccur)
                {
                    mergeTempTablesSql = $@"
                        {mergeTempTablesSql}
                        
                        SELECT TOP(0)
                            CAST('' AS nvarchar(10)) as [MERGE_ACTION],
                            CAST(-1 AS int) [{SqlBulkHelpersConstants.ROWNUMBER_COLUMN_NAME}] 
                        INTO [{tempOutputIdentityTableName}];
                    ";
                }
            }

            //NOTE: This is ALL now completed very efficiently on the Sql Server Database side with
            //          NO unnecessary round trips to the Database!
            string mergeInsertSql = string.Empty;
            if (mergeAction.HasFlag(SqlBulkHelpersMergeAction.Insert))
            {
                mergeInsertSql = $@"
                    WHEN NOT MATCHED BY TARGET THEN
                        INSERT ({columnNamesWithoutIdentityCSV}) 
                        VALUES ({columnNamesListWithoutIdentity.Select(c => $"source.[{c}]").ToCSV()})
                ";
            }

            string mergeUpdateSql = string.Empty;
            if (mergeAction.HasFlag(SqlBulkHelpersMergeAction.Update))
            {
                mergeUpdateSql = $@"
                    WHEN MATCHED THEN
                        UPDATE SET {columnNamesListWithoutIdentity.Select(c => $"target.[{c}] = source.[{c}]").ToCSV()} 
                ";
            }

            string mergeOutputSql = string.Empty;
            if (hasIdentityColumn)
            {
                //NOTE: We only Output results IF we have Identity Column data to return...
                mergeOutputSql = $@"
                    OUTPUT $action, INSERTED.[{identityColumnName}], source.[{SqlBulkHelpersConstants.ROWNUMBER_COLUMN_NAME}]
                        INTO [{tempOutputIdentityTableName}] ([MERGE_ACTION], [IDENTITY_ID], [{SqlBulkHelpersConstants.ROWNUMBER_COLUMN_NAME}]);

                    SELECT 
                        [{SqlBulkHelpersConstants.ROWNUMBER_COLUMN_NAME}], 
                        [IDENTITY_ID], 
                        [MERGE_ACTION]
                    FROM [{tempOutputIdentityTableName}]
                    ORDER BY [{SqlBulkHelpersConstants.ROWNUMBER_COLUMN_NAME}] ASC, [IDENTITY_ID] ASC;
                ";
            }

            string mergeCleanupSql;
            if (hasIdentityColumn || mergeQualifierExpression.ThrowExceptionIfNonUniqueMatchesOccur)
            {
                mergeCleanupSql = $@"
                    DROP TABLE [{tempStagingTableName}];
                    DROP TABLE [{tempOutputIdentityTableName}];
                ";
            }
            else
            {
                mergeCleanupSql = $@"
                    DROP TABLE [{tempStagingTableName}];
                ";
            }

            //Build the FULL Dynamic Merge Script here...
            //BBernard - 2019-08-07
            //NOTE: We now sort on the RowNumber column that we define; this FIXES issue with SqlBulkCopy.WriteToServer()
            //      where the order of data being written is NOT guaranteed, and there is still no support for the ORDER() hint.
            //      In general it results in inverting the order of data being sent in Bulk which then resulted in Identity
            //      values being incorrect based on the order of data specified.
            //NOTE: We MUST SORT the OUTPUT Results by ROWNUMBER and then by IDENTITY Column in case there are multiple matches due to
            //      custom match Qualifiers; this ensures that data is sorted in a way that postprocessing
            //      can occur & be validated as expected.
            string mergeProcessScriptSql = $@"
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

                {mergeCleanupSql}
            ";

            return new SqlMergeScriptResults(
                tempStagingTableName,
                tempOutputIdentityTableName,
                mergeTempTablesSql,
                mergeProcessScriptSql,
                mergeQualifierExpression
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
