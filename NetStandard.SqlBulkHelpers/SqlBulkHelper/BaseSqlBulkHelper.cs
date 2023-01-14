using System;
using System.Collections.Generic;
using Microsoft.Data.SqlClient;
using FastMember;
using SqlBulkHelpers.Interfaces;

namespace SqlBulkHelpers
{
    internal static class TypeCache
    {
        public static readonly Type SqlBulkHelperIdentitySetter = typeof(ISqlBulkHelperIdentitySetter);
    }

    //BBernard - Base Class for future flexibility...
    internal abstract class BaseSqlBulkHelper<T> : BaseHelper<T> where T : class
    {
        #region Constructors

        /// <inheritdoc/>
        protected BaseSqlBulkHelper(ISqlBulkHelpersConfig bulkHelpersConfig = null)
            : base(bulkHelpersConfig)
        {
        }

        #endregion

        protected virtual SqlBulkCopy CreateSqlBulkCopyInternal(
            List<T> entities,
            SqlBulkHelpersTableDefinition tableDefinition,
            SqlBulkHelpersProcessingDefinition processingDefinition,
            SqlTransaction transaction
        )
        {
            //Initialize the BulkCopy Factory class with parameters...
            var factory = new SqlBulkCopyFactory(BulkHelpersConfig);
            var sqlBulkCopy = factory.CreateSqlBulkCopy(entities, processingDefinition, tableDefinition, transaction);
            return sqlBulkCopy;
        }

        protected virtual SqlMergeScriptResults BuildSqlMergeScriptsInternal(
            SqlBulkHelpersTableDefinition tableDefinition,
            SqlBulkHelpersProcessingDefinition processingDefinition,
            SqlBulkHelpersMergeAction mergeAction,
            SqlMergeMatchQualifierExpression matchQualifierExpression
        )
        {
            var mergeScriptBuilder = new SqlBulkHelpersMergeScriptBuilder();
            var sqlMergeScripts = mergeScriptBuilder.BuildSqlMergeScripts(
                tableDefinition,
                processingDefinition,
                mergeAction, 
                matchQualifierExpression
            );

            return sqlMergeScripts;
        }

        //NOTE: This is Protected Class because it is ONLY needed by the SqlBulkHelper implementations with Merge Operations 
        //          for organized code when post-processing results.
        protected class MergeResult
        {
            public int RowNumber { get; set; }
            public int IdentityId { get; set; }
            //public SqlBulkHelpersMergeAction MergeAction { get; set; }
        }

        protected virtual List<T> PostProcessEntitiesWithMergeResults(
            List<T> entityList, 
            List<MergeResult> mergeResultsList, 
            TableColumnDefinition identityColumnDefinition, 
            SqlMergeMatchQualifierExpression sqlMatchQualifierExpression
        )
        {
            entityList.AssertArgumentIsNotNull(nameof(entityList));
            mergeResultsList.AssertArgumentIsNotNull(nameof(mergeResultsList));

            bool uniqueMatchValidationEnabled = sqlMatchQualifierExpression.AssertArgumentIsNotNull(nameof(sqlMatchQualifierExpression)).ThrowExceptionIfNonUniqueMatchesOccur;
            bool hasIdentityColumn = identityColumnDefinition != null;

            //If there was no Identity Column or the validation of Unique Merge actions was disabled then we can
            //  short circuit the post-processing of results as there is nothing to do...
            if (!hasIdentityColumn && !uniqueMatchValidationEnabled)
                return entityList;

            //BBernard - 12/01/2021
            //Added Optimization to support interface based Identity Setter which may be optionally implemented
            //  directly on the models...
            //However, if the Generic Type doesn't implement our Interface ISqlBulkHelperIdentitySetter then
            //  we attempt to use Reflection to set the value...
            string identityPropertyName = null;

            Type entityType = typeof(T);
            TypeAccessor fastTypeAccessor = TypeAccessor.Create(entityType);

            if (hasIdentityColumn && !TypeCache.SqlBulkHelperIdentitySetter.IsAssignableFrom(entityType))
            {
                var processingDefinition = SqlBulkHelpersProcessingDefinition.GetProcessingDefinition<T>(identityColumnDefinition);
                identityPropertyName = processingDefinition.IdentityPropDefinition?.PropertyName;

                //If there is no Identity Property (e.g. no Identity PropInfo can be found)
                //  then we can skip any further processing of Identity values....
                if (identityPropertyName == null)
                    hasIdentityColumn = false;
            }

            ////Get all Items Inserted or Updated....
            //NOTE: With the support for Custom Match Qualifiers we really need to handle Inserts & Updates,
            //      so there's no reason to filter the merge results anymore; this is more performant.
            var uniqueMatchesHashSet = new HashSet<int>();

            var entityResultsList = new List<T>();

            //foreach (var mergeResult in mergeResultsList.Where(r => r.MergeAction.HasFlag(SqlBulkHelpersMergeAction.Insert)))
            foreach (var mergeResult in mergeResultsList)
            {
                //ONLY Process uniqueness validation if necessary... otherwise skip the logic altogether.
                if (uniqueMatchValidationEnabled)
                {
                    if (uniqueMatchesHashSet.Contains(mergeResult.RowNumber))
                    {
                        throw new ArgumentOutOfRangeException(
                            nameof(mergeResultsList), 
                            "The bulk action has resulted in multiple matches for the the specified Match Qualifiers"
                            + $" [{sqlMatchQualifierExpression}] so the original Entities List cannot be safely updated."
                            + " Verify that the Match Qualifier fields result in unique matches or, if intentional, then"
                            + " this validation check may be disabled on the SqlMergeMatchQualifierExpression parameter."
                        );
                    }
                    else
                    {
                        uniqueMatchesHashSet.Add(mergeResult.RowNumber);
                    }
                }

                //ONLY Process Identity value updates if appropriate... otherwise skip the logic altogether.
                //NOTE: List is 0 (zero) based, but our RowNumber is 1 (one) based.
                var entity = entityList[mergeResult.RowNumber - 1];
                if (hasIdentityColumn)
                {
                    //BBernard
                    //If the entity supports our interface we can set the value with native performance via the Interface!
                    if (entity is ISqlBulkHelperIdentitySetter identitySetterEntity)
                    {
                        identitySetterEntity.SetIdentityId(mergeResult.IdentityId);
                    }
                    else
                    {
                        //GENERICALLY Set the Identity Value to the Int value returned, this eliminates any dependency on a Base Class!
                        fastTypeAccessor[entity, identityPropertyName] = mergeResult.IdentityId;
                    }
                }

                entityResultsList.Add(entity);
            }

            //Return the Updated Entities List (for fluent chain-ability) and easier to read code
            //NOTE: even though we have actually mutated the original list by reference this is very intuitive and helps with code readability.
            return entityResultsList;
        }

    }
}
