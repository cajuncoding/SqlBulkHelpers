using System;
using System.Collections.Generic;
using System.Linq;
using Microsoft.Data.SqlClient;
using FastMember;
using SqlBulkHelpers.Interfaces;

namespace SqlBulkHelpers
{
    internal static class TypeCache
    {
        public static readonly Type SqlBulkHelperIdentitySetter = typeof(ISqlBulkHelperIdentitySetter);
        public static readonly Type SqlBulkHelperBigIntIdentitySetter = typeof(ISqlBulkHelperBigIntIdentitySetter);
    }

    //BBernard - Base Class for future flexibility...
    internal abstract class BaseSqlBulkHelper<T> : BaseHelper<T> where T : class
    {
        protected static Type CachedEntityType { get; } = typeof(T);

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
            public long IdentityId { get; set; }
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
            //Small performance improvement here by pre-determining which, if any, identity setters may be implemented by the client;
            //  since this isn't the most often use case it's helpful to more efficiently skip the type checks while processing...
            bool implementsIntIdentitySetter = TypeCache.SqlBulkHelperIdentitySetter.IsAssignableFrom(CachedEntityType);
            bool implementsBigIntIdentitySetter = TypeCache.SqlBulkHelperBigIntIdentitySetter.IsAssignableFrom(CachedEntityType);
            bool identitySetterInterfaceSupported = implementsIntIdentitySetter || implementsBigIntIdentitySetter;

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

            if (hasIdentityColumn && !identitySetterInterfaceSupported)
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

            var identitySetterAction = hasIdentityColumn
                ? ResolveIdentitySetterAction(entityList, identityPropertyName)
                : null;

            var entityResultsList = new List<T>();
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
                identitySetterAction?.Invoke(entity, mergeResult);

                entityResultsList.Add(entity);
            }

            //Return the Updated Entities List (for fluent chain-ability) and easier to read code
            //NOTE: even though we have actually mutated the original list by reference this is very intuitive and helps with code readability.
            return entityResultsList;
        }

        private Action<T, MergeResult> ResolveIdentitySetterAction(List<T> entityList, string identityPropertyName)
        {
            var sampleEntity = entityList.FirstOrDefault();
            switch (sampleEntity)
            {
                case null:
                    //Break out to end for Invalid Operation handling...
                    break;
                //BBernard
                //For Performance if the entity type supports our Interfaces then we use those to set the Identity ID delegating all logic to the class to handle.
                case ISqlBulkHelperIdentitySetter _:
                    //Downcast our MergeResult long IdentityId to int...
                    return (entity, mergeResult) => ((ISqlBulkHelperIdentitySetter)entity).SetIdentityId((int)mergeResult.IdentityId);
                case ISqlBulkHelperBigIntIdentitySetter _:
                    return (entity, mergeResult) => ((ISqlBulkHelperBigIntIdentitySetter)entity).SetIdentityId(mergeResult.IdentityId);
                default:
                {
                    //Create our TypeAccessor once here, so it can be captured by scope in our Action but is NOT CREATED on each Action execution!
                    //NOTE: This also helps encapsulate our use of TypeAccessor in case we choose another approach to setting the property in the future!
                    var fastTypeAccessor = TypeAccessor.Create(CachedEntityType);
                    var identityPropType = fastTypeAccessor[sampleEntity, identityPropertyName]?.GetType();
                    if (identityPropType != null)
                    {
                        //BBernard
                        //For Performance we try to identity the primary Integer types and implement the Setter Action with explicit casting, however
                        //  as a fallback we will attempt to generically convert the type via Convert.ChangeType() for really strange edge cases where the Model type is
                        //  something awkward... (Heaven forbid a string), but hey we'll try to make it work.
                        if (identityPropType == typeof(long)) //BIGINT Sql Type
                        {
                            //MergeResult IdentityId is already a Long to support the superset of any other Int property types by down-casting...
                            return (entity, mergeResult) => fastTypeAccessor[entity, identityPropertyName] = mergeResult.IdentityId;
                        }
                        else if (identityPropType == typeof(int)) //INT Sql Type
                        {
                            return (entity, mergeResult) => fastTypeAccessor[entity, identityPropertyName] = (int)mergeResult.IdentityId;
                        }
                        else if (identityPropType == typeof(short)) //SMALLINT Sql Type
                        {
                            return (entity, mergeResult) => fastTypeAccessor[entity, identityPropertyName] = (short)mergeResult.IdentityId;
                        }
                        else if (identityPropType == typeof(byte)) //TINYINT Sql Type
                        {
                            return (entity, mergeResult) => fastTypeAccessor[entity, identityPropertyName] = (byte)mergeResult.IdentityId;
                        }
                        else //For NUMERIC(X, 0) Sql Type or any other, we attempt to generically change the type to match...
                        {
                            return (entity, mergeResult) => fastTypeAccessor[entity, identityPropertyName] = Convert.ChangeType(mergeResult.IdentityId, identityPropType);
                        }
                    }

                    //Break out to end for Invalid Operation handling...
                    break;
                }
            }
            
            return (entity, mergeResult) => throw new InvalidOperationException($"Unable to properly map the Identity value result into the entity Property [{identityPropertyName}] of type [{entity.GetType()}].");
        }

    }
}
