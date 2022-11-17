using FastMember;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using SqlBulkHelpers.SqlBulkHelpers.CustomExtensions;
using LazyCacheHelpers;

namespace SqlBulkHelpers
{
    /// <summary>
    /// Marker interface to denote that there is no Model to be used to get Mapping Details for...
    /// </summary>
    internal interface ISkipMappingLookup
    {
    }

    internal static class MappingAttributeNames
    {
        public const string RepoDbFieldMapAttributeName = "MapAttribute";
        public const string RepoDbFieldMapAttributePropertyName = "Name";
        public const string LinqToDbFieldMapAttributeName = "ColumnAttribute";
        public const string LinqToDbFieldMapAttributePropertyName = "Name";

        public const string RepoDbTableMapAttributeName = "MapAttribute";
        public const string RepoDbTableMapAttributePropertyName = "Name";
        public const string DapperTableMapAttributeName = "TableAttribute";
        public const string DapperTableMapAttributePropertyName = "Name";
        //NOTE: LinqToDb uses the identical Attribute Name and Property name as Dapper so it will be handled along with Dapper!
        public const string LinqToDbTableMapAttributeName = "TableAttribute";
        public const string LinqToDbTableMapAttributePropertyName = "Name";
    }

    public class SqlBulkHelpersProcessingDefinition
    {
        private static readonly LazyStaticInMemoryCache<string, SqlBulkHelpersProcessingDefinition> _processingDefinitionsLazyCache = new LazyStaticInMemoryCache<string, SqlBulkHelpersProcessingDefinition>();
        private static readonly Type _skipMappingLookupType = typeof(ISkipMappingLookup);

        public static SqlBulkHelpersProcessingDefinition GetProcessingDefinition<T>(TableColumnDefinition identityColumnDefinition = null)
        {
            var type = typeof(T);

            var processingDefinition = _processingDefinitionsLazyCache.GetOrAdd(
                key: $"[Type={type.Name}][Identity={identityColumnDefinition?.ColumnName ?? "N/A"}]",  //Cache Key
                cacheValueFactory: key =>
                {
                    var propertyInfos = type.GetProperties().Select(pi => new PropInfoDefinition(pi, identityColumnDefinition)).ToList();
                    var newProcessingDefinition = new SqlBulkHelpersProcessingDefinition(propertyInfos, type, isRowNumberColumnNameEnabled: true);
                    return newProcessingDefinition;
                }
            );

            return processingDefinition;
        }
        
        protected SqlBulkHelpersProcessingDefinition(List<PropInfoDefinition> propertyDefinitions, Type entityType, bool isRowNumberColumnNameEnabled = true)
        {
            IsMappingLookupEnabled = !_skipMappingLookupType.IsAssignableFrom(entityType);
            PropertyDefinitions = propertyDefinitions.AssertArgumentIsNotNull(nameof(propertyDefinitions)).ToArray();
            IsRowNumberColumnNameEnabled = isRowNumberColumnNameEnabled;
            MappedDbTableName = GetMappedDbTableName(entityType);
            IdentityPropDefinition = propertyDefinitions.FirstOrDefault(p => p.IsIdentityProperty);

            if (entityType.FindAttributes(nameof(SqlBulkTableAttribute)).FirstOrDefault() is SqlBulkTableAttribute tableMappingAttr)
            {
                //NOTES: Defaults to true but can be overriden by the configuration on the Table attribute.
                UniqueMatchMergeValidationEnabled = tableMappingAttr.UniqueMatchMergeValidationEnabled;
            }

            //If any Match Qualifier Fields are noted (by Attributes annotations we load them into the Match Qualifier expression...
            var matchQualifierMappedDbColumnNames = propertyDefinitions
                .Where(p => p.IsMatchQualifier)
                .Select(p => p.MappedDbColumnName)
                .ToList();

            if (matchQualifierMappedDbColumnNames.Any())
                MergeMatchQualifierExpressionFromEntityModel = new SqlMergeMatchQualifierExpression(matchQualifierMappedDbColumnNames)
                {
                    //NOTE: We need to ensure that our Merge Qualifier Expression configuration matches what may have been configured on Table attribute.
                    ThrowExceptionIfNonUniqueMatchesOccur = UniqueMatchMergeValidationEnabled
                };
        }

        /// <summary>
        /// Determines if the Entity Type Mapping information is enabled or if it should be ignored (e.g. implements ISkipMappingLookup)
        /// </summary>
        public bool IsMappingLookupEnabled { get; protected set; }

        public PropInfoDefinition[] PropertyDefinitions { get; protected set; }

        public string MappedDbTableName { get; protected set; }

        public bool IsRowNumberColumnNameEnabled { get; protected set; }

        public PropInfoDefinition IdentityPropDefinition { get; protected set; }

        public SqlMergeMatchQualifierExpression MergeMatchQualifierExpressionFromEntityModel { get; protected set; }

        public bool UniqueMatchMergeValidationEnabled { get; protected set; } = true;
        
        protected string GetMappedDbTableName(Type entityType)
        {
            var mappingAttribute = entityType.FindAttributes(
                nameof(SqlBulkTableAttribute),
                MappingAttributeNames.RepoDbTableMapAttributeName, 
                MappingAttributeNames.DapperTableMapAttributeName
                //NOTE: Removed because the value is identical to Dapper so it will be handled above.
                //MappingAttributeNames.LinqToDbTableMapAttributeName
            ).FirstOrDefault();

            switch (mappingAttribute)
            {
                //Default to the Class Type Name...
                case null:
                    return entityType.Name;
                case SqlBulkTableAttribute sqlBulkTableAttr:
                    return sqlBulkTableAttr.FullyQualifiedTableName;
                default:
                {
                    var attrAccessor = ObjectAccessor.Create(mappingAttribute);

                    switch (mappingAttribute.GetType().Name)
                    {
                        case MappingAttributeNames.RepoDbTableMapAttributeName:
                            return attrAccessor[MappingAttributeNames.RepoDbTableMapAttributePropertyName].ToString();
                        //NOTE: Dapper and LinqToDb actually have the SAME Attribute & Property Name so this handles both...
                        case MappingAttributeNames.DapperTableMapAttributeName:
                            return attrAccessor[MappingAttributeNames.DapperTableMapAttributePropertyName].ToString();
                        //NOTE: Removed because this conflicts with Dapper and both will be handled above.
                        //case MappingAttributeNames.LinqToDbTableMapAttributeName:
                        //    return attrAccessor[MappingAttributeNames.LinqToDbTableMapAttributePropertyName].ToString();
                        default:
                            return entityType.Name;
                    }
                }
            }
        }
    }

    public class PropInfoDefinition
    {
        public PropInfoDefinition(PropertyInfo propInfo, TableColumnDefinition identityColumnDef = null)
        {
            this.PropInfo = propInfo;
            this.PropertyName = propInfo.Name;
            this.PropertyType = propInfo.PropertyType;
            this.MappedDbColumnName = GetMappedDbColumnName(propInfo);
            this.IsMatchQualifier = propInfo.FindAttributes(nameof(SqlBulkMatchQualifierAttribute)).Any();
            //Early determination if a Property is an Identity Property for Fast processing later...
            this.IsIdentityProperty = identityColumnDef?.ColumnName?.Equals(propInfo.Name, StringComparison.OrdinalIgnoreCase) ?? false;
        }

        public string PropertyName { get; private set; }
        public string MappedDbColumnName { get; private set; }
        public bool IsIdentityProperty { get; private set; }
        public bool IsMatchQualifier { get; private set; }
        public PropertyInfo PropInfo { get; private set; }
        public Type PropertyType { get; private set; }

        public override string ToString()
        {
            return $"{this.PropertyName} [{this.PropertyType.Name}]";
        }

        protected string GetMappedDbColumnName(PropertyInfo propInfo)
        {
            var mappingAttribute = propInfo.FindAttributes(
                nameof(SqlBulkColumnAttribute), 
                MappingAttributeNames.RepoDbFieldMapAttributeName, 
                MappingAttributeNames.LinqToDbFieldMapAttributeName
            ).FirstOrDefault();

            switch (mappingAttribute)
            {
                //Default to the Class Property Name...
                case null:
                    return propInfo.Name;
                case SqlBulkColumnAttribute sqlBulkColumnAttr:
                    return sqlBulkColumnAttr.Name;
                default:
                {
                    var attrAccessor = ObjectAccessor.Create(mappingAttribute);

                    switch (mappingAttribute.GetType().Name)
                    {
                        case MappingAttributeNames.RepoDbFieldMapAttributeName:
                            return attrAccessor[MappingAttributeNames.RepoDbFieldMapAttributePropertyName].ToString();
                        case MappingAttributeNames.LinqToDbFieldMapAttributeName:
                            return attrAccessor[MappingAttributeNames.LinqToDbFieldMapAttributePropertyName].ToString();
                        default:
                            return propInfo.Name;
                    }
                }
            }
        }
    }
}
