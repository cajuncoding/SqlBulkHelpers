using Fasterflect;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using SqlBulkHelpers.SqlBulkHelpers.CustomExtensions;
using LazyCacheHelpers;
using SqlBulkHelpers.CustomExtensions;
using System.Collections.Immutable;
using SqlBulkHelpers.Interfaces;

namespace SqlBulkHelpers
{
    /// <summary>
    /// Marker interface to denote that there is no Model to be used to get Mapping Details for...
    /// </summary>
    internal interface ISkipMappingLookup
    {
    }

    internal static class RepoDbNames
    {
        public const string PropertyHandlerAttributeName = "PropertyHandlerAttribute";
        public const string PropertyHandlerSetOptionsClassName = "PropertyHandlerSetOptions";
        public const string PropertyHandlerTypePropertyName = "HandlerType";
        public const string PropertyHandlerSetMethodName = "Set"; //The Set method is used to write value to the DB!
    }

    internal static class MappingAttributeNames
    {
        //Field/Property Name Attributes
        public const string RepoDbFieldMapAttributeName = "MapAttribute";
        public const string RepoDbFieldMapAttributePropertyName = "Name";
        public const string LinqToDbFieldMapAttributeName = "ColumnAttribute";
        public const string LinqToDbFieldMapAttributePropertyName = "Name";
        //Table Name Attributes
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
        
        protected ILookup<string, PropInfoDefinition> PropInfoLookupByPropNameCaseInsensitive { get; }

        public static readonly Type SkipMappingLookupType = typeof(ISkipMappingLookup);

        public static SqlBulkHelpersProcessingDefinition GetProcessingDefinition<T>(TableColumnDefinition identityColumnDefinition = null)
            => GetProcessingDefinition(typeof(T), identityColumnDefinition);

        public static SqlBulkHelpersProcessingDefinition GetProcessingDefinition(Type type, TableColumnDefinition identityColumnDefinition = null)
        {
            type.AssertArgumentIsNotNull(nameof(type));

            var processingDefinition = _processingDefinitionsLazyCache.GetOrAdd(
                key: $"[Type={type.FullName}][Identity={identityColumnDefinition?.ColumnName ?? "N/A"}]",  //Cache Key
                cacheValueFactory: key =>
                {
                    var propertyInfos = type.Properties().Select(pi => new PropInfoDefinition(pi, identityColumnDefinition)).ToList();
                    var newProcessingDefinition = new SqlBulkHelpersProcessingDefinition(propertyInfos, type);
                    return newProcessingDefinition;
                }
            );

            return processingDefinition;
        }
        
        protected SqlBulkHelpersProcessingDefinition(IList<PropInfoDefinition> propertyDefinitions, Type entityType, bool isRowNumberColumnNameEnabled = true)
        {
            
            //All processed property definitions can be accessed here...
            AllPropertyDefinitions = propertyDefinitions.AssertArgumentIsNotNull(nameof(propertyDefinitions)).ToImmutableArray();
            //But for normal processing we ONLY include properties that are valid for updating (e.g. not explicitly ignored)...
            PropertyDefinitions = propertyDefinitions.Where(pd => !pd.IsIgnoredForUpdates).ToImmutableArray();

            IsMappingLookupEnabled = !SkipMappingLookupType.IsAssignableFrom(entityType);
            IsRowNumberColumnNameEnabled = isRowNumberColumnNameEnabled;
            MappedDbTableName = GetMappedDbTableName(entityType);
            IdentityPropDefinition = propertyDefinitions.FirstOrDefault(p => p.IsIdentityProperty);

            //Initialize Lookups for high performance processing since this will be cached and only initialized once...
            PropInfoLookupByPropNameCaseInsensitive = PropertyDefinitions.ToLookup(pi => pi.PropertyName, StringComparer.OrdinalIgnoreCase);

            if (entityType.Attribute<SqlBulkTableAttribute>() is SqlBulkTableAttribute tableMappingAttr)
                //NOTES: Defaults to true but can be overriden by the configuration on the Table attribute.
                UniqueMatchMergeValidationEnabled = tableMappingAttr.UniqueMatchMergeValidationEnabled;

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
        /// <summary>
        /// Returns only valid updatable property definitions (e.g. not ignored)
        /// </summary>
        public ImmutableArray<PropInfoDefinition> PropertyDefinitions { get; protected set; }
        public ImmutableArray<PropInfoDefinition> AllPropertyDefinitions { get; protected set; }
        public string MappedDbTableName { get; protected set; }
        public bool IsRowNumberColumnNameEnabled { get; protected set; }
        public PropInfoDefinition IdentityPropDefinition { get; protected set; }
        public SqlMergeMatchQualifierExpression MergeMatchQualifierExpressionFromEntityModel { get; protected set; }
        public bool UniqueMatchMergeValidationEnabled { get; protected set; } = true;
        public PropInfoDefinition this[string propName] => FindPropDefinitionByNameCaseInsensitive(propName);
        public PropInfoDefinition this[int index] => PropertyDefinitions[index];
        
        public PropInfoDefinition FindPropDefinitionByNameCaseInsensitive(string propertyName) 
            => PropInfoLookupByPropNameCaseInsensitive[propertyName].FirstOrDefault();

        protected string GetMappedDbTableName(Type entityType)
        {
            var mappingAttribute = entityType.FindAttributesByName(
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
                    switch (mappingAttribute.GetType().Name)
                    {
                        case MappingAttributeNames.RepoDbTableMapAttributeName:
                            return mappingAttribute.GetPropertyValue(MappingAttributeNames.RepoDbTableMapAttributePropertyName).AsString();
                        //NOTE: Dapper and LinqToDb actually have the SAME Attribute & Property Name so this handles both...
                        case MappingAttributeNames.DapperTableMapAttributeName:
                            return mappingAttribute.GetPropertyValue(MappingAttributeNames.DapperTableMapAttributePropertyName).AsString();
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
        private readonly MemberGetter _fasterflectPropertyValueGetter;

        public PropInfoDefinition(PropertyInfo propInfo, TableColumnDefinition identityColumnDef = null)
        {
            this.PropInfo = propInfo;
            this.PropertyName = propInfo.Name;
            this.PropertyType = propInfo.PropertyType;
            this.MappedDbColumnName = GetMappedDbColumnName(propInfo);
            this.IsMatchQualifier = propInfo.HasAttribute<SqlBulkMatchQualifierAttribute>();
            //Early determination if a Property is an Identity Property for Fast processing later...
            //NOTE: MappedDbColumnName will use annotation mapping if defined, otherwise it matches the original PropertyName...
            this.IsIdentityProperty = identityColumnDef?.ColumnName?.Equals(MappedDbColumnName, StringComparison.OrdinalIgnoreCase) ?? false;
            this.PropertyConverter = GetPropertyConverter(propInfo);

            //Initialize a fast Delegate based Property Value Getter for high performance access; this is now very easy with Fasterflect!
            //NOTE: Event though Fasterflect has internal caching There is still some minor overhead in initializing the Cache Key (CallInfo) internally
            //      which we can further avoid by initializing and keeping our Getter reference here for pure performance!
            _fasterflectPropertyValueGetter = propInfo.DelegateForGetPropertyValue();
            InvokePropertyValueGetter = new Func<object, object>(obj => {
                var propValue = _fasterflectPropertyValueGetter(obj);
                //If defined use the Property Converter, otherwise return the underlying value of the property...
                return this.PropertyConverter?.ConvertPropValue(propValue) ?? propValue;
            });

            //Added support to explicitly Ignore Properties on Models that may be transient and/or problemmatic so this eliminates
            //  them from consideration for any SQL Bulk processing...
            this.IsIgnoredForUpdates = propInfo.HasAttribute<SqlBulkIgnoreAttribute>();
        }

        public string PropertyName { get; protected set; }
        public bool IsIgnoredForUpdates { get; protected set; }
        public string MappedDbColumnName { get; protected set; }
        public bool IsIdentityProperty { get; protected set; }
        public bool IsMatchQualifier { get; protected set; }
        public PropertyInfo PropInfo { get; protected set; }
        public Type PropertyType { get; protected set; }
        public ISqlBulkHelpersPropertyConverter PropertyConverter { get; protected set; }
        public Func<object, object> InvokePropertyValueGetter { get; protected set; }

        public override string ToString() => $"{this.PropertyName} [{this.PropertyType.Name}]";

        protected static string GetMappedDbColumnName(PropertyInfo propInfo)
        {
            var mappingAttribute = propInfo.FindAttributesByName(
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
                    object attributeNameValue = null;
                    switch (mappingAttribute.GetType().Name)
                    {
                        case MappingAttributeNames.RepoDbFieldMapAttributeName:
                            attributeNameValue = mappingAttribute.GetPropertyValue(MappingAttributeNames.RepoDbFieldMapAttributePropertyName);
                            break;
                        case MappingAttributeNames.LinqToDbFieldMapAttributeName:
                            attributeNameValue = mappingAttribute.GetPropertyValue(MappingAttributeNames.LinqToDbFieldMapAttributePropertyName);
                            break;
                    }

                    return attributeNameValue.AsString() ?? propInfo.Name;
                }
            }
        }

        protected static ISqlBulkHelpersPropertyConverter GetPropertyConverter(PropertyInfo propInfo)
        {
            if (propInfo == null) return null;

            // 1) If an attribute already implements our converter, just use it.
            var explicitConverter = propInfo
                .Attributes()
                .OfType<ISqlBulkHelpersPropertyConverter>()
                .FirstOrDefault();

            if (explicitConverter != null)
                return explicitConverter;

            // 2) Detect RepoDb's [PropertyHandler(typeof(...))] by name (no RepoDb reference required)
            var repoDbAttr = propInfo
                .FindAttributesByName(RepoDbNames.PropertyHandlerAttributeName)
                .FirstOrDefault();

            if (repoDbAttr == null)
                return null;

            // 3) Extract the handler type from the attribute  with last ditch effort fallback if name/property differs
            var repoDbHandlerType = repoDbAttr.GetPropertyValue(RepoDbNames.PropertyHandlerTypePropertyName) as Type
                ?? repoDbAttr.GetType().Properties().FirstOrDefault(p => p.CanRead && p.PropertyType == TypeCache.Type)?.GetValue(repoDbAttr) as Type;

            if (repoDbHandlerType == null)
                return null;

            // 4) Locate the 'Set' method: TInput Set(TResult input, PropertyHandlerSetOptions options);
            var repoDbHandlerSetMethod = repoDbHandlerType
                .Methods()
                .FirstOrDefault(m =>
                    m.Name.Equals(RepoDbNames.PropertyHandlerSetMethodName, StringComparison.OrdinalIgnoreCase)
                    && !m.IsGenericMethodDefinition
                    && m.Parameters() is IList<ParameterInfo> methodParams
                    && methodParams.Count == 2 //Must have exactly 2 params (per RepoDb Interface) and validate the Name of the second param below...
                    && methodParams[0].ParameterType.IsAssignableFrom(propInfo.PropertyType) //Input Type (to be converted) must match the current Property Type we are processing!
                    && methodParams[1].ParameterType is Type methodType
                    //The Set method is used to write value to the DB!
                    && (methodType.FullName ?? methodType.Name).IndexOf(RepoDbNames.PropertyHandlerSetOptionsClassName, StringComparison.OrdinalIgnoreCase) >= 0
                );

            if (repoDbHandlerSetMethod == null)
                return null;

            try
            {
                // 5) Create an instance of the PropertyHandler class (as only the Type is reference in the Attribute)
                object propertyHandlerInstance = repoDbHandlerType.CreateInstance(); // Fasterflect; falls back to Activator if needed

                // 6) Create high‑performance Fasterflect invoker (delegate)
                var repoDbPropHandlerGetMethodInvoker = repoDbHandlerSetMethod.DelegateForCallMethod(); // returns Fasterflect.MethodInvoker

                // For safety, ensure the first arg (TInput) matches/accepts the property type
                //NOTE: We already know we have 2 parameters above!
                var parameters = repoDbHandlerSetMethod.Parameters();
                var optionsParamType = parameters[1].ParameterType;

                // Options: null for class, default(T) for struct which is returned when we try to create an instance...
                object optionsArg = optionsParamType.IsValueType 
                    ? optionsParamType.CreateInstance()
                    : null;

                // 8) Return a converter wrapper Interfaced Func<object, object> to encapsulate the call to the Invoker
                //      of the Get mehod of the PropertyHandler
                return new SqlBulkLambdaPropertyConverter(valueObj =>
                {
                    if (valueObj is null) return null;
                    return repoDbPropHandlerGetMethodInvoker.Invoke(propertyHandlerInstance, valueObj, optionsArg);
                });
            }
            catch
            {
                return null; // could not construct; ignore optional feature
            }
        }
    }
}
