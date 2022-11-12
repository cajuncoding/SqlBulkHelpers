using FastMember;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using SqlBulkHelpers.SqlBulkHelpers.CustomExtensions;
using LazyCacheHelpers;

namespace SqlBulkHelpers.SqlBulkHelpers.QueryProcessing
{
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

        public static SqlBulkHelpersProcessingDefinition GetProcessingDefinition<T>(SqlBulkHelpersColumnDefinition identityColumnDefinition = null)
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
            PropertyDefinitions = propertyDefinitions.AssertArgumentIsNotNull(nameof(propertyDefinitions)).ToArray();
            IsRowNumberColumnNameEnabled = isRowNumberColumnNameEnabled;
            MappedDbTableName = GetMappedDbTableName(entityType);
        }

        public PropInfoDefinition[] PropertyDefinitions { get; protected set; }

        public string MappedDbTableName { get; protected set; }

        public bool IsRowNumberColumnNameEnabled { get; protected set; }

        protected string GetMappedDbTableName(Type entityType)
        {
            var mappingAttribute = entityType.FindAttributes(
                MappingAttributeNames.RepoDbTableMapAttributeName, 
                MappingAttributeNames.DapperTableMapAttributeName
                //NOTE: Removed because the value is identical to Dapper so it will be handled above.
                //MappingAttributeNames.LinqToDbTableMapAttributeName
            ).FirstOrDefault();

            //Default to the Class Type Name...
            if (mappingAttribute == null)
                return entityType.Name;

            var attrAccessor = ObjectAccessor.Create(mappingAttribute);

            switch (mappingAttribute?.GetType().Name)
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

    public class PropInfoDefinition
    {
        public PropInfoDefinition(PropertyInfo propInfo, SqlBulkHelpersColumnDefinition identityColumnDef = null)
        {
            this.PropInfo = propInfo;
            this.PropertyName = propInfo.Name;
            this.PropertyType = propInfo.PropertyType;
            this.MappedDbFieldName = GetMappedDbFieldName(propInfo);
            //Early determination if a Property is an Identity Property for Fast processing later...
            this.IsIdentityProperty = identityColumnDef?.ColumnName?.Equals(propInfo.Name, StringComparison.OrdinalIgnoreCase) ?? false;
        }

        public string PropertyName { get; private set; }
        public string MappedDbFieldName { get; private set; }
        public bool IsIdentityProperty { get; private set; }
        public PropertyInfo PropInfo { get; private set; }
        public Type PropertyType { get; private set; }

        public override string ToString()
        {
            return $"{this.PropertyName} [{this.PropertyType.Name}]";
        }

        protected string GetMappedDbFieldName(PropertyInfo propInfo)
        {
            var mappingAttribute = propInfo.FindAttributes(MappingAttributeNames.RepoDbFieldMapAttributeName, MappingAttributeNames.LinqToDbFieldMapAttributeName).FirstOrDefault();

            //Default to the Class Property Name...
            if (mappingAttribute == null)
                return propInfo.Name;

            var attrAccessor = ObjectAccessor.Create(mappingAttribute);

            switch (mappingAttribute?.GetType().Name)
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
