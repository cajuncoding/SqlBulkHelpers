using System;
using System.Collections.Generic;
using System.Collections.Concurrent;
using System.Linq;
using System.Reflection;

namespace SqlBulkHelpers
{
    public class SqlBulkHelpersObjectReflectionFactory
    {
        private static readonly ConcurrentDictionary<String, Lazy<List<PropInfoDefinition>>> _propInfoLazyCache = new ConcurrentDictionary<String, Lazy<List<PropInfoDefinition>>>();

        public static List<PropInfoDefinition> GetPropertyDefinitions<T>(SqlBulkHelpersColumnDefinition identityColumnDefinition = null)
        {
            var type = typeof(T);
            var propInfoLazy = _propInfoLazyCache.GetOrAdd(
                    $"[Type={type.Name}][Identity={identityColumnDefinition?.ColumnName ?? "N/A"}]",  //Cache Key
                    new Lazy<List<PropInfoDefinition>>(() =>    //Lazy (Thread Safe Lazy Loader)
                    {
                        var propertyInfos = type.GetProperties().Select((pi) => new PropInfoDefinition(pi, identityColumnDefinition)).ToList();
                        return propertyInfos;
                    })
                );

            return propInfoLazy.Value;
        }
    }

    public class PropInfoDefinition
    {
        //TODO: BBERNARD - Potentially Optimize this further by compiling Delegates for Field Setters for faster execution!
        public PropInfoDefinition(PropertyInfo propInfo, SqlBulkHelpersColumnDefinition identityColumnDef = null)
        {
            this.PropInfo = propInfo;
            this.Name = propInfo.Name;
            this.PropertyType = propInfo.PropertyType;
            //Early determination if a Property is an Identity Property for Fast processing later.
            this.IsIdentityProperty = identityColumnDef?.ColumnName?.Equals(propInfo.Name, StringComparison.OrdinalIgnoreCase) ?? false;
        }

        public String Name { get; private set; }
        public bool IsIdentityProperty { get; private set; }
        public PropertyInfo PropInfo { get; private set; }
        public Type PropertyType { get; private set; }

        public override string ToString()
        {
            return $"{this.Name} [{this.PropertyType.Name}]";
        }
    }
}
