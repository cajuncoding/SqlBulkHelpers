using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Text;

namespace SqlBulkHelpers.SqlBulkHelpers.CustomExtensions
{
    public static class ReflectionExtensions
    {
        public static IEnumerable<Attribute> FindAttributes(this Type type, params string[] attributeNames)
        {
            if (type == null)
                return null;

            var attributes = type.GetCustomAttributes(true).OfType<Attribute>();
            return FindAttributes(attributes, attributeNames);
        }


        public static IEnumerable<Attribute> FindAttributes(this PropertyInfo propInfo, params string[] attributeNames)
        {
            if (propInfo == null)
                return null;

            var attributes = propInfo.GetCustomAttributes(true).OfType<Attribute>();
            return FindAttributes(attributes, attributeNames);
        }

        public static IEnumerable<Attribute> FindAttributes(this IEnumerable<Attribute> attributes, params string[] attributeNames)
        {

            if (attributeNames.IsNullOrEmpty())
                throw new ArgumentNullException(nameof(attributeNames));

            var attributeNamesToSearch = attributeNames.Select(
                name => name.EndsWith(nameof(Attribute), StringComparison.OrdinalIgnoreCase) ? name : $"{name}{nameof(Attribute)}"
            );

            var results = attributes
                .Where(attr =>
                {
                    var attrName = attr?.GetType().Name;
                    return attrName != null && attributeNamesToSearch.ContainsIgnoreCase(attrName);
                });

            return results;
        }

    }
}
