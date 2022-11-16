using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Text;
using Microsoft.IdentityModel.Protocols;

namespace SqlBulkHelpers.SqlBulkHelpers.CustomExtensions
{
    internal static class ReflectionExtensions
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

        public static IEnumerable<Attribute> FindAttributes(this IEnumerable<Attribute> attributes, params string[] attributeNamesToFind)
        {

            if (attributeNamesToFind.IsNullOrEmpty())
                throw new ArgumentNullException(nameof(attributeNamesToFind));

            var results = new List<Attribute>();
            var attributesArray = attributes as Attribute[] ?? attributes.ToArray();

            foreach (var findName in attributeNamesToFind)
            {
                var findAttrName = findName.EndsWith(nameof(Attribute), StringComparison.OrdinalIgnoreCase)
                    ? findName
                    : string.Concat(findName, nameof(Attribute));

                var foundAttr = attributesArray.FirstOrDefault(attr => attr.GetType().Name.Equals(findAttrName, StringComparison.OrdinalIgnoreCase));
                if(foundAttr != null)
                    results.Add(foundAttr);
            }

            return results;
        }

    }
}
