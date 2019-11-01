using System;
using System.Collections.Generic;

namespace SqlBulkHelpers
{
    public static class SqlBulkHelpersCustomExtensions
    {
        public static T AssertArgumentIsNotNull<T>(this T arg, string argName)
        {
            if (arg == null) throw new ArgumentNullException(argName);
            return arg;
        }

        public static TValue GetValueOrDefault<TKey, TValue>(this IDictionary<TKey, TValue> dictionary, TKey key)
        {
            TValue value;
            return dictionary.TryGetValue(key, out value) ? value : default(TValue);
        }

        public static String ToCSV(this IEnumerable<String> enumerableList)
        {
            return String.Join(", ", enumerableList);
        }
    }
}
