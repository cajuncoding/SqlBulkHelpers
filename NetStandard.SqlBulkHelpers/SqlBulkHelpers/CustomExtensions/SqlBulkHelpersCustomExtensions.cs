using System;
using System.Collections.Generic;
using System.Linq;

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
            => dictionary.TryGetValue(key, out var value) ? value : default;

        public static String ToCSV(this IEnumerable<String> enumerableList)
            => string.Join(", ", enumerableList);

        public static bool HasAny<T>(this IEnumerable<T> items)
            => items != null && items.Any();

        public static bool IsNullOrEmpty<T>(this IEnumerable<T> items)
            => !items.HasAny();

        public static bool ContainsIgnoreCase(this IEnumerable<string> items, string valueToFind)
            => items != null && items.Any(i => i.Equals(valueToFind, StringComparison.OrdinalIgnoreCase));
    }
}
