using System;
using System.Collections.Generic;
using System.Data;
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

        public static string AssertArgumentIsNotNullOrWhiteSpace(this string arg, string argName)
        {
            if (string.IsNullOrWhiteSpace(arg)) throw new ArgumentNullException(argName);
            return arg;
        }

        public static TableNameTerm ParseAsTableNameTerm(this string tableName)
        {
            string parsedSchemaName, parsedTableName;

            //Second Try Parsing the Table & Schema name a Direct Lookup and return if found...
            var terms = tableName.Split(TableNameTerm.TermSeparator);
            switch (terms.Length)
            {
                //Split will always return an array with at least 1 element
                case 1:
                    parsedSchemaName = TableNameTerm.DefaultSchemaName;
                    parsedTableName = terms[0].TrimTableNameTerm();
                    break;
                default:
                    var schemaTerm = terms[0].TrimTableNameTerm();
                    parsedSchemaName = schemaTerm ?? TableNameTerm.DefaultSchemaName;
                    parsedTableName = terms[1].TrimTableNameTerm();
                    break;
            }

            if(parsedTableName == null)
                throw new ArgumentException("The Table Name specified could not be parsed; parsing resulted in null/empty value.");

            return new TableNameTerm(parsedSchemaName, parsedTableName);
        }

        public static string TrimTableNameTerm(this string term)
        {
            if (string.IsNullOrWhiteSpace(term))
                return null;

            var trimmedTerm = term.Trim('[', ']', ' ');
            return trimmedTerm;
        }

        public static string QualifySqlTerm(this string term)
        {
            return string.IsNullOrWhiteSpace(term) 
                ? null 
                : $"[{term.TrimTableNameTerm()}]";
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
