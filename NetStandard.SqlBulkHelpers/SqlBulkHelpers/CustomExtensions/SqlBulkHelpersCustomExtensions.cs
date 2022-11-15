﻿using System;
using System.Collections.Generic;
using System.Linq;

namespace SqlBulkHelpers
{
    internal static class SqlBulkHelpersCustomExtensions
    {
        public const string DefaultSchemaName = "dbo";

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

        public static (string SchemaName, string TableName, string FullyQualifiedTableName) ParseAsTableNameTerm(this string tableName)
        {
            var loweredTableName = tableName.ToLowerInvariant();

            string parsedSchemaName = DefaultSchemaName;
            string parsedTableName = null;

            //Second Try Parsing the Table & Schema name a Direct Lookup and return if found...
            var terms = loweredTableName.Split('.');
            switch (terms.Length)
            {
                //Split will always return an array with at least 1 element
                case 1:
                    parsedTableName = TrimTableNameTerm(terms[0]);
                    break;
                default:
                    var schemaTerm = TrimTableNameTerm(terms[0]);
                    parsedSchemaName = schemaTerm ?? DefaultSchemaName;
                    parsedTableName = TrimTableNameTerm(terms[1]);
                    break;
            }

            if(parsedTableName == null)
                throw new ArgumentException("The Table Name specified could not be parsed; parsing resulted in null/empty value.");

            return (
                parsedSchemaName, 
                parsedTableName, 
                $"[{parsedSchemaName}].[{parsedTableName}]"
            );
        }

        private static string TrimTableNameTerm(string term)
        {
            if (string.IsNullOrWhiteSpace(term))
                return null;

            var trimmedTerm = term.Trim('[', ']', ' ');
            return trimmedTerm;
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
