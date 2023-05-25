using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Data.SqlClient;

namespace SqlBulkHelpers
{
    public static class SqlBulkHelpersCustomExtensions
    {
        private const int MaxTableNameLength = 116;

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

        public static async Task<SqlConnection> EnsureSqlConnectionIsOpenAsync(this SqlConnection sqlConnection)
        {
            sqlConnection.AssertArgumentIsNotNull(nameof(sqlConnection));
            if (sqlConnection.State != ConnectionState.Open)
                await sqlConnection.OpenAsync().ConfigureAwait(false);

            return sqlConnection;
        }

        public static SqlConnection EnsureSqlConnectionIsOpen(this SqlConnection sqlConnection)
        {
            sqlConnection.AssertArgumentIsNotNull(nameof(sqlConnection));
            if (sqlConnection.State != ConnectionState.Open)
                sqlConnection.Open();

            return sqlConnection;
        }

        public static TableNameTerm GetSqlBulkHelpersMappedTableNameTerm(this Type type, string tableNameOverride = null)
        {
            string tableName = tableNameOverride;
            if (string.IsNullOrWhiteSpace(tableName))
            {
                var processingDef = SqlBulkHelpersProcessingDefinition.GetProcessingDefinition(type);
                if (processingDef.IsMappingLookupEnabled)
                    tableName = processingDef.MappedDbTableName;
            }

            return tableName.ParseAsTableNameTerm();
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

        public static string EnforceUnderscoreTableNameTerm(this string term)
            => term?.Replace(" ", "_");

        public static string QualifySqlTerm(this string term)
        {
            return string.IsNullOrWhiteSpace(term) 
                ? null 
                : $"[{term.TrimTableNameTerm()}]";
        }

        public static IEnumerable<string> QualifySqlTerms(this IEnumerable<string> terms)
            => terms.Select(t => t.QualifySqlTerm());

        public static string MakeTableNameUnique(this string tableNameToMakeUnique, int uniqueTokenLength = 10)
        {
            if (string.IsNullOrWhiteSpace(tableNameToMakeUnique))
                throw new ArgumentNullException(nameof(tableNameToMakeUnique));

            var uniqueTokenSuffix = string.Concat("_", IdGenerator.NewId(uniqueTokenLength));
            var uniqueName = string.Concat(tableNameToMakeUnique, uniqueTokenSuffix);
                
            if (uniqueName.Length > MaxTableNameLength)
                uniqueName = string.Concat(tableNameToMakeUnique.Substring(0, MaxTableNameLength - uniqueTokenSuffix.Length), uniqueTokenSuffix);

            return uniqueName;
        }

        public static string ToCsv(this IEnumerable<string> enumerableList)
            => string.Join(", ", enumerableList);

        public static bool HasAny<T>(this IEnumerable<T> items)
            => items != null && items.Any();

        public static bool IsNullOrEmpty<T>(this IEnumerable<T> items)
            => !items.HasAny();

        public static T[] AsArray<T>(this IEnumerable<T> items)
            => items is T[] itemArray ? itemArray : items.ToArray();

        public static bool ContainsIgnoreCase(this IEnumerable<string> items, string valueToFind)
            => items != null && items.Any(i => i.Equals(valueToFind, StringComparison.OrdinalIgnoreCase));

        public static bool TryAdd<TKey, TValue>(this Dictionary<TKey, TValue> dictionary, TKey key, TValue value)
        {
            if (dictionary.ContainsKey(key)) return false;
            dictionary.Add(key, value);
            return true;
        }
    }
}
