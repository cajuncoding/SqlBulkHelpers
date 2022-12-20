using System;
using System.Reflection;
using System.Collections.Generic;
using System.Text.RegularExpressions;
using System.IO;
using System.Linq;
using System.Diagnostics;

namespace SqlBulkHelpers.CustomExtensions
{
    //BBernard:
    //Shared from my common library via MIT License!
    internal static class EmbeddedResourcesCustomExtensions
    {
        public static byte[] LoadEmbeddedResourceDataAsBytes(this Assembly assembly, String resourceNameLiteral, bool enableLoosePathSeparatorMatching = true)
        {
            //BBernard 
            //NOTE: For literal values we assume they are the File Name that would be at the END of a RegEx Resource pattern match
            //so we dynamically construct the proper regex to match the literal at the End of a match.
            var regexNamePatternText = GetRegexPatternFromLiteral(resourceNameLiteral, enableLoosePathSeparatorMatching);
            var bytes = assembly?.LoadEmbeddedResourceData(regexNamePatternText);
            return bytes;
        }

        public static String LoadEmbeddedResourceDataAsString(this Assembly assembly, String resourceNameLiteral, bool enableLoosePathSeparatorMatching = true)
        {
            //BBernard 
            //NOTE: For literal values we assume they are the File Name that would be at the END of a RegEx Resource pattern match
            //so we dynamically construct the proper regex to match the literal at the End of a match.
            var regexNamePatternText = GetRegexPatternFromLiteral(resourceNameLiteral, enableLoosePathSeparatorMatching);
            return assembly?.LoadEmbeddedResourceDataFromRegexAsString(regexNamePatternText);
        }

        private static readonly Regex _loosePathSeparatorRegex = new Regex(@"[\.\\/]", RegexOptions.Compiled);

        private static string GetRegexPatternFromLiteral(string resourceNameLiteral, bool enableLoosePathSeparatorMatching = true)
        {
            var resourceRegex = enableLoosePathSeparatorMatching
                ? string.Join(@"\.", _loosePathSeparatorRegex.Split(resourceNameLiteral).Select(Regex.Escape))
                : Regex.Escape(resourceNameLiteral);
            return $".*{resourceRegex}$";
        }

        public static String LoadEmbeddedResourceDataFromRegexAsString(this Assembly assembly, String resourceNameRegexPattern)
        {
            var bytes = assembly?.LoadEmbeddedResourceData(resourceNameRegexPattern);
            if (bytes == null)
                return null;

            using (var memoryStream = new MemoryStream(bytes))
            using (var streamReader = new StreamReader(memoryStream))
            {
                //BBernard
                //NOTE: UTF8 Encoding does NOT handle byte-order-mark correctly (e.g. JObject.Parse() may fail due to unexpected BOM),
                //          therefore we need to use StreamReader for more robust handling of text content;
                //          Since it is initially loaded as a Stream we need to Parse it as a Stream also!
                //          More Info here: https://stackoverflow.com/a/11701560/7293142
                //var textContent = Encoding.UTF8.GetString(bytes);
                var textContent = streamReader.ReadToEnd();
                return textContent;
            }
        }

        public static byte[] LoadEmbeddedResourceData(this Assembly assembly, String resourceNameRegexPattern)
        {
            var enumerableResults = assembly?.GetManifestResourceBytesRegex(resourceNameRegexPattern);
            return enumerableResults?.FirstOrDefault();
        }

        public static IEnumerable<byte[]> GetManifestResourceBytesRegex(this Assembly assembly, String resourceNameRegexPattern)
        {
            var resourceBytes = assembly
                ?.GetManifestResourceNamesByRegex(resourceNameRegexPattern)
                ?.Select(assembly.GetManifestResourceBytes);

            return resourceBytes;
        }

        public static List<String> GetManifestResourceNamesByRegex(this Assembly assembly, String resourceNameRegexPattern)
        {
            Regex rx = new Regex(resourceNameRegexPattern, RegexOptions.Compiled | RegexOptions.IgnoreCase);
            return assembly?.GetManifestResourceNamesByRegex(rx);
        }

        public static List<String> GetManifestResourceNamesByRegex(this Assembly assembly, Regex rxResourceNamePatter)
        {
            var resourceNames = assembly
                ?.GetManifestResourceNames()
                .Where(n => rxResourceNamePatter.IsMatch(n))
                .ToList();

            #if DEBUG
            Debug.WriteLine("Matched Resource Names:");
            if (resourceNames.HasAny())
            {
                foreach (var name in resourceNames)
                {
                    Debug.WriteLine($" - {name}");
                }
            }
            #endif

            return resourceNames;
        }

        public static byte[] GetManifestResourceBytes(this Assembly assembly, string fullyQualifiedName)
        {
            using (var stream = assembly?.GetManifestResourceStream(fullyQualifiedName))
            {
                return stream?.ToByteArray();
            }
        }
    }

}
